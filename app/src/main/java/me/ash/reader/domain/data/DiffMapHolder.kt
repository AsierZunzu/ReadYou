package me.ash.reader.domain.data

import android.content.Context
import android.util.Log
import androidx.compose.runtime.mutableStateMapOf
import androidx.compose.runtime.snapshotFlow
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import dagger.hilt.android.qualifiers.ApplicationContext
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.debounce
import kotlinx.coroutines.flow.mapNotNull
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import me.ash.reader.domain.model.account.Account
import me.ash.reader.domain.model.account.AccountType
import me.ash.reader.domain.model.article.ArticleWithFeed
import me.ash.reader.domain.service.AccountService
import me.ash.reader.domain.service.RssService
import me.ash.reader.infrastructure.di.ApplicationScope
import me.ash.reader.infrastructure.di.IODispatcher
import java.io.File
import java.util.Date
import javax.inject.Inject

private const val TAG = "DiffMapHolder"

/** How long to coalesce rapid swipes before writing them to the database. */
private const val COMMIT_DEBOUNCE_MS = 300L

/**
 * Upper bound on how long a change may sit uncommitted. Guards against a continuous stream of
 * swipes perpetually resetting the debounce timer.
 */
private const val MAX_BATCH_WINDOW_MS = 3_000L

/** Delay before re-attempting a remote push that failed, doubled on each successive failure. */
private const val RETRY_BASE_DELAY_MS = 5_000L

private const val MAX_RETRY_DELAY_MS = 5 * 60_000L

/**
 * How long a local read-state change may remain unacknowledged by the remote before it is
 * abandoned and rolled back to the remote's value.
 */
private const val PENDING_TTL_MS = 24 * 60 * 60 * 1000L

@OptIn(FlowPreview::class)
class DiffMapHolder @Inject constructor(
    @ApplicationContext private val context: Context,
    @ApplicationScope private val applicationScope: CoroutineScope,
    @IODispatcher private val ioDispatcher: CoroutineDispatcher,
    private val accountService: AccountService,
    private val rssService: RssService,
) {
    /**
     * Optimistic overlay rendered on top of the database rows, so a swipe is reflected in the UI
     * before it is persisted. Entries are removed once they reach the database.
     */
    val diffMap = mutableStateMapOf<String, Diff>()

    val diffMapSnapshotFlow = snapshotFlow { diffMap.toMap() }.stateIn(
        applicationScope, SharingStarted.Eagerly, emptyMap()
    )

    val shouldSyncWithRemote get() = currentAccount?.type != AccountType.Local

    private val gson = Gson()

    private val cacheDir = context.cacheDir.resolve("diff")
    private var userCacheDir = cacheDir

    private var currentAccount: Account? = null

    private val cacheFile: File get() = userCacheDir.resolve("diff_map.json")

    /** Serialises database commits and remote pushes so they can never interleave. */
    private val flushMutex = Mutex()

    /** Consecutive remote push failures, used to back off. Reset on any success. */
    private var consecutiveFailures = 0

    var dbJob: Job? = null
    var remoteJob: Job? = null

    init {
        applicationScope.launch {
            accountService.currentAccountFlow.mapNotNull { it }.collect { account ->
                val previousAccount = currentAccount
                if (previousAccount != null && previousAccount != account) {
                    cleanup(previousAccount)
                }
                currentAccount = account
                init(account)
            }
        }
    }

    private fun init(account: Account) {
        userCacheDir = cacheDir.resolve(account.id.toString())
        applicationScope.launch(ioDispatcher) {
            restoreDiffsFromCache()
            commitDiffs()
        }
        commitOnChange()
        if (account.type != AccountType.Local) {
            syncOnChange()
        }
    }

    private fun cleanup(account: Account) {
        dbJob?.cancel()
        remoteJob?.cancel()
        writeDiffsToCache()
        diffMap.clear()
        consecutiveFailures = 0
    }

    private fun commitOnChange() {
        dbJob = applicationScope.launch(ioDispatcher) {
            launch {
                diffMapSnapshotFlow.debounce(COMMIT_DEBOUNCE_MS).collect {
                    if (it.isNotEmpty()) {
                        writeDiffsToCache()
                        commitDiffs()
                    }
                }
            }
            // Backstop: a sustained stream of swipes keeps resetting the debounce above, so force
            // a flush at a fixed interval regardless.
            launch {
                while (isActive) {
                    delay(MAX_BATCH_WINDOW_MS)
                    if (diffMap.isNotEmpty()) {
                        commitDiffs()
                    }
                }
            }
        }
    }

    private fun syncOnChange() {
        remoteJob = applicationScope.launch(ioDispatcher) {
            while (isActive) {
                val pushed = pushPendingReadStatus()
                val delayMs = if (pushed) {
                    RETRY_BASE_DELAY_MS
                } else {
                    (RETRY_BASE_DELAY_MS shl consecutiveFailures.coerceAtMost(6))
                        .coerceAtMost(MAX_RETRY_DELAY_MS)
                }
                delay(delayMs)
            }
        }
    }

    fun checkIfUnread(articleWithFeed: ArticleWithFeed): Boolean {
        return diffMap[articleWithFeed.article.id]?.isUnread ?: articleWithFeed.article.isUnread
    }

    /**
     * Updates the diff map with changes to an article's read/unread status.
     *
     * This function manages a map (`diffMap`) that tracks pending changes (diffs) to the
     * read/unread status of articles. These changes are not immediately applied to the
     * underlying data store but are held in `diffMap` until a later commit operation.
     *
     * The function supports three modes of updating:
     *
     * 1. **Toggle:** If `isUnread` is `null`, the function toggles the current read/unread
     *    status of the article.  If the article is currently unread, it will be marked as read,
     *    and vice-versa.
     * 2. **Mark as Unread:** If `isUnread` is `true`, the article will be marked as unread,
     *    regardless of its current status.
     * 3. **Mark as Read:** If `isUnread` is `false`, the article will be marked as read,
     *    regardless of its current status.
     *
     * The function determines if a change needs to be tracked based on the current status and desired status:
     *  - If the requested change matches the article's current status, the diff is removed from the map, if it exists. (No change is needed.)
     *  - Otherwise, the diff is added to or updated in the map.
     *
     * @param articleWithFeed The article and its associated feed data. This is used to identify the article
     *                        and access its current read/unread state.
     * @param isUnread An optional boolean indicating the desired read/unread status of the article.
     *                 - `null`: Toggles the current read/unread status.
     *                 - `true`: Marks the article as unread.
     *                 - `false`: Marks the article as read.
     *
     * @return A [Diff] object representing the changes made to the article.
     *
     * @see Diff
     */
    private fun updateDiffInternal(
        articleWithFeed: ArticleWithFeed, isUnread: Boolean? = null
    ): Diff? {
        val articleId = articleWithFeed.article.id

        val diff = diffMap[articleId]

        if (diff == null) {
            val isUnread = isUnread ?: !articleWithFeed.article.isUnread
            val diff = Diff(
                isUnread = isUnread, articleWithFeed = articleWithFeed
            )
            diffMap[articleId] = diff
            return diff
        } else {
            if (isUnread == null || diff.isUnread != isUnread) {
                val diff = diffMap.remove(articleId)
                return diff?.copy(isUnread = !diff.isUnread)
            }
        }
        return null
    }

    fun updateDiff(
        vararg articleWithFeed: ArticleWithFeed, isUnread: Boolean? = null
    ) {
        articleWithFeed.forEach { updateDiffInternal(it, isUnread) }
    }

    fun commitDiffsToDb() {
        applicationScope.launch(ioDispatcher) { commitDiffs() }
    }

    /**
     * Commits the overlay to the database and pushes any resulting local changes to the remote,
     * suspending until both have finished.
     *
     * Callers that are about to run a full sync must await this: the sync reconciler treats the
     * remote as authoritative for anything without a pending local claim, so a change that has
     * not yet reached the database would be silently reverted.
     */
    suspend fun flushAll() {
        commitDiffs()
        if (shouldSyncWithRemote) {
            pushPendingReadStatus()
        }
    }

    /**
     * Writes the overlay to the database, stamping each change as a locally-originated one that
     * the remote has not acknowledged.
     *
     * The overlay is cleared only *after* the write succeeds, and only for the entries that were
     * actually written — swipes made while the write was in flight are preserved. If the write
     * fails the overlay and its cache file are left intact so the change can be retried.
     */
    private suspend fun commitDiffs() = flushMutex.withLock {
        withContext(ioDispatcher) {
            val snapshot = diffMap.toMap()
            if (snapshot.isEmpty()) return@withContext

            val markAsReadArticles = snapshot.filterValues { !it.isUnread }.keys
            val markAsUnreadArticles = snapshot.filterValues { it.isUnread }.keys

            runCatching {
                val service = rssService.get()
                // Only stamp when there is a remote that can acknowledge the change; otherwise
                // the mark would never be cleared.
                val stamp =
                    if (shouldSyncWithRemote && service.supportsReadStatusSync) Date() else null
                service.batchMarkAsRead(markAsReadArticles, isUnread = false, updatedAt = stamp)
                service.batchMarkAsRead(markAsUnreadArticles, isUnread = true, updatedAt = stamp)
            }.onSuccess {
                snapshot.forEach { (id, diff) ->
                    // Leave entries that changed again while the write was in flight.
                    if (diffMap[id] == diff) diffMap.remove(id)
                }
                writeDiffsToCacheBlocking()
            }.onFailure {
                Log.e(TAG, "Failed to commit diffs to db, keeping them for retry", it)
            }
        }
    }

    /**
     * Pushes unacknowledged local read-state changes to the remote.
     *
     * The pending set is read back from the database rather than held in memory, so changes
     * survive process death — previously they were lost on restart and the next sync would
     * resurrect the articles.
     *
     * Successfully pushed changes are marked acknowledged. Changes that remain undeliverable past
     * [PENDING_TTL_MS] are abandoned and rolled back to the remote's value, so the two sides stop
     * disagreeing rather than fighting on every sync.
     *
     * @return true if there was nothing to do or everything was delivered.
     */
    private suspend fun pushPendingReadStatus(): Boolean = flushMutex.withLock {
        withContext(ioDispatcher) {
            if (!shouldSyncWithRemote) return@withContext true
            val service = rssService.get()
            if (!service.supportsReadStatusSync) return@withContext true

            val pending = runCatching { service.queryPendingReadStatus() }.getOrElse {
                Log.e(TAG, "Failed to query pending read status", it)
                return@withContext false
            }
            if (pending.isEmpty()) {
                consecutiveFailures = 0
                return@withContext true
            }

            val markAsRead = pending.filter { !it.isUnread }.map { it.id }.toSet()
            val markAsUnread = pending.filter { it.isUnread }.map { it.id }.toSet()

            val synced = supervisorScope {
                val read = async {
                    service.syncReadStatus(articleIds = markAsRead, isUnread = false)
                }
                val unread = async {
                    service.syncReadStatus(articleIds = markAsUnread, isUnread = true)
                }
                runCatching { read.await() }.getOrElse { emptySet() } +
                        runCatching { unread.await() }.getOrElse { emptySet() }
            }

            runCatching { service.clearPendingReadStatus(synced) }
                .onFailure { Log.e(TAG, "Failed to clear acknowledged read status", it) }

            val failed = pending.filter { it.id !in synced }
            if (failed.isEmpty()) {
                consecutiveFailures = 0
                return@withContext true
            }
            consecutiveFailures++
            Log.w(TAG, "Failed to push ${failed.size} read-status change(s) to remote")

            // Give up on changes the remote has refused for too long and restore its value.
            val deadline = System.currentTimeMillis() - PENDING_TTL_MS
            val expired = failed.filter { (it.readStatusUpdateAt?.time ?: 0L) < deadline }
            if (expired.isNotEmpty()) {
                Log.w(TAG, "Abandoning ${expired.size} undeliverable read-status change(s)")
                runCatching {
                    // The push never landed, so the remote still holds the opposite value.
                    service.revertPendingReadStatus(
                        articleIds = expired.filter { !it.isUnread }.map { it.id }.toSet(),
                        isUnread = true,
                    )
                    service.revertPendingReadStatus(
                        articleIds = expired.filter { it.isUnread }.map { it.id }.toSet(),
                        isUnread = false,
                    )
                }.onFailure { Log.e(TAG, "Failed to revert undeliverable read status", it) }
            }
            false
        }
    }

    private fun writeDiffsToCache() {
        applicationScope.launch(ioDispatcher) { writeDiffsToCacheBlocking() }
    }

    private fun writeDiffsToCacheBlocking() {
        try {
            val tmpJson = gson.toJson(diffMap.toMap())
            userCacheDir.mkdirs()
            cacheFile.createNewFile()
            if (cacheFile.exists() && cacheFile.canWrite()) {
                cacheFile.writeText(tmpJson)
            }
        } catch (e: Exception) {
            Log.e(TAG, "Failed to write diffs to cache", e)
        }
    }

    /**
     * Restores the overlay saved before the process was killed. Merged rather than assigned, so
     * diffs made since startup are not discarded.
     */
    private fun restoreDiffsFromCache() {
        try {
            if (!cacheFile.exists() || !cacheFile.canRead()) return
            val mapType = object : TypeToken<Map<String, Diff>>() {}.type
            val diffMapFromCache =
                gson.fromJson<Map<String, Diff>>(cacheFile.readText(), mapType) ?: return
            diffMapFromCache.forEach { (id, diff) -> diffMap.putIfAbsent(id, diff) }
        } catch (e: Exception) {
            Log.e(TAG, "Failed to restore diffs from cache", e)
        }
    }
}

data class Diff(
    val isUnread: Boolean, val articleId: String, val feedId: String
) {
    constructor(isUnread: Boolean, articleWithFeed: ArticleWithFeed) : this(
        isUnread = isUnread,
        articleId = articleWithFeed.article.id,
        feedId = articleWithFeed.feed.id,
    )
}
