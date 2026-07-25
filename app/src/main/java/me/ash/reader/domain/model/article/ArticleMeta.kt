package me.ash.reader.domain.model.article

import androidx.room.ColumnInfo
import androidx.room.PrimaryKey
import java.util.Date

/**
 * Data class for article metadata processing only.
 */

data class ArticleMeta(
    @PrimaryKey
    var id: String,
    @ColumnInfo
    var isUnread: Boolean = true,
    @ColumnInfo
    var isStarred: Boolean = false,
    /** @see me.ash.reader.domain.model.article.Article.readStatusUpdateAt */
    @ColumnInfo
    var readStatusUpdateAt: Date? = null,
) {
    /** True when a local read-state change is still waiting to be acknowledged by the remote. */
    val hasPendingReadStatus: Boolean get() = readStatusUpdateAt != null
}
