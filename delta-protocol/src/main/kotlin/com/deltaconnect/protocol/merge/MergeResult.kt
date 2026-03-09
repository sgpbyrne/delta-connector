package com.deltaconnect.protocol.merge

import com.deltaconnect.protocol.actions.AddFile
import com.deltaconnect.protocol.actions.RemoveFile
import org.apache.avro.generic.GenericRecord

/**
 * Source-agnostic merge operation types.
 */
enum class MergeOperation {
    INSERT,
    UPDATE,
    DELETE
}

/**
 * A source record paired with the merge operation to apply.
 *
 * @property record The data record conforming to the table schema.
 *   For [MergeOperation.DELETE], the record must contain at least the merge
 *   key columns; other columns may be null.
 * @property operation The merge operation to apply.
 */
data class SourceRecord(
    val record: GenericRecord,
    val operation: MergeOperation
)

/**
 * Composite merge key wrapping one or more column values.
 */
data class MergeKey(val values: List<Any?>)

/**
 * Source record stored in the merge index along with its ordinal position
 * for last writer wins deduplication.
 *
 * @property record The data record.
 * @property operation The merge operation to apply.
 * @property ordinal Position in the original source batch (higher = later).
 */
data class IndexedSourceRecord(
    val record: GenericRecord,
    val operation: MergeOperation,
    val ordinal: Int
)

/**
 * Min/max bounds for a single merge key column extracted from the source batch.
 */
data class KeyBounds(
    val min: Comparable<*>,
    val max: Comparable<*>
)

/**
 * Result of file pruning.
 *
 * @property matchingFiles Files whose stats overlap with the source key range.
 * @property prunedCount Number of files skipped (no possible overlap).
 */
data class PruneResult(
    val matchingFiles: List<AddFile>,
    val prunedCount: Int
)

/**
 * Result of merging a single existing file against the source index.
 *
 * @property outputRows Rows to write into the replacement file.
 *   Empty if all rows were deleted.
 * @property updatedCount Number of existing rows replaced by source records.
 * @property deletedCount Number of existing rows removed.
 * @property copiedCount Number of existing rows passed through unchanged.
 */
data class FileMergeResult(
    val outputRows: List<GenericRecord>,
    val updatedCount: Long,
    val deletedCount: Long,
    val copiedCount: Long
)

/**
 * Result of a complete MERGE operation, containing the actions to commit
 * to the Delta transaction log.
 *
 * @property addFiles New data files created by the merge.
 * @property removeFiles Existing data files replaced or fully deleted by the merge.
 * @property recordsInserted Number of new rows inserted (unmatched source records).
 * @property recordsUpdated Number of existing rows updated (matched, non-delete).
 * @property recordsDeleted Number of existing rows deleted.
 * @property recordsCopied Number of existing rows copied unchanged from rewritten files.
 * @property filesRewritten Number of existing files that were rewritten.
 * @property filesCreated Total number of new files created (rewrites + insert files).
 * @property filesSkipped Number of files pruned by stats (not read at all).
 */
data class MergeResult(
    val addFiles: List<AddFile>,
    val removeFiles: List<RemoveFile>,
    val recordsInserted: Long,
    val recordsUpdated: Long,
    val recordsDeleted: Long,
    val recordsCopied: Long,
    val filesRewritten: Int,
    val filesCreated: Int,
    val filesSkipped: Int
)
