package com.deltaconnect.protocol.merge

import org.apache.avro.generic.GenericRecord
import org.slf4j.LoggerFactory

/**
 * Row-level merge logic.
 *
 * For each existing row:
 * - Extract merge key, look up in [sourceIndex].
 * - If matched and operation is [MergeOperation.DELETE]: skip the row.
 * - If matched and operation is [MergeOperation.UPDATE] or [MergeOperation.INSERT]:
 *   emit the source record (full row replacement).
 * - If not matched: emit the existing row unchanged (pass-through).
 *
 * @property sourceIndex Source records indexed by merge key. Built by
 *   [DeltaMergeEngine] with last writer wins deduplication.
 * @property mergeKeyNames Ordered list of column names forming the merge key.
 */
class RowMerger(
    private val sourceIndex: Map<MergeKey, IndexedSourceRecord>,
    private val mergeKeyNames: List<String>
) {

    /**
     * Merge a single file's rows against the source index.
     *
     * @param existingRows Iterator over rows from the existing Parquet file.
     * @param matchedKeys Mutable set tracking which source keys have been
     *   matched. Shared across all files processed during the merge to
     *   prevent duplicate inserts.
     * @return [FileMergeResult] with output rows and per-file counters.
     */
    fun mergeFile(
        existingRows: Iterator<GenericRecord>,
        matchedKeys: MutableSet<MergeKey>
    ): FileMergeResult {
        val outputRows = mutableListOf<GenericRecord>()
        var updatedCount = 0L
        var deletedCount = 0L
        var copiedCount = 0L

        while (existingRows.hasNext()) {
            val row = existingRows.next()
            val key = extractKey(row)
            val match = sourceIndex[key]

            if (match != null) {
                matchedKeys.add(key)
                when (match.operation) {
                    MergeOperation.DELETE -> {
                        deletedCount++
                    }
                    MergeOperation.UPDATE,
                    MergeOperation.INSERT -> {
                        updatedCount++
                        outputRows.add(match.record)
                    }
                }
            } else {
                copiedCount++
                outputRows.add(row)
            }
        }

        logger.debug(
            "File merge complete: updated={}, deleted={}, copied={}, outputRows={}",
            updatedCount, deletedCount, copiedCount, outputRows.size
        )

        return FileMergeResult(outputRows, updatedCount, deletedCount, copiedCount)
    }

    /**
     * Extract the merge key from a [GenericRecord].
     *
     * Avro [CharSequence] values are normalized to [String]
     * for consistent [MergeKey.equals] and [MergeKey.hashCode].
     */
    fun extractKey(record: GenericRecord): MergeKey {
        val values = mergeKeyNames.map { name ->
            val value = record.get(name)
            if (value is CharSequence) value.toString() else value
        }
        return MergeKey(values)
    }

    companion object {
        private val logger = LoggerFactory.getLogger(RowMerger::class.java)
    }
}
