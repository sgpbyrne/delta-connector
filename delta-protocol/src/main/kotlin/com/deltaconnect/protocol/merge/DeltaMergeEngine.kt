package com.deltaconnect.protocol.merge

import com.deltaconnect.protocol.DeltaSnapshot
import com.deltaconnect.protocol.actions.AddFile
import com.deltaconnect.protocol.actions.RemoveFile
import com.deltaconnect.protocol.parquet.ParquetFileReader
import com.deltaconnect.protocol.parquet.ParquetFileWriter
import com.deltaconnect.protocol.schema.DeltaType
import com.deltaconnect.protocol.schema.StructField
import com.deltaconnect.protocol.storage.DeltaLogStore
import org.slf4j.LoggerFactory
import java.util.UUID

/**
 * MERGE engine for Delta Lake tables.
 *
 * Takes a batch of source records and merges them into the existing
 * table files with [AddFile] and [RemoveFile] actions for the
 * transaction log.
 *
 * Algorithm:
 * 1. Index source batch by merge keys, deduplicating (keep latest per key).
 * 2. Extract min/max merge key bounds from the source batch.
 * 3. Prune files using [FilePruner] (stats-based filtering).
 * 4. For each matching file: read rows, merge via [RowMerger].
 * 5. Collect unmatched source records where operation != DELETE as inserts.
 * 6. Write new Parquet files, collect stats, generate AddFile/RemoveFile.
 * 7. Return [MergeResult].
 *
 * @property logStore Storage backend for reading/writing data files.
 * @property schema Table schema (all columns including merge keys).
 * @property mergeKeyNames Ordered list of column names forming the merge key.
 * @property tablePath Root path of the Delta table (for file naming).
 */
class DeltaMergeEngine(
    private val logStore: DeltaLogStore,
    private val schema: DeltaType.StructType,
    private val mergeKeyNames: List<String>,
    private val tablePath: String
) {

    private val mergeKeyFields: List<StructField> = mergeKeyNames.map { name ->
        schema.fields.find { it.name == name }
            ?: throw IllegalArgumentException(
                "Merge key column '$name' not found in schema"
            )
    }

    /**
     * Execute a MERGE operation.
     *
     * @param snapshot Current table snapshot (provides activeFiles).
     * @param sourceRecords Batch of source records to merge.
     *   Must be ordered so that later records for the same key
     *   represent the latest state (last writer wins deduplication).
     * @return [MergeResult] containing AddFile and RemoveFile actions.
     */
    fun merge(
        snapshot: DeltaSnapshot,
        sourceRecords: List<SourceRecord>
    ): MergeResult {
        val startTime = System.currentTimeMillis()

        logger.info(
            "MERGE started: table={}, sourceRecords={}, activeFiles={}, mergeKeys={}",
            tablePath, sourceRecords.size, snapshot.activeFiles.size, mergeKeyNames
        )

        if (sourceRecords.isEmpty()) {
            logger.info("MERGE complete: empty source batch, no operation")
            return MergeResult(
                addFiles = emptyList(),
                removeFiles = emptyList(),
                recordsInserted = 0,
                recordsUpdated = 0,
                recordsDeleted = 0,
                recordsCopied = 0,
                filesRewritten = 0,
                filesCreated = 0,
                filesSkipped = 0
            )
        }

        if (sourceRecords.size > LARGE_BATCH_THRESHOLD) {
            logger.warn(
                "Large source batch: table={}, records={}",
                tablePath, sourceRecords.size
            )
        }

        // Index source batch by merge keys (deduplicate)
        val sourceIndex = buildSourceIndex(sourceRecords)

        // Extract min/max key bounds from source
        val sourceKeyBounds = extractKeyBounds(sourceIndex)

        // Prune files via stats
        val pruner = FilePruner(mergeKeyFields)
        val pruneResult = pruner.prune(snapshot.activeFiles, sourceKeyBounds)

        logger.info(
            "File pruning: matching={}, pruned={}, total={}",
            pruneResult.matchingFiles.size, pruneResult.prunedCount,
            snapshot.activeFiles.size
        )

        // Merge each matching file
        val rowMerger = RowMerger(sourceIndex, mergeKeyNames)
        val matchedKeys = mutableSetOf<MergeKey>()
        val addFiles = mutableListOf<AddFile>()
        val removeFiles = mutableListOf<RemoveFile>()
        var totalUpdated = 0L
        var totalDeleted = 0L
        var totalCopied = 0L
        var filesRewritten = 0

        val reader = ParquetFileReader(logStore)
        val writer = ParquetFileWriter(logStore, schema)
        val now = System.currentTimeMillis()

        for (file in pruneResult.matchingFiles) {
            val fileMergeResult = reader.readIterator(file.path).use { iterator ->
                rowMerger.mergeFile(iterator, matchedKeys)
            }

            logger.debug(
                "File result: path={}, updated={}, deleted={}, copied={}",
                file.path, fileMergeResult.updatedCount,
                fileMergeResult.deletedCount, fileMergeResult.copiedCount
            )

            // Only rewrite if the file was actually modified
            if (fileMergeResult.updatedCount > 0 || fileMergeResult.deletedCount > 0) {
                filesRewritten++

                removeFiles.add(
                    RemoveFile(
                        path = file.path,
                        deletionTimestamp = now,
                        dataChange = true,
                        partitionValues = file.partitionValues,
                        size = file.size
                    )
                )

                if (fileMergeResult.outputRows.isNotEmpty()) {
                    val newPath = generateFilePath()
                    val writeResult = writer.write(newPath, fileMergeResult.outputRows)
                    addFiles.add(
                        AddFile(
                            path = writeResult.filePath,
                            partitionValues = file.partitionValues,
                            size = writeResult.fileSize,
                            modificationTime = now,
                            dataChange = true,
                            stats = writeResult.statsJson
                        )
                    )
                }
            }

            totalUpdated += fileMergeResult.updatedCount
            totalDeleted += fileMergeResult.deletedCount
            totalCopied += fileMergeResult.copiedCount
        }

        // Unmatched source records where operation != DELETE
        val insertRows = sourceIndex
            .filter { (key, _) -> key !in matchedKeys }
            .filter { (_, indexed) -> indexed.operation != MergeOperation.DELETE }
            .map { (_, indexed) -> indexed.record }

        if (insertRows.isNotEmpty()) {
            val newPath = generateFilePath()
            val writeResult = writer.write(newPath, insertRows)
            addFiles.add(
                AddFile(
                    path = writeResult.filePath,
                    partitionValues = emptyMap(),
                    size = writeResult.fileSize,
                    modificationTime = now,
                    dataChange = true,
                    stats = writeResult.statsJson
                )
            )
        }

        val durationMs = System.currentTimeMillis() - startTime

        val result = MergeResult(
            addFiles = addFiles,
            removeFiles = removeFiles,
            recordsInserted = insertRows.size.toLong(),
            recordsUpdated = totalUpdated,
            recordsDeleted = totalDeleted,
            recordsCopied = totalCopied,
            filesRewritten = filesRewritten,
            filesCreated = addFiles.size,
            filesSkipped = pruneResult.prunedCount
        )

        logger.info(
            "MERGE complete: table={}, inserts={}, updates={}, deletes={}," +
                " copied={}, filesRewritten={}, filesCreated={}," +
                " filesSkipped={}, durationMs={}",
            tablePath, result.recordsInserted, result.recordsUpdated,
            result.recordsDeleted, result.recordsCopied, result.filesRewritten,
            result.filesCreated, result.filesSkipped, durationMs
        )

        return result
    }

    /**
     * Build the source index with last writer wins deduplication.
     */
    private fun buildSourceIndex(
        sourceRecords: List<SourceRecord>
    ): Map<MergeKey, IndexedSourceRecord> {
        val index = HashMap<MergeKey, IndexedSourceRecord>(sourceRecords.size)

        for ((ordinal, source) in sourceRecords.withIndex()) {
            val key = extractMergeKey(source.record, mergeKeyNames)
            val existing = index[key]
            if (existing == null || ordinal > existing.ordinal) {
                index[key] = IndexedSourceRecord(
                    source.record, source.operation, ordinal
                )
            }
        }

        return index
    }

    /**
     * Extract min/max bounds for each merge key column from the source index.
     */
    @Suppress("UNCHECKED_CAST")
    private fun extractKeyBounds(
        sourceIndex: Map<MergeKey, IndexedSourceRecord>
    ): Map<String, KeyBounds> {
        val bounds = mutableMapOf<String, KeyBounds>()

        for ((i, field) in mergeKeyFields.withIndex()) {
            var min: Comparable<Any>? = null
            var max: Comparable<Any>? = null

            for ((key, _) in sourceIndex) {
                val value = key.values[i] ?: continue
                val comparable = value as? Comparable<Any> ?: continue

                if (min == null || comparable < min) {
                    min = comparable
                }
                if (max == null || comparable > max) {
                    max = comparable
                }
            }

            if (min != null && max != null) {
                bounds[field.name] = KeyBounds(min, max)
            }
        }

        return bounds
    }

    private fun generateFilePath(): String =
        "$tablePath/part-${UUID.randomUUID()}.parquet"

    companion object {
        private val logger = LoggerFactory.getLogger(DeltaMergeEngine::class.java)
        private const val LARGE_BATCH_THRESHOLD = 100_000
    }
}
