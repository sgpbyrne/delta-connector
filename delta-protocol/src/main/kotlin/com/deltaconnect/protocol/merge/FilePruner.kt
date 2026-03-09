package com.deltaconnect.protocol.merge

import com.deltaconnect.protocol.actions.AddFile
import com.deltaconnect.protocol.schema.DeltaType
import com.deltaconnect.protocol.schema.StructField
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.slf4j.LoggerFactory
import java.math.BigDecimal
import java.time.LocalDate
import java.time.OffsetDateTime

/**
 * File pruning using stats for MERGE operations.
 *
 * Uses min/max statistics from [AddFile.stats] to determine which files
 * could contain rows matching the source batch's merge key range.
 * Files whose key columns have no overlap with the source range are skipped,
 * avoiding unnecessary Parquet reads.
 *
 * If stats are missing, malformed, or a column cannot be compared,
 * the file is assumed to match.
 *
 * @property mergeKeyFields The merge key column definitions (name and type).
 */
class FilePruner(private val mergeKeyFields: List<StructField>) {

    /**
     * Partition active files into matching and pruned sets.
     *
     * @param files All active files from the snapshot.
     * @param sourceKeyBounds Min/max bounds for each merge key column
     *   extracted from the source batch. Map key is the column name.
     * @return [PruneResult] with the matching files and count of pruned files.
     */
    fun prune(
        files: Set<AddFile>,
        sourceKeyBounds: Map<String, KeyBounds>
    ): PruneResult {
        val matching = mutableListOf<AddFile>()
        var prunedCount = 0

        for (file in files) {
            if (canPrune(file, sourceKeyBounds)) {
                prunedCount++
                logger.debug("File pruned: path={}", file.path)
            } else {
                matching.add(file)
                logger.debug("File matched: path={}", file.path)
            }
        }

        return PruneResult(matching, prunedCount)
    }

    private fun canPrune(file: AddFile, sourceKeyBounds: Map<String, KeyBounds>): Boolean {
        val statsNode = parseStats(file.stats) ?: return false
        val minValues = statsNode.get("minValues") ?: return false
        val maxValues = statsNode.get("maxValues") ?: return false

        for (field in mergeKeyFields) {
            val sourceBounds = sourceKeyBounds[field.name] ?: continue
            val fileMin = resolveNestedNode(minValues, field.name)
            val fileMax = resolveNestedNode(maxValues, field.name)

            if (fileMin == null || fileMax == null || fileMin.isNull || fileMax.isNull) {
                continue // Cannot prune due to missing stats
            }

            if (hasNoOverlap(fileMin, fileMax, sourceBounds, field.type)) {
                return true // No overlap on this column = prune
            }
        }

        return false
    }

    /**
     * Check whether the file's range and source range have no overlap
     * for a given column.
     *
     * No overlap when: fileMax < sourceMin OR fileMin > sourceMax
     */
    private fun hasNoOverlap(
        fileMin: JsonNode,
        fileMax: JsonNode,
        sourceBounds: KeyBounds,
        fieldType: DeltaType
    ): Boolean {
        val fileMaxVsSourceMin = compareStatToValue(fileMax, sourceBounds.min, fieldType)
        if (fileMaxVsSourceMin != null && fileMaxVsSourceMin < 0) {
            return true
        }

        val fileMinVsSourceMax = compareStatToValue(fileMin, sourceBounds.max, fieldType)
        if (fileMinVsSourceMax != null && fileMinVsSourceMax > 0) {
            return true
        }

        return false
    }

    /**
     * Compare stats JSON value to source key bound value.
     *
     * Returns negative if stat < source, zero if equal, positive if stat > source.
     * Returns null if comparison is not possible (type mismatch, unsupported type).
     *
     * The comparison logic mirrors [StatsCollector]'s serialization Format
     *
     */
    @Suppress("UNCHECKED_CAST")
    private fun compareStatToValue(
        statNode: JsonNode,
        sourceValue: Comparable<*>,
        fieldType: DeltaType
    ): Int? {
        return try {
            when (fieldType) {
                is DeltaType.IntegerType,
                is DeltaType.ShortType,
                is DeltaType.ByteType -> {
                    if (!statNode.isNumber) return null
                    val statVal = statNode.intValue()
                    val srcVal = (sourceValue as Number).toInt()
                    statVal.compareTo(srcVal)
                }
                is DeltaType.LongType -> {
                    if (!statNode.isNumber) return null
                    val statVal = statNode.longValue()
                    val srcVal = (sourceValue as Number).toLong()
                    statVal.compareTo(srcVal)
                }
                is DeltaType.FloatType -> {
                    if (!statNode.isNumber) return null
                    val statVal = statNode.floatValue()
                    val srcVal = (sourceValue as Number).toFloat()
                    statVal.compareTo(srcVal)
                }
                is DeltaType.DoubleType -> {
                    if (!statNode.isNumber) return null
                    val statVal = statNode.doubleValue()
                    val srcVal = (sourceValue as Number).toDouble()
                    statVal.compareTo(srcVal)
                }
                is DeltaType.StringType -> {
                    if (!statNode.isTextual) return null
                    val statVal = statNode.textValue()
                    val srcVal = sourceValue.toString()
                    statVal.compareTo(srcVal)
                }
                is DeltaType.BooleanType -> {
                    if (!statNode.isBoolean) return null
                    val statVal = statNode.booleanValue()
                    val srcVal = sourceValue as Boolean
                    statVal.compareTo(srcVal)
                }
                is DeltaType.TimestampType -> {
                    if (!statNode.isTextual) return null
                    val statMicros = parseTimestampToMicros(statNode.textValue())
                        ?: return null
                    val srcMicros = (sourceValue as Number).toLong()
                    statMicros.compareTo(srcMicros)
                }
                is DeltaType.DateType -> {
                    if (!statNode.isTextual) return null
                    val statDays = parseDateToEpochDays(statNode.textValue())
                        ?: return null
                    val srcDays = (sourceValue as Number).toInt()
                    statDays.compareTo(srcDays)
                }
                is DeltaType.DecimalType -> {
                    if (!statNode.isTextual) return null
                    val statDecimal = BigDecimal(statNode.textValue())
                    val srcDecimal = sourceValue as BigDecimal
                    statDecimal.compareTo(srcDecimal)
                }
                is DeltaType.BinaryType,
                is DeltaType.ArrayType,
                is DeltaType.MapType,
                is DeltaType.StructType -> null
            }
        } catch (_: Exception) {
            null // Comparison failed, cannot prune
        }
    }

    /**
     * Resolve a potentially dot-separated column name against nested JSON.
     */
    private fun resolveNestedNode(node: JsonNode, path: String): JsonNode? {
        val parts = path.split('.')
        var current: JsonNode? = node
        for (part in parts) {
            current = current?.get(part) ?: return null
        }
        return current
    }

    private fun parseStats(statsJson: String?): JsonNode? {
        if (statsJson == null) return null
        return try {
            objectMapper.readTree(statsJson)
        } catch (_: Exception) {
            logger.warn("Cannot parse stats, assuming match: stats={}", statsJson)
            null
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(FilePruner::class.java)
        private val objectMapper = jacksonObjectMapper()

        /**
         * Parse an ISO 8601 UTC timestamp string to epoch microseconds.
         * Matches the format produced by [StatsCollector]
         */
        internal fun parseTimestampToMicros(isoString: String): Long? {
            return try {
                val odt = OffsetDateTime.parse(isoString)
                val epochSecond = odt.toEpochSecond()
                val nanos = odt.nano.toLong()
                epochSecond * 1_000_000L + nanos / 1_000L
            } catch (_: Exception) {
                null
            }
        }

        /**
         * Parse an ISO date string (yyyy-MM-dd) to epoch days.
         */
        internal fun parseDateToEpochDays(isoDate: String): Int? {
            return try {
                LocalDate.parse(isoDate).toEpochDay().toInt()
            } catch (_: Exception) {
                null
            }
        }
    }
}
