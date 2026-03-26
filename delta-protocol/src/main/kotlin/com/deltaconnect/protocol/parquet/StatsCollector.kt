package com.deltaconnect.protocol.parquet

import com.deltaconnect.protocol.schema.DeltaType
import com.deltaconnect.protocol.schema.StructField
import com.deltaconnect.protocol.util.NestedFieldResolver
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.avro.generic.GenericRecord
import java.math.BigDecimal
import java.nio.ByteBuffer
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

/**
 * Collects per-column min/max/nullCount statistics from Avro GenericRecords.
 *
 * Statistics are collected row-by-row during write and serialized to the JSON
 * format expected by Delta Lake's [AddFile.stats] field.
 *
 * Only the first [MAX_STATS_COLUMNS] leaf columns are tracked.
 * Type-specific JSON serialization:
 * - Integers/Longs: JSON numbers
 * - Decimals: JSON strings ("123.45")
 * - Timestamps: ISO 8601 UTC strings
 * - Dates: ISO 8601 date strings (yyyy-MM-dd)
 * - Strings: truncated to [MAX_STRING_LENGTH] characters
 * - Booleans: JSON booleans
 * - Binary/Array/Map/Struct: not included in stats
 *
 * @param schema Delta struct type describing the record schema.
 */
class StatsCollector(
    schema: DeltaType.StructType,
) {
    private val columns: List<ColumnStats>
    private var recordCount: Long = 0

    init {
        val cols = mutableListOf<ColumnStats>()
        collectLeafColumns(schema.fields, cols)
        columns = cols
    }

    /**
     * Update stats with values from a single record.
     */
    fun update(record: GenericRecord) {
        recordCount++
        for (col in columns) {
            val value = NestedFieldResolver.resolveGenericRecord(record, col.name)
            col.update(value)
        }
    }

    /**
     * Serialize collected stats to JSON string for [AddFile.stats].
     *
     * Output format:
     * ```json
     * {
     *   "numRecords": 100,
     *   "minValues": {"col1": 1, "col2": "abc"},
     *   "maxValues": {"col1": 99, "col2": "xyz"},
     *   "nullCount": {"col1": 0, "col2": 5}
     * }
     * ```
     */
    fun toJson(): String {
        val minValues = mutableMapOf<String, Any>()
        val maxValues = mutableMapOf<String, Any>()
        val nullCount = mutableMapOf<String, Any>()

        for (col in columns) {
            setNestedValue(nullCount, col.name, col.nullCount)
            val min = col.serializeMin()
            val max = col.serializeMax()
            if (min != null) setNestedValue(minValues, col.name, min)
            if (max != null) setNestedValue(maxValues, col.name, max)
        }

        val stats =
            linkedMapOf<String, Any>(
                "numRecords" to recordCount,
                "minValues" to minValues,
                "maxValues" to maxValues,
                "nullCount" to nullCount,
            )
        return objectMapper.writeValueAsString(stats)
    }

    @Suppress("UNCHECKED_CAST")
    private fun setNestedValue(
        root: MutableMap<String, Any>,
        path: String,
        value: Any,
    ) {
        val parts = path.split('.')
        var current = root
        for (i in 0 until parts.size - 1) {
            current = current.getOrPut(parts[i]) { mutableMapOf<String, Any>() } as MutableMap<String, Any>
        }
        current[parts.last()] = value
    }

    private fun collectLeafColumns(
        fields: List<StructField>,
        out: MutableList<ColumnStats>,
        pathPrefix: String = "",
    ) {
        for (field in fields) {
            if (out.size >= MAX_STATS_COLUMNS) break
            val fullPath = if (pathPrefix.isEmpty()) field.name else "$pathPrefix.${field.name}"
            when (field.type) {
                is DeltaType.StructType -> {
                    collectLeafColumns(field.type.fields, out, fullPath)
                }
                is DeltaType.ArrayType,
                is DeltaType.MapType,
                -> {
                    // No stats for complex types
                }
                else -> {
                    out.add(ColumnStats(fullPath, field.type))
                }
            }
        }
    }

    companion object {
        /** Maximum number of leaf columns to collect stats for. */
        const val MAX_STATS_COLUMNS: Int = 32

        /** Maximum string length in stats (strings are truncated beyond this). */
        const val MAX_STRING_LENGTH: Int = 32

        private val objectMapper: ObjectMapper = jacksonObjectMapper()
    }
}

/**
 * Tracks min/max/nullCount for a single column.
 */
internal class ColumnStats(
    val name: String,
    private val deltaType: DeltaType,
) {
    var nullCount: Long = 0L
        private set

    private var minValue: Comparable<Any>? = null
    private var maxValue: Comparable<Any>? = null
    private var hasNonNull: Boolean = false

    fun update(value: Any?) {
        if (value == null) {
            nullCount++
            return
        }

        hasNonNull = true
        @Suppress("UNCHECKED_CAST")
        val comparable = toComparable(value) ?: return

        val currentMin = minValue
        if (currentMin == null || comparable < currentMin) {
            minValue = comparable
        }
        val currentMax = maxValue
        if (currentMax == null || comparable > currentMax) {
            maxValue = comparable
        }
    }

    fun serializeMin(): Any? {
        if (!hasNonNull) return null
        return serializeValue(minValue ?: return null, isMax = false)
    }

    fun serializeMax(): Any? {
        if (!hasNonNull) return null
        return serializeValue(maxValue ?: return null, isMax = true)
    }

    @Suppress("UNCHECKED_CAST")
    private fun toComparable(value: Any): Comparable<Any>? =
        when (deltaType) {
            is DeltaType.StringType -> value.toString() as Comparable<Any>
            is DeltaType.LongType -> (value as Number).toLong() as Comparable<Any>
            is DeltaType.IntegerType -> (value as Number).toInt() as Comparable<Any>
            is DeltaType.ShortType -> (value as Number).toInt() as Comparable<Any>
            is DeltaType.ByteType -> (value as Number).toInt() as Comparable<Any>
            is DeltaType.FloatType -> (value as Number).toFloat() as Comparable<Any>
            is DeltaType.DoubleType -> (value as Number).toDouble() as Comparable<Any>
            is DeltaType.BooleanType -> (value as Boolean) as Comparable<Any>
            is DeltaType.DateType -> (value as Number).toInt() as Comparable<Any>
            is DeltaType.TimestampType -> (value as Number).toLong() as Comparable<Any>
            is DeltaType.DecimalType -> {
                when (value) {
                    is BigDecimal -> value as Comparable<Any>
                    is ByteBuffer -> {
                        val bytes = ByteArray(value.remaining())
                        value.duplicate().get(bytes)
                        BigDecimal(java.math.BigInteger(bytes), deltaType.scale) as Comparable<Any>
                    }
                    else -> null
                }
            }
            is DeltaType.BinaryType -> null
            is DeltaType.ArrayType -> null
            is DeltaType.MapType -> null
            is DeltaType.StructType -> null
        }

    private fun serializeValue(
        value: Comparable<*>,
        isMax: Boolean,
    ): Any? =
        when (deltaType) {
            is DeltaType.StringType -> {
                val str = value as String
                if (str.length > StatsCollector.MAX_STRING_LENGTH) {
                    val truncated = str.substring(0, StatsCollector.MAX_STRING_LENGTH)
                    if (isMax) incrementTruncatedMax(truncated) else truncated
                } else {
                    str
                }
            }
            is DeltaType.LongType -> value as Long
            is DeltaType.IntegerType -> value as Int
            is DeltaType.ShortType -> value as Int
            is DeltaType.ByteType -> value as Int
            is DeltaType.FloatType -> value as Float
            is DeltaType.DoubleType -> value as Double
            is DeltaType.BooleanType -> value as Boolean
            is DeltaType.DateType -> {
                val epochDay = (value as Int).toLong()
                LocalDate.ofEpochDay(epochDay).format(DateTimeFormatter.ISO_LOCAL_DATE)
            }
            is DeltaType.TimestampType -> {
                val micros = value as Long
                val epochSecond = Math.floorDiv(micros, 1_000_000L)
                val microsFraction = Math.floorMod(micros, 1_000_000L)
                val instant = Instant.ofEpochSecond(epochSecond, microsFraction * 1_000L)
                instant.atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
            }
            is DeltaType.DecimalType -> (value as BigDecimal).toPlainString()
            else -> null
        }

    private fun incrementTruncatedMax(truncated: String): String? {
        val chars = truncated.toCharArray()
        var i = chars.size - 1
        while (i >= 0) {
            if (chars[i] < Char.MAX_VALUE) {
                chars[i]++
                return String(chars, 0, i + 1)
            }
            i--
        }
        return null
    }
}
