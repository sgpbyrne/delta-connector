package com.deltaconnect.protocol.parquet

import com.deltaconnect.protocol.schema.AvroToDeltaConverter
import com.deltaconnect.protocol.schema.DeltaType
import com.deltaconnect.protocol.schema.StructField
import com.deltaconnect.protocol.storage.LocalFileSystemLogStore
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import io.kotest.matchers.comparables.shouldBeGreaterThan
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.math.BigDecimal
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path

class ParquetFileWriterTest {

    @TempDir
    lateinit var tempDir: Path

    private lateinit var logStore: LocalFileSystemLogStore

    private val objectMapper = jacksonObjectMapper()

    @BeforeEach
    fun setUp() {
        logStore = LocalFileSystemLogStore(tempDir)
    }

    @Nested
    inner class BasicWriteRoundTrip {

        @Test
        fun `writes and reads back simple records`() {
            val schema = DeltaType.StructType(
                listOf(
                    StructField("id", DeltaType.IntegerType, nullable = false),
                    StructField("name", DeltaType.StringType, nullable = true)
                )
            )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)
            val records = listOf(
                GenericRecordBuilder(avroSchema).set("id", 1).set("name", "Alice").build(),
                GenericRecordBuilder(avroSchema).set("id", 2).set("name", "Bob").build()
            )

            val writer = ParquetFileWriter(logStore, schema)
            val result = writer.write("data/part-00000.parquet", records)

            result.filePath shouldBe "data/part-00000.parquet"
            result.recordCount shouldBe 2L
            result.fileSize shouldBeGreaterThan 0L

            val readRecords = readParquetFile("data/part-00000.parquet")
            readRecords.size shouldBe 2
            readRecords[0].get("id") shouldBe 1
            readRecords[0].get("name").toString() shouldBe "Alice"
            readRecords[1].get("id") shouldBe 2
            readRecords[1].get("name").toString() shouldBe "Bob"
        }

        @Test
        fun `writes empty file`() {
            val schema = DeltaType.StructType(
                listOf(StructField("id", DeltaType.IntegerType, nullable = false))
            )

            val writer = ParquetFileWriter(logStore, schema)
            val result = writer.write("data/empty.parquet", emptyList())

            result.recordCount shouldBe 0L
            result.fileSize shouldBeGreaterThan 0L // Parquet header/footer still present

            val readRecords = readParquetFile("data/empty.parquet")
            readRecords.size shouldBe 0
        }

        @Test
        fun `file size matches actual file on disk`() {
            val schema = DeltaType.StructType(
                listOf(
                    StructField("id", DeltaType.IntegerType, nullable = false),
                    StructField("value", DeltaType.StringType, nullable = true)
                )
            )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)
            val records = (1..100).map { i ->
                GenericRecordBuilder(avroSchema)
                    .set("id", i)
                    .set("value", "value-$i")
                    .build()
            }

            val writer = ParquetFileWriter(logStore, schema)
            val result = writer.write("data/sized.parquet", records)

            val actualSize = Files.size(tempDir.resolve("data/sized.parquet"))
            result.fileSize shouldBe actualSize
        }
    }

    @Nested
    inner class AllDeltaTypes {

        @Test
        fun `roundtrips all primitive types`() {
            val schema = DeltaType.StructType(
                listOf(
                    StructField("col_string", DeltaType.StringType),
                    StructField("col_long", DeltaType.LongType),
                    StructField("col_int", DeltaType.IntegerType),
                    StructField("col_short", DeltaType.ShortType),
                    StructField("col_byte", DeltaType.ByteType),
                    StructField("col_float", DeltaType.FloatType),
                    StructField("col_double", DeltaType.DoubleType),
                    StructField("col_boolean", DeltaType.BooleanType),
                    StructField("col_binary", DeltaType.BinaryType),
                    StructField("col_date", DeltaType.DateType),
                    StructField("col_timestamp", DeltaType.TimestampType)
                )
            )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)

            val record = GenericRecordBuilder(avroSchema)
                .set("col_string", "hello")
                .set("col_long", 42L)
                .set("col_int", 7)
                .set("col_short", 3) // Avro INT
                .set("col_byte", 1) // Avro INT
                .set("col_float", 3.14f)
                .set("col_double", 2.718)
                .set("col_boolean", true)
                .set("col_binary", ByteBuffer.wrap(byteArrayOf(0xDE.toByte(), 0xAD.toByte())))
                .set("col_date", 19000) // epoch days
                .set("col_timestamp", 1700000000000000L) // epoch micros
                .build()

            val writer = ParquetFileWriter(logStore, schema)
            writer.write("data/all-types.parquet", listOf(record))

            val readRecords = readParquetFile("data/all-types.parquet")
            readRecords.size shouldBe 1
            val r = readRecords[0]
            r.get("col_string").toString() shouldBe "hello"
            r.get("col_long") shouldBe 42L
            r.get("col_int") shouldBe 7
            r.get("col_short") shouldBe 3
            r.get("col_byte") shouldBe 1
            r.get("col_float") shouldBe 3.14f
            r.get("col_double") shouldBe 2.718
            r.get("col_boolean") shouldBe true
            r.get("col_date") shouldBe 19000
            r.get("col_timestamp") shouldBe 1700000000000000L
        }

        @Test
        fun `roundtrips decimal type`() {
            val schema = DeltaType.StructType(
                listOf(StructField("amount", DeltaType.DecimalType(10, 2)))
            )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)

            val decimal = BigDecimal("123.45")
            val bytes = decimal.unscaledValue().toByteArray()
            val record = GenericRecordBuilder(avroSchema)
                .set("amount", ByteBuffer.wrap(bytes))
                .build()

            val writer = ParquetFileWriter(logStore, schema)
            writer.write("data/decimal.parquet", listOf(record))

            val readRecords = readParquetFile("data/decimal.parquet")
            readRecords.size shouldBe 1
            val readBuf = readRecords[0].get("amount") as ByteBuffer
            val readBytes = ByteArray(readBuf.remaining())
            readBuf.get(readBytes)
            val readDecimal = BigDecimal(java.math.BigInteger(readBytes), 2)
            readDecimal shouldBe decimal
        }
    }

    @Nested
    inner class NullHandling {

        @Test
        fun `handles null values in nullable columns`() {
            val schema = DeltaType.StructType(
                listOf(
                    StructField("id", DeltaType.IntegerType, nullable = false),
                    StructField("name", DeltaType.StringType, nullable = true)
                )
            )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)

            val records = listOf(
                GenericRecordBuilder(avroSchema).set("id", 1).set("name", "Alice").build(),
                GenericRecordBuilder(avroSchema).set("id", 2).set("name", null).build(),
                GenericRecordBuilder(avroSchema).set("id", 3).set("name", null).build()
            )

            val writer = ParquetFileWriter(logStore, schema)
            val result = writer.write("data/nulls.parquet", records)

            val readRecords = readParquetFile("data/nulls.parquet")
            readRecords.size shouldBe 3
            readRecords[0].get("name").toString() shouldBe "Alice"
            readRecords[1].get("name") shouldBe null
            readRecords[2].get("name") shouldBe null

            // Verify stats
            val stats = parseStats(result.statsJson)
            stats.numRecords shouldBe 3
            stats.nullCount["name"] shouldBe 2L
            stats.nullCount["id"] shouldBe 0L
        }

        @Test
        fun `all-null column has no min max but correct null count`() {
            val schema = DeltaType.StructType(
                listOf(
                    StructField("id", DeltaType.IntegerType, nullable = false),
                    StructField("value", DeltaType.StringType, nullable = true)
                )
            )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)

            val records = listOf(
                GenericRecordBuilder(avroSchema).set("id", 1).set("value", null).build(),
                GenericRecordBuilder(avroSchema).set("id", 2).set("value", null).build()
            )

            val writer = ParquetFileWriter(logStore, schema)
            val result = writer.write("data/all-null.parquet", records)

            val stats = parseStats(result.statsJson)
            stats.nullCount["value"] shouldBe 2L
            stats.minValues.containsKey("value") shouldBe false
            stats.maxValues.containsKey("value") shouldBe false
        }
    }

    @Nested
    inner class StatsValidation {

        @Test
        fun `stats contain correct min max for integers`() {
            val schema = DeltaType.StructType(
                listOf(StructField("value", DeltaType.IntegerType, nullable = false))
            )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)

            val records = listOf(5, 1, 9, 3, 7).map { v ->
                GenericRecordBuilder(avroSchema).set("value", v).build()
            }

            val writer = ParquetFileWriter(logStore, schema)
            val result = writer.write("data/int-stats.parquet", records)

            val stats = parseStats(result.statsJson)
            stats.numRecords shouldBe 5
            stats.minValues["value"] shouldBe 1
            stats.maxValues["value"] shouldBe 9
            stats.nullCount["value"] shouldBe 0L
        }

        @Test
        fun `stats contain correct min max for strings`() {
            val schema = DeltaType.StructType(
                listOf(StructField("name", DeltaType.StringType, nullable = false))
            )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)

            val records = listOf("Charlie", "Alice", "Bob").map { name ->
                GenericRecordBuilder(avroSchema).set("name", name).build()
            }

            val writer = ParquetFileWriter(logStore, schema)
            val result = writer.write("data/str-stats.parquet", records)

            val stats = parseStats(result.statsJson)
            stats.minValues["name"] shouldBe "Alice"
            stats.maxValues["name"] shouldBe "Charlie"
        }

        @Test
        fun `string stats truncated to 32 chars`() {
            val schema = DeltaType.StructType(
                listOf(StructField("text", DeltaType.StringType, nullable = false))
            )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)

            val longString = "a".repeat(100)
            val records = listOf(
                GenericRecordBuilder(avroSchema).set("text", longString).build()
            )

            val writer = ParquetFileWriter(logStore, schema)
            val result = writer.write("data/long-str.parquet", records)

            val stats = parseStats(result.statsJson)
            val minStr = stats.minValues["text"] as String
            val maxStr = stats.maxValues["text"] as String
            minStr.length shouldBe 32
            maxStr.length shouldBe 32
        }

        @Test
        fun `stats for long type`() {
            val schema = DeltaType.StructType(
                listOf(StructField("big", DeltaType.LongType, nullable = false))
            )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)

            val records = listOf(
                GenericRecordBuilder(avroSchema).set("big", Long.MIN_VALUE).build(),
                GenericRecordBuilder(avroSchema).set("big", 0L).build(),
                GenericRecordBuilder(avroSchema).set("big", Long.MAX_VALUE).build()
            )

            val writer = ParquetFileWriter(logStore, schema)
            val result = writer.write("data/long-stats.parquet", records)

            val stats = parseStats(result.statsJson)
            stats.minValues["big"] shouldBe Long.MIN_VALUE
            stats.maxValues["big"] shouldBe Long.MAX_VALUE
        }

        @Test
        fun `stats for float and double`() {
            val schema = DeltaType.StructType(
                listOf(
                    StructField("f", DeltaType.FloatType, nullable = false),
                    StructField("d", DeltaType.DoubleType, nullable = false)
                )
            )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)

            val records = listOf(
                GenericRecordBuilder(avroSchema).set("f", 1.5f).set("d", 10.0).build(),
                GenericRecordBuilder(avroSchema).set("f", 3.5f).set("d", 20.0).build()
            )

            val writer = ParquetFileWriter(logStore, schema)
            val result = writer.write("data/float-stats.parquet", records)

            val stats = parseStats(result.statsJson)
            stats.minValues["f"] shouldBe 1.5
            stats.maxValues["f"] shouldBe 3.5
            stats.minValues["d"] shouldBe 10.0
            stats.maxValues["d"] shouldBe 20.0
        }

        @Test
        fun `stats for boolean type`() {
            val schema = DeltaType.StructType(
                listOf(StructField("flag", DeltaType.BooleanType, nullable = false))
            )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)

            val records = listOf(
                GenericRecordBuilder(avroSchema).set("flag", false).build(),
                GenericRecordBuilder(avroSchema).set("flag", true).build()
            )

            val writer = ParquetFileWriter(logStore, schema)
            val result = writer.write("data/bool-stats.parquet", records)

            val stats = parseStats(result.statsJson)
            stats.minValues["flag"] shouldBe false
            stats.maxValues["flag"] shouldBe true
        }

        @Test
        fun `stats for date type serialized as ISO date`() {
            val schema = DeltaType.StructType(
                listOf(StructField("d", DeltaType.DateType, nullable = false))
            )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)

            // 2024-01-15 = epoch day 19737
            val records = listOf(
                GenericRecordBuilder(avroSchema).set("d", 19737).build(),
                GenericRecordBuilder(avroSchema).set("d", 19738).build()
            )

            val writer = ParquetFileWriter(logStore, schema)
            val result = writer.write("data/date-stats.parquet", records)

            val stats = parseStats(result.statsJson)
            stats.minValues["d"] shouldBe "2024-01-15"
            stats.maxValues["d"] shouldBe "2024-01-16"
        }

        @Test
        fun `stats for timestamp type serialized as ISO 8601 UTC`() {
            val schema = DeltaType.StructType(
                listOf(StructField("ts", DeltaType.TimestampType, nullable = false))
            )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)

            // 2024-01-15T12:00:00Z = 1705320000000000 micros
            val micros = 1705320000000000L
            val records = listOf(
                GenericRecordBuilder(avroSchema).set("ts", micros).build()
            )

            val writer = ParquetFileWriter(logStore, schema)
            val result = writer.write("data/ts-stats.parquet", records)

            val stats = parseStats(result.statsJson)
            val tsStr = stats.minValues["ts"] as String
            tsStr shouldContain "2024-01-15"
            tsStr shouldContain "12:00"
        }

        @Test
        fun `stats for decimal type serialized as string`() {
            val schema = DeltaType.StructType(
                listOf(StructField("amount", DeltaType.DecimalType(10, 2)))
            )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)

            val dec1 = BigDecimal("10.50")
            val dec2 = BigDecimal("99.99")
            val records = listOf(
                GenericRecordBuilder(avroSchema)
                    .set("amount", ByteBuffer.wrap(dec1.unscaledValue().toByteArray()))
                    .build(),
                GenericRecordBuilder(avroSchema)
                    .set("amount", ByteBuffer.wrap(dec2.unscaledValue().toByteArray()))
                    .build()
            )

            val writer = ParquetFileWriter(logStore, schema)
            val result = writer.write("data/dec-stats.parquet", records)

            val stats = parseStats(result.statsJson)
            stats.minValues["amount"] shouldBe "10.50"
            stats.maxValues["amount"] shouldBe "99.99"
        }

        @Test
        fun `binary columns excluded from min max stats`() {
            val schema = DeltaType.StructType(
                listOf(
                    StructField("id", DeltaType.IntegerType, nullable = false),
                    StructField("data", DeltaType.BinaryType, nullable = true)
                )
            )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)

            val records = listOf(
                GenericRecordBuilder(avroSchema)
                    .set("id", 1)
                    .set("data", ByteBuffer.wrap(byteArrayOf(1, 2, 3)))
                    .build()
            )

            val writer = ParquetFileWriter(logStore, schema)
            val result = writer.write("data/binary-stats.parquet", records)

            val stats = parseStats(result.statsJson)
            stats.minValues.containsKey("data") shouldBe false
            stats.maxValues.containsKey("data") shouldBe false
        }

        @Test
        fun `stats with empty file have zero record count`() {
            val schema = DeltaType.StructType(
                listOf(StructField("id", DeltaType.IntegerType, nullable = false))
            )

            val writer = ParquetFileWriter(logStore, schema)
            val result = writer.write("data/empty-stats.parquet", emptyList())

            val stats = parseStats(result.statsJson)
            stats.numRecords shouldBe 0
        }
    }

    @Nested
    inner class Compression {

        @Test
        fun `supports uncompressed`() {
            val schema = DeltaType.StructType(
                listOf(StructField("id", DeltaType.IntegerType, nullable = false))
            )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)
            val records = listOf(
                GenericRecordBuilder(avroSchema).set("id", 1).build()
            )

            val writer = ParquetFileWriter(
                logStore, schema, compression = CompressionCodecName.UNCOMPRESSED
            )
            val result = writer.write("data/uncompressed.parquet", records)

            result.recordCount shouldBe 1L
            val readRecords = readParquetFile("data/uncompressed.parquet")
            readRecords.size shouldBe 1
            readRecords[0].get("id") shouldBe 1
        }
    }

    @Nested
    inner class ManyRecords {

        @Test
        fun `handles large batch without OOM`() {
            val schema = DeltaType.StructType(
                listOf(
                    StructField("id", DeltaType.IntegerType, nullable = false),
                    StructField("value", DeltaType.StringType, nullable = true)
                )
            )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)

            val batchSize = 10_000
            val records = (0 until batchSize).map { i ->
                GenericRecordBuilder(avroSchema)
                    .set("id", i)
                    .set("value", "val-$i")
                    .build()
            }

            val writer = ParquetFileWriter(logStore, schema)
            val result = writer.write("data/large.parquet", records)

            result.recordCount shouldBe batchSize.toLong()

            val stats = parseStats(result.statsJson)
            stats.numRecords shouldBe batchSize.toLong()
            stats.minValues["id"] shouldBe 0
            stats.maxValues["id"] shouldBe batchSize - 1
        }
    }

    @Nested
    inner class ComplexTypeExclusion {

        @Test
        fun `array and map columns excluded from stats`() {
            val schema = DeltaType.StructType(
                listOf(
                    StructField("id", DeltaType.IntegerType, nullable = false),
                    StructField("tags", DeltaType.ArrayType(DeltaType.StringType, containsNull = false)),
                    StructField("props", DeltaType.MapType(
                        DeltaType.StringType, DeltaType.StringType, valueContainsNull = true
                    ))
                )
            )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)

            val records = listOf(
                GenericRecordBuilder(avroSchema)
                    .set("id", 1)
                    .set("tags", listOf("a", "b"))
                    .set("props", mapOf("k" to "v"))
                    .build()
            )

            val writer = ParquetFileWriter(logStore, schema)
            val result = writer.write("data/complex.parquet", records)

            val stats = parseStats(result.statsJson)
            stats.minValues.containsKey("id") shouldBe true
            stats.minValues.containsKey("tags") shouldBe false
            stats.minValues.containsKey("props") shouldBe false
            stats.nullCount.containsKey("tags") shouldBe false
            stats.nullCount.containsKey("props") shouldBe false
        }
    }

    private fun readParquetFile(filePath: String): List<GenericRecord> {
        return ParquetFileReader(logStore).read(filePath)
    }

    private fun parseStats(json: String): ParsedStats {
        val map: Map<String, Any> = objectMapper.readValue(json)
        @Suppress("UNCHECKED_CAST")
        return ParsedStats(
            numRecords = (map["numRecords"] as Number).toLong(),
            minValues = map["minValues"] as Map<String, Any>,
            maxValues = map["maxValues"] as Map<String, Any>,
            nullCount = (map["nullCount"] as Map<String, Any>).mapValues { (_, v) -> (v as Number).toLong() }
        )
    }

    data class ParsedStats(
        val numRecords: Long,
        val minValues: Map<String, Any>,
        val maxValues: Map<String, Any>,
        val nullCount: Map<String, Long>
    )
}
