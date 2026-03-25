package com.deltaconnect.protocol.parquet

import com.deltaconnect.protocol.schema.AvroToDeltaConverter
import com.deltaconnect.protocol.schema.DeltaType
import com.deltaconnect.protocol.schema.StructField
import com.deltaconnect.protocol.storage.LocalFileSystemLogStore
import io.kotest.matchers.comparables.shouldBeGreaterThan
import io.kotest.matchers.shouldBe
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

    @BeforeEach
    fun setUp() {
        logStore = LocalFileSystemLogStore(tempDir)
    }

    @Nested
    inner class BasicWriteRoundTrip {
        @Test
        fun `writes and reads back simple records`() {
            val schema =
                DeltaType.StructType(
                    listOf(
                        StructField("id", DeltaType.IntegerType, nullable = false),
                        StructField("name", DeltaType.StringType, nullable = true),
                    ),
                )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)
            val records =
                listOf(
                    GenericRecordBuilder(avroSchema).set("id", 1).set("name", "Alice").build(),
                    GenericRecordBuilder(avroSchema).set("id", 2).set("name", "Bob").build(),
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
            val schema =
                DeltaType.StructType(
                    listOf(StructField("id", DeltaType.IntegerType, nullable = false)),
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
            val schema =
                DeltaType.StructType(
                    listOf(
                        StructField("id", DeltaType.IntegerType, nullable = false),
                        StructField("value", DeltaType.StringType, nullable = true),
                    ),
                )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)
            val records =
                (1..100).map { i ->
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
            val schema =
                DeltaType.StructType(
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
                        StructField("col_timestamp", DeltaType.TimestampType),
                    ),
                )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)

            val record =
                GenericRecordBuilder(avroSchema)
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
            val schema =
                DeltaType.StructType(
                    listOf(StructField("amount", DeltaType.DecimalType(10, 2))),
                )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)

            val decimal = BigDecimal("123.45")
            val bytes = decimal.unscaledValue().toByteArray()
            val record =
                GenericRecordBuilder(avroSchema)
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
            val schema =
                DeltaType.StructType(
                    listOf(
                        StructField("id", DeltaType.IntegerType, nullable = false),
                        StructField("name", DeltaType.StringType, nullable = true),
                    ),
                )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)

            val records =
                listOf(
                    GenericRecordBuilder(avroSchema).set("id", 1).set("name", "Alice").build(),
                    GenericRecordBuilder(avroSchema).set("id", 2).set("name", null).build(),
                    GenericRecordBuilder(avroSchema).set("id", 3).set("name", null).build(),
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
    }

    @Nested
    inner class Compression {
        @Test
        fun `supports uncompressed`() {
            val schema =
                DeltaType.StructType(
                    listOf(StructField("id", DeltaType.IntegerType, nullable = false)),
                )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)
            val records =
                listOf(
                    GenericRecordBuilder(avroSchema).set("id", 1).build(),
                )

            val writer =
                ParquetFileWriter(
                    logStore,
                    schema,
                    compression = CompressionCodecName.UNCOMPRESSED,
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
            val schema =
                DeltaType.StructType(
                    listOf(
                        StructField("id", DeltaType.IntegerType, nullable = false),
                        StructField("value", DeltaType.StringType, nullable = true),
                    ),
                )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)

            val batchSize = 10_000
            val records =
                (0 until batchSize).map { i ->
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

    private fun readParquetFile(filePath: String): List<GenericRecord> = ParquetFileReader(logStore).read(filePath)

    private fun parseStats(json: String): ParsedStats = ParsedStats.fromJson(json)
}
