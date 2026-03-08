package com.deltaconnect.protocol.parquet

import com.deltaconnect.protocol.schema.AvroToDeltaConverter
import com.deltaconnect.protocol.schema.DeltaType
import com.deltaconnect.protocol.schema.StructField
import com.deltaconnect.protocol.storage.LocalFileSystemLogStore
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.math.BigDecimal
import java.nio.ByteBuffer
import java.nio.file.Path

class ParquetFileReaderTest {

    @TempDir
    lateinit var tempDir: Path

    private lateinit var logStore: LocalFileSystemLogStore
    private lateinit var reader: ParquetFileReader

    @BeforeEach
    fun setUp() {
        logStore = LocalFileSystemLogStore(tempDir)
        reader = ParquetFileReader(logStore)
    }

    @Nested
    inner class BasicRead {

        @Test
        fun `reads back records written by ParquetFileWriter`() {
            val schema = DeltaType.StructType(
                listOf(
                    StructField("id", DeltaType.IntegerType, nullable = false),
                    StructField("name", DeltaType.StringType, nullable = true)
                )
            )
            writeTestFile("data/test.parquet", schema, listOf(
                mapOf("id" to 1, "name" to "Alice"),
                mapOf("id" to 2, "name" to "Bob"),
                mapOf("id" to 3, "name" to "Charlie")
            ))

            val records = reader.read("data/test.parquet")

            records shouldHaveSize 3
            records[0].get("id") shouldBe 1
            records[0].get("name").toString() shouldBe "Alice"
            records[1].get("id") shouldBe 2
            records[1].get("name").toString() shouldBe "Bob"
            records[2].get("id") shouldBe 3
            records[2].get("name").toString() shouldBe "Charlie"
        }

        @Test
        fun `reads empty file`() {
            val schema = DeltaType.StructType(
                listOf(StructField("id", DeltaType.IntegerType, nullable = false))
            )
            writeTestFile("data/empty.parquet", schema, emptyList())

            val records = reader.read("data/empty.parquet")

            records shouldHaveSize 0
        }

        @Test
        fun `reads all primitive types`() {
            val schema = DeltaType.StructType(
                listOf(
                    StructField("col_string", DeltaType.StringType),
                    StructField("col_long", DeltaType.LongType),
                    StructField("col_int", DeltaType.IntegerType),
                    StructField("col_float", DeltaType.FloatType),
                    StructField("col_double", DeltaType.DoubleType),
                    StructField("col_boolean", DeltaType.BooleanType),
                    StructField("col_date", DeltaType.DateType),
                    StructField("col_timestamp", DeltaType.TimestampType)
                )
            )
            writeTestFile("data/primitives.parquet", schema, listOf(
                mapOf(
                    "col_string" to "hello",
                    "col_long" to 42L,
                    "col_int" to 7,
                    "col_float" to 3.14f,
                    "col_double" to 2.718,
                    "col_boolean" to true,
                    "col_date" to 19000,
                    "col_timestamp" to 1700000000000000L
                )
            ))

            val records = reader.read("data/primitives.parquet")

            records shouldHaveSize 1
            val r = records[0]
            r.get("col_string").toString() shouldBe "hello"
            r.get("col_long") shouldBe 42L
            r.get("col_int") shouldBe 7
            r.get("col_float") shouldBe 3.14f
            r.get("col_double") shouldBe 2.718
            r.get("col_boolean") shouldBe true
            r.get("col_date") shouldBe 19000
            r.get("col_timestamp") shouldBe 1700000000000000L
        }

        @Test
        fun `reads null values correctly`() {
            val schema = DeltaType.StructType(
                listOf(
                    StructField("id", DeltaType.IntegerType, nullable = false),
                    StructField("value", DeltaType.StringType, nullable = true)
                )
            )
            writeTestFile("data/nulls.parquet", schema, listOf(
                mapOf("id" to 1, "value" to "present"),
                mapOf("id" to 2, "value" to null),
                mapOf("id" to 3, "value" to null)
            ))

            val records = reader.read("data/nulls.parquet")

            records shouldHaveSize 3
            records[0].get("value").toString() shouldBe "present"
            records[1].get("value") shouldBe null
            records[2].get("value") shouldBe null
        }
    }

    @Nested
    inner class ColumnProjection {

        @Test
        fun `reads only projected columns`() {
            val fullSchema = DeltaType.StructType(
                listOf(
                    StructField("id", DeltaType.IntegerType, nullable = false),
                    StructField("name", DeltaType.StringType, nullable = true),
                    StructField("age", DeltaType.IntegerType, nullable = true),
                    StructField("email", DeltaType.StringType, nullable = true)
                )
            )
            writeTestFile("data/wide.parquet", fullSchema, listOf(
                mapOf("id" to 1, "name" to "Alice", "age" to 30, "email" to "alice@test.com"),
                mapOf("id" to 2, "name" to "Bob", "age" to 25, "email" to "bob@test.com")
            ))

            val projectionSchema = DeltaType.StructType(
                listOf(
                    StructField("id", DeltaType.IntegerType, nullable = false),
                    StructField("name", DeltaType.StringType, nullable = true)
                )
            )

            val records = reader.read("data/wide.parquet", projectionSchema)

            records shouldHaveSize 2
            records[0].get("id") shouldBe 1
            records[0].get("name").toString() shouldBe "Alice"
            // Projected schema only has id and name
            records[0].schema.fields.map { it.name() } shouldBe listOf("id", "name")
        }

        @Test
        fun `projection with single column`() {
            val fullSchema = DeltaType.StructType(
                listOf(
                    StructField("id", DeltaType.IntegerType, nullable = false),
                    StructField("name", DeltaType.StringType, nullable = true),
                    StructField("value", DeltaType.LongType, nullable = true)
                )
            )
            writeTestFile("data/single-proj.parquet", fullSchema, listOf(
                mapOf("id" to 1, "name" to "A", "value" to 100L),
                mapOf("id" to 2, "name" to "B", "value" to 200L)
            ))

            val projectionSchema = DeltaType.StructType(
                listOf(StructField("value", DeltaType.LongType, nullable = true))
            )

            val records = reader.read("data/single-proj.parquet", projectionSchema)

            records shouldHaveSize 2
            records[0].get("value") shouldBe 100L
            records[1].get("value") shouldBe 200L
            records[0].schema.fields.map { it.name() } shouldBe listOf("value")
        }
    }

    @Nested
    inner class SchemaEvolution {

        @Test
        fun `reads file with fewer columns than projection - missing columns are null`() {
            // Write file with narrow schema
            val narrowSchema = DeltaType.StructType(
                listOf(
                    StructField("id", DeltaType.IntegerType, nullable = false),
                    StructField("name", DeltaType.StringType, nullable = true)
                )
            )
            writeTestFile("data/narrow.parquet", narrowSchema, listOf(
                mapOf("id" to 1, "name" to "Alice"),
                mapOf("id" to 2, "name" to "Bob")
            ))

            // Read with wider schema (added "age" column)
            val widerSchema = DeltaType.StructType(
                listOf(
                    StructField("id", DeltaType.IntegerType, nullable = false),
                    StructField("name", DeltaType.StringType, nullable = true),
                    StructField("age", DeltaType.IntegerType, nullable = true)
                )
            )

            val records = reader.read("data/narrow.parquet", widerSchema)

            records shouldHaveSize 2
            records[0].get("id") shouldBe 1
            records[0].get("name").toString() shouldBe "Alice"
            records[0].get("age") shouldBe null
            records[1].get("id") shouldBe 2
            records[1].get("name").toString() shouldBe "Bob"
            records[1].get("age") shouldBe null
        }

        @Test
        fun `reads file with extra columns when projecting subset`() {
            val wideSchema = DeltaType.StructType(
                listOf(
                    StructField("id", DeltaType.IntegerType, nullable = false),
                    StructField("name", DeltaType.StringType, nullable = true),
                    StructField("age", DeltaType.IntegerType, nullable = true),
                    StructField("email", DeltaType.StringType, nullable = true)
                )
            )
            writeTestFile("data/evolved.parquet", wideSchema, listOf(
                mapOf("id" to 1, "name" to "Alice", "age" to 30, "email" to "a@test.com")
            ))

            val narrowSchema = DeltaType.StructType(
                listOf(
                    StructField("id", DeltaType.IntegerType, nullable = false),
                    StructField("name", DeltaType.StringType, nullable = true)
                )
            )

            val records = reader.read("data/evolved.parquet", narrowSchema)

            records shouldHaveSize 1
            records[0].get("id") shouldBe 1
            records[0].get("name").toString() shouldBe "Alice"
            records[0].schema.fields.map { it.name() } shouldBe listOf("id", "name")
        }
    }

    @Nested
    inner class StreamingIterator {

        @Test
        fun `iterator reads records lazily`() {
            val schema = DeltaType.StructType(
                listOf(StructField("id", DeltaType.IntegerType, nullable = false))
            )
            writeTestFile("data/iter.parquet", schema, (1..5).map { mapOf("id" to it) })

            val iter = reader.readIterator("data/iter.parquet")
            val collected = mutableListOf<Int>()

            iter.use {
                while (it.hasNext()) {
                    collected.add(it.next().get("id") as Int)
                }
            }

            collected shouldBe listOf(1, 2, 3, 4, 5)
        }

        @Test
        fun `iterator with projection`() {
            val schema = DeltaType.StructType(
                listOf(
                    StructField("id", DeltaType.IntegerType, nullable = false),
                    StructField("name", DeltaType.StringType, nullable = true)
                )
            )
            writeTestFile("data/iter-proj.parquet", schema, listOf(
                mapOf("id" to 1, "name" to "Alice"),
                mapOf("id" to 2, "name" to "Bob")
            ))

            val projSchema = DeltaType.StructType(
                listOf(StructField("name", DeltaType.StringType, nullable = true))
            )

            val names = mutableListOf<String>()
            reader.readIterator("data/iter-proj.parquet", projSchema).use { iter ->
                while (iter.hasNext()) {
                    names.add(iter.next().get("name").toString())
                }
            }

            names shouldBe listOf("Alice", "Bob")
        }

        @Test
        fun `iterator can be closed early`() {
            val schema = DeltaType.StructType(
                listOf(StructField("id", DeltaType.IntegerType, nullable = false))
            )
            writeTestFile("data/early-close.parquet", schema, (1..100).map { mapOf("id" to it) })

            val iter = reader.readIterator("data/early-close.parquet")
            val first = iter.next()
            first.get("id") shouldBe 1

            // Close without reading all records — should not throw
            iter.close()
        }

        @Test
        fun `iterator on empty file`() {
            val schema = DeltaType.StructType(
                listOf(StructField("id", DeltaType.IntegerType, nullable = false))
            )
            writeTestFile("data/empty-iter.parquet", schema, emptyList())

            val iter = reader.readIterator("data/empty-iter.parquet")
            iter.use {
                it.hasNext() shouldBe false
            }
        }

        @Test
        fun `iterator asSequence works`() {
            val schema = DeltaType.StructType(
                listOf(
                    StructField("id", DeltaType.IntegerType, nullable = false),
                    StructField("value", DeltaType.StringType, nullable = true)
                )
            )
            writeTestFile("data/seq.parquet", schema, listOf(
                mapOf("id" to 1, "value" to "a"),
                mapOf("id" to 2, "value" to "b"),
                mapOf("id" to 3, "value" to "c")
            ))

            val result = reader.readIterator("data/seq.parquet").use { iter ->
                iter.asSequence()
                    .filter { (it.get("id") as Int) > 1 }
                    .map { it.get("value").toString() }
                    .toList()
            }

            result shouldBe listOf("b", "c")
        }
    }

    @Nested
    inner class DecimalType {

        @Test
        fun `reads decimal values correctly`() {
            val schema = DeltaType.StructType(
                listOf(StructField("amount", DeltaType.DecimalType(10, 2), nullable = false))
            )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)

            val dec = BigDecimal("123.45")
            val records = listOf(
                GenericRecordBuilder(avroSchema)
                    .set("amount", ByteBuffer.wrap(dec.unscaledValue().toByteArray()))
                    .build()
            )
            val writer = ParquetFileWriter(logStore, schema)
            writer.write("data/decimal.parquet", records)

            val readRecords = reader.read("data/decimal.parquet")
            readRecords shouldHaveSize 1

            val readBuf = readRecords[0].get("amount") as ByteBuffer
            val readBytes = ByteArray(readBuf.remaining())
            readBuf.get(readBytes)
            val readDecimal = BigDecimal(java.math.BigInteger(readBytes), 2)
            readDecimal shouldBe dec
        }
    }

    @Nested
    inner class LargeFile {

        @Test
        fun `reads large file without issue`() {
            val schema = DeltaType.StructType(
                listOf(
                    StructField("id", DeltaType.IntegerType, nullable = false),
                    StructField("value", DeltaType.StringType, nullable = true)
                )
            )
            val count = 10_000
            writeTestFile("data/large.parquet", schema, (0 until count).map { i ->
                mapOf("id" to i, "value" to "row-$i")
            })

            val records = reader.read("data/large.parquet")

            records shouldHaveSize count
            records.first().get("id") shouldBe 0
            records.last().get("id") shouldBe count - 1
        }

        @Test
        fun `streams large file via iterator`() {
            val schema = DeltaType.StructType(
                listOf(StructField("id", DeltaType.IntegerType, nullable = false))
            )
            val count = 10_000
            writeTestFile("data/large-iter.parquet", schema, (0 until count).map { mapOf("id" to it) })

            var recordCount = 0
            reader.readIterator("data/large-iter.parquet").use { iter ->
                while (iter.hasNext()) {
                    iter.next()
                    recordCount++
                }
            }

            recordCount shouldBe count
        }
    }

    @Nested
    inner class ComplexTypes {

        @Test
        fun `reads arrays and maps`() {
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
                    .set("props", mapOf("key" to "val"))
                    .build()
            )
            val writer = ParquetFileWriter(logStore, schema)
            writer.write("data/complex.parquet", records)

            val readRecords = reader.read("data/complex.parquet")

            readRecords shouldHaveSize 1
            readRecords[0].get("id") shouldBe 1
            @Suppress("UNCHECKED_CAST")
            val tags = readRecords[0].get("tags") as List<Any>
            tags.map { it.toString() } shouldBe listOf("a", "b")
        }
    }

    /**
     * Write a test Parquet file using ParquetFileWriter.
     * Values map keys must match field names in the schema.
     */
    private fun writeTestFile(
        filePath: String,
        schema: DeltaType.StructType,
        rows: List<Map<String, Any?>>
    ) {
        val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)
        val records = rows.map { row ->
            val builder = GenericRecordBuilder(avroSchema)
            for ((key, value) in row) {
                builder.set(key, value)
            }
            builder.build()
        }
        val writer = ParquetFileWriter(logStore, schema)
        writer.write(filePath, records)
    }
}
