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
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir
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

        @Test
        fun `next on exhausted iterator throws NoSuchElementException`() {
            val schema = DeltaType.StructType(
                listOf(StructField("id", DeltaType.IntegerType, nullable = false))
            )
            writeTestFile("data/exhausted.parquet", schema, listOf(mapOf("id" to 1)))

            val iter = reader.readIterator("data/exhausted.parquet")
            iter.use {
                it.next() // consume the single record
                assertThrows<NoSuchElementException> { it.next() }
            }
        }
    }


    @Nested
    inner class LargeFile {

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
