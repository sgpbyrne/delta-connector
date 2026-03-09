package com.deltaconnect.protocol.merge

import com.deltaconnect.protocol.DeltaSnapshot
import com.deltaconnect.protocol.actions.AddFile
import com.deltaconnect.protocol.actions.Format
import com.deltaconnect.protocol.actions.MetaData
import com.deltaconnect.protocol.actions.Protocol
import com.deltaconnect.protocol.parquet.ParquetFileReader
import com.deltaconnect.protocol.parquet.ParquetFileWriter
import com.deltaconnect.protocol.schema.AvroToDeltaConverter
import com.deltaconnect.protocol.schema.DeltaSchema
import com.deltaconnect.protocol.schema.DeltaType
import com.deltaconnect.protocol.schema.StructField
import com.deltaconnect.protocol.storage.LocalFileSystemLogStore
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.longs.shouldBeGreaterThan
import io.kotest.matchers.shouldBe
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path

class DeltaMergeEngineTest {

    @TempDir
    lateinit var tempDir: Path

    private lateinit var logStore: LocalFileSystemLogStore
    private lateinit var reader: ParquetFileReader

    private val schema = DeltaType.StructType(
        listOf(
            StructField("id", DeltaType.IntegerType, nullable = false),
            StructField("name", DeltaType.StringType, nullable = true),
            StructField("value", DeltaType.IntegerType, nullable = true)
        )
    )
    private val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)
    private val tablePath = "test-table"
    private val mergeKeyNames = listOf("id")

    @BeforeEach
    fun setUp() {
        logStore = LocalFileSystemLogStore(tempDir)
        reader = ParquetFileReader(logStore)
    }

    private fun record(id: Int, name: String?, value: Int? = null): GenericRecord =
        GenericRecordBuilder(avroSchema)
            .set("id", id)
            .set("name", name)
            .set("value", value)
            .build()

    private fun sourceRecord(
        id: Int,
        name: String?,
        value: Int? = null,
        op: MergeOperation
    ): SourceRecord = SourceRecord(record(id, name, value), op)

    private fun writeDataFile(
        path: String,
        records: List<GenericRecord>
    ): AddFile {
        val writer = ParquetFileWriter(logStore, schema)
        val result = writer.write(path, records)
        return AddFile(
            path = result.filePath,
            size = result.fileSize,
            modificationTime = System.currentTimeMillis(),
            dataChange = true,
            stats = result.statsJson
        )
    }

    private fun snapshot(vararg files: AddFile): DeltaSnapshot = DeltaSnapshot(
        version = 0L,
        activeFiles = files.toSet(),
        metaData = MetaData(
            id = "test",
            format = Format(),
            schemaString = DeltaSchema.toJson(schema)
        ),
        protocol = Protocol(),
        transactions = emptyMap(),
        commitInfo = null
    )

    private fun engine(): DeltaMergeEngine =
        DeltaMergeEngine(logStore, schema, mergeKeyNames, tablePath)

    private fun readRecords(addFile: AddFile): List<GenericRecord> =
        reader.read(addFile.path)

    private fun extractIds(records: List<GenericRecord>): List<Int> =
        records.map { it.get("id") as Int }

    @Nested
    inner class InsertOnly {

        @Test
        fun `all new keys inserted into new file`() {
            val file = writeDataFile(
                "$tablePath/part-existing.parquet",
                listOf(record(1, "Alice"), record(2, "Bob"), record(3, "Charlie"))
            )
            val snap = snapshot(file)
            val source = listOf(
                sourceRecord(4, "Dave", op = MergeOperation.INSERT),
                sourceRecord(5, "Elsa", op = MergeOperation.INSERT)
            )

            val result = engine().merge(snap, source)

            result.recordsInserted shouldBe 2
            result.recordsUpdated shouldBe 0
            result.recordsDeleted shouldBe 0
            result.filesRewritten shouldBe 0
            result.removeFiles.shouldBeEmpty()
            result.addFiles shouldHaveSize 1
            val newRecords = readRecords(result.addFiles[0])
            extractIds(newRecords).shouldContainExactlyInAnyOrder(4, 5)
        }

        @Test
        fun `insert into empty table`() {
            val snap = snapshot()
            val source = listOf(
                sourceRecord(1, "Alice", op = MergeOperation.INSERT),
                sourceRecord(2, "Bob", op = MergeOperation.INSERT)
            )

            val result = engine().merge(snap, source)

            result.recordsInserted shouldBe 2
            result.filesRewritten shouldBe 0
            result.filesSkipped shouldBe 0
            result.addFiles shouldHaveSize 1
            result.removeFiles.shouldBeEmpty()
        }
    }

    @Nested
    inner class UpdateOnly {

        @Test
        fun `all existing keys updated`() {
            val file = writeDataFile(
                "$tablePath/part-existing.parquet",
                listOf(record(1, "Alice"), record(2, "Bob"))
            )
            val snap = snapshot(file)
            val source = listOf(
                sourceRecord(1, "ALICE", op = MergeOperation.UPDATE),
                sourceRecord(2, "BOB", op = MergeOperation.UPDATE)
            )

            val result = engine().merge(snap, source)

            result.recordsUpdated shouldBe 2
            result.recordsInserted shouldBe 0
            result.recordsDeleted shouldBe 0
            result.recordsCopied shouldBe 0
            result.filesRewritten shouldBe 1
            result.removeFiles shouldHaveSize 1
            result.removeFiles[0].path shouldBe file.path
            result.addFiles shouldHaveSize 1

            val newRecords = readRecords(result.addFiles[0])
            newRecords shouldHaveSize 2
            newRecords.map { it.get("name").toString() }
                .shouldContainExactlyInAnyOrder("ALICE", "BOB")
        }
    }

    @Nested
    inner class DeleteOnly {

        @Test
        fun `all existing keys deleted produces RemoveFile with no AddFile`() {
            val file = writeDataFile(
                "$tablePath/part-existing.parquet",
                listOf(record(1, "Alice"), record(2, "Bob"))
            )
            val snap = snapshot(file)
            val source = listOf(
                sourceRecord(1, null, op = MergeOperation.DELETE),
                sourceRecord(2, null, op = MergeOperation.DELETE)
            )

            val result = engine().merge(snap, source)

            result.recordsDeleted shouldBe 2
            result.recordsInserted shouldBe 0
            result.recordsCopied shouldBe 0
            result.filesRewritten shouldBe 1
            result.removeFiles shouldHaveSize 1
            result.removeFiles[0].path shouldBe file.path
            // No AddFile since all rows deleted
            result.addFiles.shouldBeEmpty()
        }

        @Test
        fun `partial delete within file`() {
            val file = writeDataFile(
                "$tablePath/part-existing.parquet",
                listOf(record(1, "Alice"), record(2, "Bob"), record(3, "Charlie"))
            )
            val snap = snapshot(file)
            val source = listOf(
                sourceRecord(2, null, op = MergeOperation.DELETE)
            )

            val result = engine().merge(snap, source)

            result.recordsDeleted shouldBe 1
            result.recordsCopied shouldBe 2
            result.removeFiles shouldHaveSize 1
            result.addFiles shouldHaveSize 1
            val remaining = readRecords(result.addFiles[0])
            extractIds(remaining).shouldContainExactlyInAnyOrder(1, 3)
        }
    }

    @Nested
    inner class MixedOperations {

        @Test
        fun `insert update and delete in same batch`() {
            val file = writeDataFile(
                "$tablePath/part-existing.parquet",
                listOf(
                    record(1, "Alice", 10),
                    record(2, "Bob", 20),
                    record(3, "Charlie", 30)
                )
            )
            val snap = snapshot(file)
            val source = listOf(
                sourceRecord(1, "ALICE", 100, MergeOperation.UPDATE),
                sourceRecord(2, null, null, MergeOperation.DELETE),
                sourceRecord(4, "Dave", 40, MergeOperation.INSERT)
            )

            val result = engine().merge(snap, source)

            result.recordsUpdated shouldBe 1
            result.recordsDeleted shouldBe 1
            result.recordsInserted shouldBe 1
            result.recordsCopied shouldBe 1 // id=3 unchanged
            result.filesRewritten shouldBe 1

            result.addFiles shouldHaveSize 2
            result.removeFiles shouldHaveSize 1

            val allNewRecords = result.addFiles.flatMap { readRecords(it) }
            extractIds(allNewRecords).shouldContainExactlyInAnyOrder(1, 3, 4)
        }
    }

    @Nested
    inner class Deduplication {

        @Test
        fun `keeps latest record when same key appears multiple times`() {
            val file = writeDataFile(
                "$tablePath/part-existing.parquet",
                listOf(record(1, "Alice"))
            )
            val snap = snapshot(file)
            val source = listOf(
                sourceRecord(1, "B", op = MergeOperation.UPDATE),
                sourceRecord(1, "C", op = MergeOperation.UPDATE)
            )

            val result = engine().merge(snap, source)

            result.recordsUpdated shouldBe 1
            result.addFiles shouldHaveSize 1
            val newRecords = readRecords(result.addFiles[0])
            newRecords[0].get("name").toString() shouldBe "C"
        }

        @Test
        fun `delete after insert for same key results in no insert`() {
            val snap = snapshot() // empty table
            val source = listOf(
                sourceRecord(1, "Alice", op = MergeOperation.INSERT),
                sourceRecord(1, null, op = MergeOperation.DELETE)
            )

            val result = engine().merge(snap, source)

            result.recordsInserted shouldBe 0
            result.addFiles.shouldBeEmpty()
        }

        @Test
        fun `insert after delete for same key replaces existing row`() {
            val file = writeDataFile(
                "$tablePath/part-existing.parquet",
                listOf(record(1, "Alice"))
            )
            val snap = snapshot(file)
            val source = listOf(
                sourceRecord(1, null, op = MergeOperation.DELETE),
                sourceRecord(1, "Bob", op = MergeOperation.INSERT)
            )

            val result = engine().merge(snap, source)

            result.recordsUpdated shouldBe 1
            result.addFiles shouldHaveSize 1
            val newRecords = readRecords(result.addFiles[0])
            newRecords[0].get("name").toString() shouldBe "Bob"
        }
    }

    @Nested
    inner class FilePruningEffectiveness {

        @Test
        fun `files outside key range are not rewritten`() {
            val f1 = writeDataFile(
                "$tablePath/part-low.parquet",
                listOf(record(1, "A"), record(2, "B"), record(3, "C"))
            )
            val f2 = writeDataFile(
                "$tablePath/part-mid.parquet",
                listOf(record(10, "J"), record(11, "K"), record(12, "L"))
            )
            val f3 = writeDataFile(
                "$tablePath/part-high.parquet",
                listOf(record(100, "X"), record(101, "Y"), record(102, "Z"))
            )
            val snap = snapshot(f1, f2, f3)
            val source = listOf(
                sourceRecord(10, "J-UPDATED", op = MergeOperation.UPDATE),
                sourceRecord(50, "NEW", op = MergeOperation.INSERT)
            )

            val result = engine().merge(snap, source)

            result.filesSkipped shouldBe 2
            result.recordsUpdated shouldBe 1
            result.recordsInserted shouldBe 1
            result.filesRewritten shouldBe 1
            result.removeFiles shouldHaveSize 1
            result.removeFiles[0].path shouldBe f2.path
        }
    }

    @Nested
    inner class EmptyMerge {

        @Test
        fun `no-op when source batch is empty`() {
            val file = writeDataFile(
                "$tablePath/part-existing.parquet",
                listOf(record(1, "Alice"))
            )
            val snap = snapshot(file)

            val result = engine().merge(snap, emptyList())

            result.recordsInserted shouldBe 0
            result.recordsUpdated shouldBe 0
            result.recordsDeleted shouldBe 0
            result.filesRewritten shouldBe 0
            result.filesCreated shouldBe 0
            result.addFiles.shouldBeEmpty()
            result.removeFiles.shouldBeEmpty()
        }

        @Test
        fun `source matches no existing rows - all treated as inserts`() {
            val file = writeDataFile(
                "$tablePath/part-existing.parquet",
                listOf(record(1, "Alice"), record(2, "Bob"))
            )
            val snap = snapshot(file)
            val source = listOf(
                sourceRecord(100, "New", op = MergeOperation.INSERT)
            )

            val result = engine().merge(snap, source)

            result.recordsInserted shouldBe 1
            result.recordsUpdated shouldBe 0
            result.filesRewritten shouldBe 0
        }
    }

    @Nested
    inner class CompositeKeys {

        @Test
        fun `merge with two-column composite key`() {
            val compositeSchema = CompositeKeyFixtures.schema
            val writer = ParquetFileWriter(logStore, compositeSchema)
            val result = writer.write(
                "$tablePath/part-composite.parquet",
                listOf(
                    CompositeKeyFixtures.record("A", 1, "Alice"),
                    CompositeKeyFixtures.record("A", 2, "Bob"),
                    CompositeKeyFixtures.record("B", 1, "Charlie")
                )
            )
            val file = AddFile(
                path = result.filePath,
                size = result.fileSize,
                modificationTime = System.currentTimeMillis(),
                dataChange = true,
                stats = result.statsJson
            )
            val snap = DeltaSnapshot(
                version = 0L,
                activeFiles = setOf(file),
                metaData = MetaData(
                    id = "test",
                    format = Format(),
                    schemaString = DeltaSchema.toJson(compositeSchema)
                ),
                protocol = Protocol(),
                transactions = emptyMap(),
                commitInfo = null
            )

            val engine = DeltaMergeEngine(
                logStore, compositeSchema,
                CompositeKeyFixtures.keyNames, tablePath
            )
            val source = listOf(
                SourceRecord(
                    CompositeKeyFixtures.record("A", 1, "ALICE"),
                    MergeOperation.UPDATE
                )
            )

            val mergeResult = engine.merge(snap, source)

            mergeResult.recordsUpdated shouldBe 1
            mergeResult.recordsCopied shouldBe 2
            mergeResult.filesRewritten shouldBe 1

            val newRecords = reader.read(mergeResult.addFiles[0].path)
            newRecords shouldHaveSize 3
            val updatedRow = newRecords.find {
                it.get("tenant_id").toString() == "A" && it.get("id") == 1
            }!!
            updatedRow.get("name").toString() shouldBe "ALICE"
        }
    }

    @Nested
    inner class MultipleExistingFiles {

        @Test
        fun `processes changes across multiple files`() {
            val f1 = writeDataFile(
                "$tablePath/part-1.parquet",
                listOf(record(1, "A"), record(2, "B"), record(3, "C"))
            )
            val f2 = writeDataFile(
                "$tablePath/part-2.parquet",
                listOf(record(4, "D"), record(5, "E"), record(6, "F"))
            )
            val snap = snapshot(f1, f2)
            val source = listOf(
                sourceRecord(2, null, op = MergeOperation.DELETE),
                sourceRecord(5, "E-UPDATED", op = MergeOperation.UPDATE),
                sourceRecord(7, "G", op = MergeOperation.INSERT)
            )

            val result = engine().merge(snap, source)

            result.filesRewritten shouldBe 2
            result.removeFiles shouldHaveSize 2
            result.recordsDeleted shouldBe 1
            result.recordsUpdated shouldBe 1
            result.recordsInserted shouldBe 1
            result.recordsCopied shouldBe 4 // ids 1,3 from f1 + ids 4,6 from f2

            val allNewRecords = result.addFiles.flatMap { readRecords(it) }
            extractIds(allNewRecords)
                .shouldContainExactlyInAnyOrder(1, 3, 4, 5, 6, 7)
        }
    }

    @Nested
    inner class AddFileAndRemoveFileProperties {

        @Test
        fun `AddFile has correct stats and size`() {
            val file = writeDataFile(
                "$tablePath/part-existing.parquet",
                listOf(record(1, "Alice"))
            )
            val snap = snapshot(file)
            val source = listOf(
                sourceRecord(1, "ALICE", op = MergeOperation.UPDATE)
            )

            val result = engine().merge(snap, source)

            result.addFiles shouldHaveSize 1
            val addFile = result.addFiles[0]
            addFile.size shouldBeGreaterThan 0
            addFile.dataChange shouldBe true
            addFile.stats!!.contains("numRecords") shouldBe true
            addFile.path.startsWith(tablePath) shouldBe true
            addFile.path.endsWith(".parquet") shouldBe true
        }

        @Test
        fun `RemoveFile captures original file metadata`() {
            val file = writeDataFile(
                "$tablePath/part-existing.parquet",
                listOf(record(1, "Alice"))
            )
            val snap = snapshot(file)
            val source = listOf(
                sourceRecord(1, null, op = MergeOperation.DELETE)
            )

            val result = engine().merge(snap, source)

            result.removeFiles shouldHaveSize 1
            val removeFile = result.removeFiles[0]
            removeFile.path shouldBe file.path
            removeFile.dataChange shouldBe true
            removeFile.size shouldBe file.size
        }
    }

    @Nested
    inner class NullHandling {

        @Test
        fun `null values in non-key columns preserved through merge`() {
            val file = writeDataFile(
                "$tablePath/part-existing.parquet",
                listOf(record(1, null, null))
            )
            val snap = snapshot(file)
            val source = listOf(
                sourceRecord(2, null, null, MergeOperation.INSERT)
            )

            val result = engine().merge(snap, source)

            result.recordsInserted shouldBe 1
            val newRecords = readRecords(result.addFiles[0])
            newRecords[0].get("name") shouldBe null
            newRecords[0].get("value") shouldBe null
        }
    }

    @Nested
    inner class UnmatchedDeleteIgnored {

        @Test
        fun `unmatched delete does not produce insert`() {
            val file = writeDataFile(
                "$tablePath/part-existing.parquet",
                listOf(record(1, "Alice"))
            )
            val snap = snapshot(file)
            val source = listOf(
                sourceRecord(99, null, op = MergeOperation.DELETE)
            )

            val result = engine().merge(snap, source)

            result.recordsInserted shouldBe 0
            result.recordsDeleted shouldBe 0
            result.filesRewritten shouldBe 0
            result.addFiles.shouldBeEmpty()
            result.removeFiles.shouldBeEmpty()
        }
    }

    @Nested
    inner class LargeBatch {

        @Test
        fun `handles large batch across multiple files`() {
            val files = (0 until 5).map { fileIdx ->
                val start = fileIdx * 200
                val records = (start until start + 200).map { id ->
                    record(id, "name-$id", id * 10)
                }
                writeDataFile("$tablePath/part-$fileIdx.parquet", records)
            }
            val snap = snapshot(*files.toTypedArray())

            val source = (0 until 1000 step 10).map { id ->
                sourceRecord(id, "UPDATED-$id", id * 100, MergeOperation.UPDATE)
            }

            val result = engine().merge(snap, source)

            result.recordsUpdated shouldBe 100
            result.recordsCopied shouldBe 900
            result.recordsInserted shouldBe 0
            result.filesRewritten shouldBe 5

            val allNewRecords = result.addFiles.flatMap { readRecords(it) }
            allNewRecords shouldHaveSize 1000

            val rec0 = allNewRecords.find { it.get("id") == 0 }!!
            rec0.get("name").toString() shouldBe "UPDATED-0"
            rec0.get("value") shouldBe 0
        }
    }

    @Nested
    inner class InvalidMergeKey {

        @Test
        fun `throws when merge key column not in schema`() {
            val exception = org.junit.jupiter.api.assertThrows<IllegalArgumentException> {
                DeltaMergeEngine(logStore, schema, listOf("nonexistent"), tablePath)
            }
            exception.message shouldBe "Merge key column 'nonexistent' not found in schema"
        }
    }
}
