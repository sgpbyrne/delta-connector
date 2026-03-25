package com.deltaconnect.protocol.merge

import com.deltaconnect.protocol.schema.AvroToDeltaConverter
import com.deltaconnect.protocol.schema.DeltaType
import com.deltaconnect.protocol.schema.StructField
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import org.apache.avro.generic.GenericRecordBuilder
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

class RowMergerTest {
    private val schema =
        DeltaType.StructType(
            listOf(
                StructField("id", DeltaType.IntegerType, nullable = false),
                StructField("name", DeltaType.StringType, nullable = true),
            ),
        )
    private val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)
    private val mergeKeyNames = listOf("id")

    private fun record(
        id: Int,
        name: String?,
    ): org.apache.avro.generic.GenericRecord = GenericRecordBuilder(avroSchema).set("id", id).set("name", name).build()

    private fun buildIndex(vararg entries: Triple<Int, String?, MergeOperation>): Map<MergeKey, IndexedSourceRecord> {
        val index = mutableMapOf<MergeKey, IndexedSourceRecord>()
        for ((ordinal, entry) in entries.withIndex()) {
            val (id, name, op) = entry
            val rec = record(id, name)
            index[MergeKey(listOf(id))] = IndexedSourceRecord(rec, op, ordinal)
        }
        return index
    }

    @Nested
    inner class UpdateExistingRows {
        @Test
        fun `replaces matched row with source record on UPDATE`() {
            val sourceIndex = buildIndex(Triple(1, "ALICE", MergeOperation.UPDATE))
            val merger = RowMerger(sourceIndex, mergeKeyNames)
            val matchedKeys = mutableSetOf<MergeKey>()

            val existing = listOf(record(1, "Alice"), record(2, "Bob"))
            val result = merger.mergeFile(existing.iterator(), matchedKeys)

            result.updatedCount shouldBe 1
            result.copiedCount shouldBe 1
            result.deletedCount shouldBe 0
            result.outputRows shouldHaveSize 2
            result.outputRows[0].get("name").toString() shouldBe "ALICE"
            result.outputRows[1].get("name").toString() shouldBe "Bob"
            matchedKeys shouldHaveSize 1
        }
    }

    @Nested
    inner class DeleteExistingRows {
        @Test
        fun `removes matched row on DELETE`() {
            val sourceIndex = buildIndex(Triple(2, null, MergeOperation.DELETE))
            val merger = RowMerger(sourceIndex, mergeKeyNames)
            val matchedKeys = mutableSetOf<MergeKey>()

            val existing = listOf(record(1, "Alice"), record(2, "Bob"))
            val result = merger.mergeFile(existing.iterator(), matchedKeys)

            result.deletedCount shouldBe 1
            result.copiedCount shouldBe 1
            result.updatedCount shouldBe 0
            result.outputRows shouldHaveSize 1
            result.outputRows[0].get("name").toString() shouldBe "Alice"
            matchedKeys shouldHaveSize 1
        }

        @Test
        fun `all rows deleted produces empty output`() {
            val sourceIndex =
                buildIndex(
                    Triple(1, null, MergeOperation.DELETE),
                    Triple(2, null, MergeOperation.DELETE),
                )
            val merger = RowMerger(sourceIndex, mergeKeyNames)
            val matchedKeys = mutableSetOf<MergeKey>()

            val existing = listOf(record(1, "Alice"), record(2, "Bob"))
            val result = merger.mergeFile(existing.iterator(), matchedKeys)

            result.deletedCount shouldBe 2
            result.copiedCount shouldBe 0
            result.outputRows.shouldBeEmpty()
            matchedKeys shouldHaveSize 2
        }
    }

    @Nested
    inner class PassThrough {
        @Test
        fun `unmatched rows pass through unchanged`() {
            val sourceIndex = buildIndex(Triple(99, "New", MergeOperation.INSERT))
            val merger = RowMerger(sourceIndex, mergeKeyNames)
            val matchedKeys = mutableSetOf<MergeKey>()

            val existing = listOf(record(1, "Alice"), record(2, "Bob"))
            val result = merger.mergeFile(existing.iterator(), matchedKeys)

            result.copiedCount shouldBe 2
            result.updatedCount shouldBe 0
            result.deletedCount shouldBe 0
            result.outputRows shouldHaveSize 2
            matchedKeys.shouldBeEmpty()
        }
    }

    @Nested
    inner class MixedOperations {
        @Test
        fun `handles update delete and pass-through in single file`() {
            val sourceIndex =
                buildIndex(
                    Triple(1, "ALICE", MergeOperation.UPDATE),
                    Triple(2, null, MergeOperation.DELETE),
                )
            val merger = RowMerger(sourceIndex, mergeKeyNames)
            val matchedKeys = mutableSetOf<MergeKey>()

            val existing = listOf(record(1, "Alice"), record(2, "Bob"), record(3, "Charlie"))
            val result = merger.mergeFile(existing.iterator(), matchedKeys)

            result.updatedCount shouldBe 1
            result.deletedCount shouldBe 1
            result.copiedCount shouldBe 1
            result.outputRows shouldHaveSize 2
            result.outputRows
                .map { it.get("name").toString() }
                .shouldContainExactlyInAnyOrder("ALICE", "Charlie")
        }
    }

    @Nested
    inner class InsertMatchesExisting {
        @Test
        fun `INSERT on existing key treated as replacement`() {
            val sourceIndex = buildIndex(Triple(1, "ALICE", MergeOperation.INSERT))
            val merger = RowMerger(sourceIndex, mergeKeyNames)
            val matchedKeys = mutableSetOf<MergeKey>()

            val existing = listOf(record(1, "Alice"))
            val result = merger.mergeFile(existing.iterator(), matchedKeys)

            result.updatedCount shouldBe 1
            result.outputRows shouldHaveSize 1
            result.outputRows[0].get("name").toString() shouldBe "ALICE"
            matchedKeys shouldHaveSize 1
        }
    }

    @Nested
    inner class CompositeKeyMerge {
        @Test
        fun `matches on composite key correctly`() {
            val sourceRec = CompositeKeyFixtures.record("A", 1, "ALICE")
            val sourceIndex =
                mapOf(
                    MergeKey(listOf("A", 1)) to
                        IndexedSourceRecord(
                            sourceRec,
                            MergeOperation.UPDATE,
                            0,
                        ),
                )
            val merger = RowMerger(sourceIndex, CompositeKeyFixtures.keyNames)
            val matchedKeys = mutableSetOf<MergeKey>()

            val existing =
                listOf(
                    CompositeKeyFixtures.record("A", 1, "Alice"),
                    CompositeKeyFixtures.record("A", 2, "Bob"),
                    CompositeKeyFixtures.record("B", 1, "Charlie"),
                )
            val result = merger.mergeFile(existing.iterator(), matchedKeys)

            result.updatedCount shouldBe 1
            result.copiedCount shouldBe 2
            result.outputRows shouldHaveSize 3
            matchedKeys shouldBe setOf(MergeKey(listOf("A", 1)))
        }
    }

    @Nested
    inner class NullKeyValues {
        @Test
        fun `null merge key values match correctly`() {
            val nullIdSchema =
                DeltaType.StructType(
                    listOf(
                        StructField("id", DeltaType.IntegerType, nullable = true),
                        StructField("name", DeltaType.StringType, nullable = true),
                    ),
                )
            val nullIdAvro = AvroToDeltaConverter.toAvroSchema(nullIdSchema)
            val existingRow =
                GenericRecordBuilder(nullIdAvro)
                    .set("id", null)
                    .set("name", "Ghost")
                    .build()
            val sourceRow =
                GenericRecordBuilder(nullIdAvro)
                    .set("id", null)
                    .set("name", "GHOST")
                    .build()

            val sourceIndex =
                mapOf(
                    MergeKey(listOf(null)) to
                        IndexedSourceRecord(
                            sourceRow,
                            MergeOperation.UPDATE,
                            0,
                        ),
                )
            val merger = RowMerger(sourceIndex, listOf("id"))
            val matchedKeys = mutableSetOf<MergeKey>()

            val result = merger.mergeFile(listOf(existingRow).iterator(), matchedKeys)

            result.updatedCount shouldBe 1
            result.outputRows shouldHaveSize 1
            result.outputRows[0].get("name").toString() shouldBe "GHOST"
        }
    }

    @Nested
    inner class MatchedKeysAcrossFiles {
        @Test
        fun `matched keys accumulate across multiple mergeFile calls`() {
            val sourceIndex =
                buildIndex(
                    Triple(1, "ALICE", MergeOperation.UPDATE),
                    Triple(2, "BOB", MergeOperation.UPDATE),
                    Triple(3, "CHARLIE", MergeOperation.UPDATE),
                )
            val merger = RowMerger(sourceIndex, mergeKeyNames)
            val matchedKeys = mutableSetOf<MergeKey>()

            val file1Rows = listOf(record(1, "Alice"))
            merger.mergeFile(file1Rows.iterator(), matchedKeys)
            matchedKeys shouldHaveSize 1

            val file2Rows = listOf(record(2, "Bob"), record(3, "Charlie"))
            merger.mergeFile(file2Rows.iterator(), matchedKeys)
            matchedKeys shouldHaveSize 3

            matchedKeys shouldBe
                setOf(
                    MergeKey(listOf(1)),
                    MergeKey(listOf(2)),
                    MergeKey(listOf(3)),
                )
        }
    }

    @Nested
    inner class ExtractKey {
        @Test
        fun `normalizes Avro Utf8 to String for consistent equality`() {
            val merger = RowMerger(emptyMap(), listOf("name"))
            val rec = record(1, "Alice")

            val key = merger.extractKey(rec)

            key.values[0] shouldBe "Alice"
            key.values[0]!!::class shouldBe String::class
        }
    }
}
