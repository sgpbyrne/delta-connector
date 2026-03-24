package com.deltaconnect.connect

import com.deltaconnect.protocol.DeltaTable
import com.deltaconnect.protocol.merge.MergeOperation
import com.deltaconnect.protocol.merge.SourceRecord
import com.deltaconnect.protocol.schema.DeltaSchema
import com.deltaconnect.protocol.schema.DeltaType
import com.deltaconnect.protocol.storage.LocalFileSystemLogStore
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.longs.shouldBeGreaterThan
import io.kotest.matchers.maps.shouldBeEmpty
import io.kotest.matchers.maps.shouldContainKey
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path

class TableWriterTest {

    @TempDir
    lateinit var tempDir: Path

    private lateinit var logStore: LocalFileSystemLogStore

    private val avroSchema = SchemaBuilder.record("record")
        .namespace("com.deltaconnect")
        .fields()
        .requiredInt("id")
        .optionalString("name")
        .name("value").type().nullable().intType().noDefault()
        .endRecord()

    @BeforeEach
    fun setUp() {
        logStore = LocalFileSystemLogStore(tempDir)
    }

    @Test
    fun `flush creates table and writes data`() {
        val writer = mergeWriter("test_table")

        writer.buffer(sourceRecord(1, "alice", 100), tp(0), 0L)
        writer.buffer(sourceRecord(2, "bob", 200), tp(0), 1L)

        val offsets = writer.flush()
        offsets shouldContainKey tp(0)
        offsets[tp(0)] shouldBe 1L

        val snapshot = DeltaTable.forPath(logStore, "test_table").snapshot()
        snapshot.version shouldBeGreaterThan -1
        snapshot.activeFiles shouldHaveSize 1
    }

    @Test
    fun `flush with empty buffer returns empty offsets`() {
        val writer = mergeWriter("test_table")
        val offsets = writer.flush()
        offsets.shouldBeEmpty()
    }

    @Test
    fun `buffer tracks max offset per partition`() {
        val writer = mergeWriter("test_table")

        writer.buffer(sourceRecord(1, "alice", 100), tp(0), 5L)
        writer.buffer(sourceRecord(2, "bob", 200), tp(0), 3L) // lower offset
        writer.buffer(sourceRecord(3, "charlie", 300), tp(0), 7L)

        writer.bufferSize() shouldBe 3

        val offsets = writer.flush()
        offsets[tp(0)] shouldBe 7L // max offset
    }

    @Test
    fun `flush tracks offsets for multiple partitions`() {
        val writer = mergeWriter("test_table")

        writer.buffer(sourceRecord(1, "alice", 100), tp(0), 10L)
        writer.buffer(sourceRecord(2, "bob", 200), tp(1), 20L)
        writer.buffer(sourceRecord(3, "charlie", 300), tp(2), 30L)

        val offsets = writer.flush()
        offsets shouldContainKey tp(0)
        offsets shouldContainKey tp(1)
        offsets shouldContainKey tp(2)
        offsets[tp(0)] shouldBe 10L
        offsets[tp(1)] shouldBe 20L
        offsets[tp(2)] shouldBe 30L
    }

    @Test
    fun `flush writes SetTransaction for each partition`() {
        val writer = mergeWriter("test_table")

        writer.buffer(sourceRecord(1, "alice", 100), tp(0), 5L)
        writer.buffer(sourceRecord(2, "bob", 200), tp(1), 10L)
        writer.flush()

        val snapshot = DeltaTable.forPath(logStore, "test_table").snapshot()
        snapshot.transactions shouldContainKey TableWriter.makeAppId(tp(0))
        snapshot.transactions shouldContainKey TableWriter.makeAppId(tp(1))
        snapshot.transactions[TableWriter.makeAppId(tp(0))]!!.version shouldBe 5L
        snapshot.transactions[TableWriter.makeAppId(tp(1))]!!.version shouldBe 10L
    }

    @Test
    fun `merge updates existing records`() {
        val writer = mergeWriter("test_table")

        writer.buffer(sourceRecord(1, "alice", 100, MergeOperation.INSERT), tp(0), 0L)
        writer.buffer(sourceRecord(2, "bob", 200, MergeOperation.INSERT), tp(0), 1L)
        writer.flush()

        writer.buffer(sourceRecord(1, "alice_updated", 150, MergeOperation.UPDATE), tp(0), 2L)
        writer.flush()

        val snapshot = DeltaTable.forPath(logStore, "test_table").snapshot()
        snapshot.version shouldBe 2L
    }

    @Test
    fun `merge deletes records`() {
        val writer = mergeWriter("test_table")

        writer.buffer(sourceRecord(1, "alice", 100), tp(0), 0L)
        writer.buffer(sourceRecord(2, "bob", 200), tp(0), 1L)
        writer.flush()

        writer.buffer(sourceRecord(2, "bob", 200, MergeOperation.DELETE), tp(0), 2L)
        writer.flush()

        val snapshot = DeltaTable.forPath(logStore, "test_table").snapshot()
        snapshot.version shouldBeGreaterThan 0
    }

    @Test
    fun `append-only mode creates separate files`() {
        val writer = appendWriter("test_table")

        writer.buffer(sourceRecord(1, "alice", 100), tp(0), 0L)
        writer.flush()

        writer.buffer(sourceRecord(2, "bob", 200), tp(0), 1L)
        writer.flush()

        val snapshot = DeltaTable.forPath(logStore, "test_table").snapshot()
        snapshot.activeFiles shouldHaveSize 2 // separate file per flush
    }

    @Test
    fun `getCommittedOffset returns null for new table`() {
        val writer = mergeWriter("test_table")
        writer.getCommittedOffset(tp(0)).shouldBeNull()
    }

    @Test
    fun `getCommittedOffset returns committed offset`() {
        val writer = mergeWriter("test_table")

        writer.buffer(sourceRecord(1, "alice", 100), tp(0), 42L)
        writer.flush()

        writer.getCommittedOffset(tp(0)) shouldBe 42L
    }

    @Test
    fun `getCommittedOffset returns null for untracked partition`() {
        val writer = mergeWriter("test_table")

        writer.buffer(sourceRecord(1, "alice", 100), tp(0), 42L)
        writer.flush()

        writer.getCommittedOffset(tp(1)).shouldBeNull()
    }

    @Test
    fun `hasBufferedRecords reflects buffer state`() {
        val writer = mergeWriter("test_table")

        writer.hasBufferedRecords() shouldBe false

        writer.buffer(sourceRecord(1, "alice", 100), tp(0), 0L)
        writer.hasBufferedRecords() shouldBe true

        writer.flush()
        writer.hasBufferedRecords() shouldBe false
    }

    @Test
    fun `makeAppId generates stable partition identifier`() {
        val tp = TopicPartition("orders", 3)
        TableWriter.makeAppId(tp) shouldBe "delta-sink:orders:3"
    }

    @Test
    fun `reuses existing table on subsequent flush`() {
        val writer = mergeWriter("test_table")

        writer.buffer(sourceRecord(1, "alice", 100), tp(0), 0L)
        writer.flush()

        val schema1 = DeltaTable.forPath(logStore, "test_table").snapshot().metaData!!.schemaString

        writer.buffer(sourceRecord(2, "bob", 200), tp(0), 1L)
        writer.flush()

        val schema2 = DeltaTable.forPath(logStore, "test_table").snapshot().metaData!!.schemaString
        schema2 shouldBe schema1 // same schema, table was reused
    }

    @Test
    fun `opens existing table created by another writer`() {
        val writer1 = mergeWriter("test_table")
        writer1.buffer(sourceRecord(1, "alice", 100), tp(0), 0L)
        writer1.flush()

        val writer2 = mergeWriter("test_table")
        writer2.buffer(sourceRecord(2, "bob", 200), tp(0), 1L)
        writer2.flush()

        val snapshot = DeltaTable.forPath(logStore, "test_table").snapshot()
        snapshot.version shouldBe 2L // version 0 = create, 1 = first merge, 2 = second merge
    }

    private fun mergeWriter(tablePath: String): TableWriter = TableWriter(
        logStore = logStore,
        tablePath = tablePath,
        mergeKeys = listOf("id"),
        writeMode = DeltaSinkConfig.WriteMode.CDC
    )

    private fun appendWriter(tablePath: String): TableWriter = TableWriter(
        logStore = logStore,
        tablePath = tablePath,
        mergeKeys = emptyList(),
        writeMode = DeltaSinkConfig.WriteMode.APPEND
    )

    private fun tp(partition: Int): TopicPartition = TopicPartition("test_topic", partition)

    private fun sourceRecord(
        id: Int,
        name: String,
        value: Int,
        op: MergeOperation = MergeOperation.INSERT
    ): SourceRecord {
        val record = GenericData.Record(avroSchema)
        record.put("id", id)
        record.put("name", name)
        record.put("value", value)
        return SourceRecord(record, op)
    }
}
