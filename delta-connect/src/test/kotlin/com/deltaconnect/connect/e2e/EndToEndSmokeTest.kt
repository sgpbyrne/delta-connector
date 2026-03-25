package com.deltaconnect.connect.e2e

import com.deltaconnect.connect.DeltaSinkConfig
import com.deltaconnect.connect.DeltaSinkTask
import com.deltaconnect.connect.TableWriter
import com.deltaconnect.connect.storage.LocalStorageProvider
import com.deltaconnect.connect.storage.StorageProviderRegistry
import com.deltaconnect.protocol.DeltaTable
import com.deltaconnect.protocol.parquet.ParquetFileReader
import com.deltaconnect.protocol.schema.DeltaSchema
import com.deltaconnect.protocol.storage.LocalFileSystemLogStore
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.maps.shouldContainKey
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTaskContext
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path

/**
 * End-to-end smoke tests validating the full pipeline:
 * SinkRecord - RecordConverter - DeltaSinkTask - TableWriter -
 * DeltaMergeEngine - Parquet files on local FS - read back and verify.
 *
 * Uses real components (no mocks except SinkTaskContext). Tests run
 * entirely in process with [LocalFileSystemLogStore].
 */
class EndToEndSmokeTest {
    @TempDir
    lateinit var tempDir: Path

    private lateinit var logStore: LocalFileSystemLogStore
    private lateinit var task: DeltaSinkTask
    private lateinit var mockContext: SinkTaskContext
    private var currentTime: Long = 1_000_000L

    private val dataSchema: Schema =
        SchemaBuilder
            .struct()
            .field("id", Schema.INT32_SCHEMA)
            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
            .field("value", Schema.OPTIONAL_INT32_SCHEMA)
            .optional()
            .build()

    private val envelopeSchema: Schema =
        SchemaBuilder
            .struct()
            .field("before", dataSchema)
            .field("after", dataSchema)
            .field("op", Schema.STRING_SCHEMA)
            .field("ts_ms", Schema.INT64_SCHEMA)
            .build()

    private val plainSchema: Schema =
        SchemaBuilder
            .struct()
            .field("id", Schema.INT32_SCHEMA)
            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
            .field("value", Schema.OPTIONAL_INT32_SCHEMA)
            .build()

    @BeforeEach
    fun setUp() {
        StorageProviderRegistry.clear()
        StorageProviderRegistry.register(LocalStorageProvider(tempDir))

        logStore = LocalFileSystemLogStore(tempDir)
        mockContext = mockk(relaxed = true)
    }

    @AfterEach
    fun tearDown() {
        StorageProviderRegistry.clear()
    }

    @Nested
    inner class CdcInsert {
        @Test
        fun `inserts batch of records and verifies Parquet data`() {
            val task = startTask(cdcProps())
            task.open(listOf(tp("orders", 0)))

            val records =
                (1..10).map { i ->
                    debeziumRecord(
                        id = i,
                        name = "user_$i",
                        value = i * 100,
                        op = "c",
                        topic = "orders",
                        partition = 0,
                        offset = i.toLong(),
                    )
                }
            task.put(records)
            task.stop()

            val rows = readAllRows("orders")
            rows shouldHaveSize 10
            val ids = rows.map { it.get("id") as Int }.sorted()
            ids shouldBe (1..10).toList()

            val snapshot = DeltaTable.forPath(logStore, tablePath("orders")).snapshot()
            snapshot.version shouldBe 1L
        }
    }

    @Nested
    inner class CdcUpdate {
        @Test
        fun `updates existing records via merge`() {
            val task = startTask(cdcProps(batchSize = 100))
            task.open(listOf(tp("orders", 0)))

            val inserts =
                (1..5).map { i ->
                    debeziumRecord(i, "user_$i", i * 100, "c", "orders", 0, i.toLong())
                }
            task.put(inserts)
            forceFlush(task)

            val updates =
                (1..3).map { i ->
                    debeziumRecord(
                        id = i,
                        name = "user_$i",
                        value = i * 999,
                        op = "u",
                        before = dataStruct(i, "user_$i", i * 100),
                        topic = "orders",
                        partition = 0,
                        offset = 10L + i,
                    )
                }
            task.put(updates)
            forceFlush(task)
            task.stop()

            val rows = readAllRows("orders")
            rows shouldHaveSize 5
            val rowMap = rows.associate { (it.get("id") as Int) to (it.get("value") as Int) }
            rowMap[1] shouldBe 999
            rowMap[2] shouldBe 1998
            rowMap[3] shouldBe 2997
            rowMap[4] shouldBe 400
            rowMap[5] shouldBe 500

            val snapshot = DeltaTable.forPath(logStore, tablePath("orders")).snapshot()
            snapshot.version shouldBe 2L
        }
    }

    @Nested
    inner class CdcDelete {
        @Test
        fun `deletes records via merge`() {
            val task = startTask(cdcProps(batchSize = 100))
            task.open(listOf(tp("orders", 0)))

            val inserts =
                (1..5).map { i ->
                    debeziumRecord(i, "user_$i", i * 100, "c", "orders", 0, i.toLong())
                }
            task.put(inserts)
            forceFlush(task)

            val deletes =
                listOf(2, 4).mapIndexed { idx, id ->
                    debeziumDelete(id, "user_$id", id * 100, "orders", 0, 10L + idx)
                }
            task.put(deletes)
            forceFlush(task)
            task.stop()

            val rows = readAllRows("orders")
            rows shouldHaveSize 3
            val ids = rows.map { it.get("id") as Int }.sorted()
            ids shouldBe listOf(1, 3, 5)
        }
    }

    @Nested
    inner class CdcMixedOperations {
        @Test
        fun `handles mixed insert, update, delete in one batch`() {
            val task = startTask(cdcProps(batchSize = 100))
            task.open(listOf(tp("orders", 0)))

            val inserts =
                (1..3).map { i ->
                    debeziumRecord(i, "user_$i", i * 100, "c", "orders", 0, i.toLong())
                }
            task.put(inserts)
            forceFlush(task)

            val mixed =
                listOf(
                    debeziumRecord(
                        1,
                        "updated_1",
                        999,
                        "u",
                        before = dataStruct(1, "user_1", 100),
                        topic = "orders",
                        partition = 0,
                        offset = 10,
                    ),
                    debeziumDelete(2, "user_2", 200, "orders", 0, 11),
                    debeziumRecord(4, "user_4", 400, "c", "orders", 0, 12),
                )
            task.put(mixed)
            forceFlush(task)
            task.stop()

            val rows = readAllRows("orders")
            rows shouldHaveSize 3
            val rowMap = rows.associate { (it.get("id") as Int) to it.get("name").toString() }
            rowMap[1] shouldBe "updated_1" // updated
            rowMap.containsKey(2) shouldBe false // deleted
            rowMap[3] shouldBe "user_3" // untouched
            rowMap[4] shouldBe "user_4" // inserted
        }
    }

    @Nested
    inner class AppendOnly {
        @Test
        fun `append mode creates separate files per flush without merge`() {
            val task = startTask(appendProps())
            task.open(listOf(tp("events", 0)))

            val batch1 =
                (1..3).map { i ->
                    plainRecord(i, "event_$i", i * 10, "events", 0, i.toLong())
                }
            task.put(batch1)
            forceFlush(task)

            val batch2 =
                (1..3).map { i ->
                    plainRecord(i, "event_${i}_v2", i * 20, "events", 0, 10L + i)
                }
            task.put(batch2)
            forceFlush(task)
            task.stop()

            val snapshot = DeltaTable.forPath(logStore, tablePath("events")).snapshot()
            snapshot.version shouldBe 2L
            snapshot.activeFiles shouldHaveSize 2

            val rows = readAllRows("events")
            rows shouldHaveSize 6
        }
    }

    @Nested
    inner class MultiTopic {
        @Test
        fun `routes records to separate Delta tables per topic`() {
            val task = startTask(cdcProps(batchSize = 100))
            task.open(listOf(tp("orders", 0), tp("customers", 0)))

            val orderRecords =
                (1..3).map { i ->
                    debeziumRecord(i, "order_$i", i * 100, "c", "orders", 0, i.toLong())
                }
            val customerRecords =
                (1..2).map { i ->
                    debeziumRecord(i, "customer_$i", i * 10, "c", "customers", 0, i.toLong())
                }
            task.put(orderRecords + customerRecords)
            forceFlush(task)
            task.stop()

            val orderRows = readAllRows("orders")
            orderRows shouldHaveSize 3
            orderRows
                .map { it.get("name").toString() }
                .shouldContainExactlyInAnyOrder("order_1", "order_2", "order_3")

            val customerRows = readAllRows("customers")
            customerRows shouldHaveSize 2
            customerRows
                .map { it.get("name").toString() }
                .shouldContainExactlyInAnyOrder("customer_1", "customer_2")
        }
    }

    @Nested
    inner class OffsetTracking {
        @Test
        fun `tracks offsets in Delta transaction log`() {
            val task = startTask(cdcProps(batchSize = 100))
            task.open(listOf(tp("orders", 0)))

            val records =
                (1..5).map { i ->
                    debeziumRecord(i, "user_$i", i * 100, "c", "orders", 0, i.toLong())
                }
            task.put(records)
            forceFlush(task)
            task.stop()

            val snapshot = DeltaTable.forPath(logStore, tablePath("orders")).snapshot()
            val appId = TableWriter.makeAppId(TopicPartition("orders", 0))
            snapshot.transactions shouldContainKey appId
            snapshot.transactions[appId]!!.version shouldBe 5L
        }

        @Test
        fun `recovers offset on restart and seeks past committed`() {
            val task1 = startTask(cdcProps(batchSize = 100))
            task1.open(listOf(tp("orders", 0)))

            val records =
                (1..5).map { i ->
                    debeziumRecord(i, "user_$i", i * 100, "c", "orders", 0, i.toLong())
                }
            task1.put(records)
            forceFlush(task1)
            task1.stop()

            val seekSlot = slot<Map<TopicPartition, Long>>()
            val mockContext2 = mockk<SinkTaskContext>(relaxed = true)
            every { mockContext2.offset(capture(seekSlot)) } returns Unit

            val task2 = DeltaSinkTask()
            task2.initialize(mockContext2)
            task2.logStoreFactory = { logStore }
            task2.clock = { currentTime }
            task2.start(cdcProps(batchSize = 100))
            task2.open(listOf(tp("orders", 0)))

            // Verify context.offset was called with seek position = committed + 1
            seekSlot.isCaptured shouldBe true
            seekSlot.captured[TopicPartition("orders", 0)] shouldBe 6L
            task2.stop()
        }
    }

    @Nested
    inner class TimeBasedFlush {
        @Test
        fun `flushes on time interval via preCommit`() {
            val task = startTask(cdcProps(batchSize = 1000, intervalMs = 5000))
            task.open(listOf(tp("orders", 0)))

            // Put records (won't trigger batch flush, batchSize=1000)
            val records =
                (1..3).map { i ->
                    debeziumRecord(i, "user_$i", i * 100, "c", "orders", 0, i.toLong())
                }
            task.put(records)

            // Table not yet created (no flush happened)
            val snapshotBefore = DeltaTable.forPath(logStore, tablePath("orders")).snapshot()
            snapshotBefore.version shouldBe -1L

            // Advance clock past interval
            currentTime += 6000L
            task.preCommit(emptyMap())

            // Now data should be flushed
            val snapshotAfter = DeltaTable.forPath(logStore, tablePath("orders")).snapshot()
            snapshotAfter.version shouldBe 1L

            val rows = readAllRows("orders")
            rows shouldHaveSize 3
            task.stop()
        }
    }

    @Nested
    inner class BatchDedup {
        @Test
        fun `last-writer-wins for same key in one batch`() {
            val task = startTask(cdcProps(batchSize = 100))
            task.open(listOf(tp("orders", 0)))

            val records =
                listOf(
                    debeziumRecord(1, "v1", 100, "c", "orders", 0, 1),
                    debeziumRecord(
                        1,
                        "v2",
                        200,
                        "u",
                        before = dataStruct(1, "v1", 100),
                        topic = "orders",
                        partition = 0,
                        offset = 2,
                    ),
                    debeziumRecord(
                        1,
                        "v3",
                        300,
                        "u",
                        before = dataStruct(1, "v2", 200),
                        topic = "orders",
                        partition = 0,
                        offset = 3,
                    ),
                )
            task.put(records)
            forceFlush(task)
            task.stop()

            val rows = readAllRows("orders")
            rows shouldHaveSize 1
            rows[0].get("name").toString() shouldBe "v3"
            rows[0].get("value") shouldBe 300
        }
    }

    @Nested
    inner class DeleteDisabled {
        @Test
        fun `ignores deletes when delete enabled is false`() {
            val props = cdcProps(batchSize = 100).toMutableMap()
            props[DeltaSinkConfig.DELTA_MERGE_DELETE_ENABLED] = "false"

            val task = startTask(props)
            task.open(listOf(tp("orders", 0)))

            val inserts =
                (1..3).map { i ->
                    debeziumRecord(i, "user_$i", i * 100, "c", "orders", 0, i.toLong())
                }
            task.put(inserts)
            forceFlush(task)

            val deletes =
                listOf(
                    debeziumDelete(2, "user_2", 200, "orders", 0, 10),
                )
            task.put(deletes)
            forceFlush(task)
            task.stop()

            val rows = readAllRows("orders")
            rows shouldHaveSize 3 // all preserved, delete ignored
            rows.map { it.get("id") as Int }.sorted() shouldBe listOf(1, 2, 3)
        }
    }

    @Nested
    inner class SchemaEvolutionE2E {
        private val dataSchemaV2: Schema =
            SchemaBuilder
                .struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .field("value", Schema.OPTIONAL_INT32_SCHEMA)
                .field("email", Schema.OPTIONAL_STRING_SCHEMA)
                .optional()
                .build()

        private val envelopeSchemaV2: Schema =
            SchemaBuilder
                .struct()
                .field("before", dataSchemaV2)
                .field("after", dataSchemaV2)
                .field("op", Schema.STRING_SCHEMA)
                .field("ts_ms", Schema.INT64_SCHEMA)
                .build()

        @Test
        fun `evolves schema when new column arrives in CDC mode`() {
            val task = startTask(cdcProps(batchSize = 100))
            task.open(listOf(tp("orders", 0)))

            val v1Records =
                (1..3).map { i ->
                    debeziumRecord(i, "user_$i", i * 100, "c", "orders", 0, i.toLong())
                }
            task.put(v1Records)
            forceFlush(task)

            val v2Records =
                (4..5).map { i ->
                    debeziumRecordV2(
                        i,
                        "user_$i",
                        i * 100,
                        "user_$i@test.com",
                        "c",
                        "orders",
                        0,
                        10L + i,
                    )
                }
            task.put(v2Records)
            forceFlush(task)
            task.stop()

            val rows = readAllRowsWithTableSchema("orders")
            rows shouldHaveSize 5

            val rowMap = rows.associate { (it.get("id") as Int) to it }
            rowMap[1]!!.get("email") shouldBe null
            rowMap[4]!!.get("email").toString() shouldBe "user_4@test.com"
        }

        @Suppress("LongParameterList")
        private fun debeziumRecordV2(
            id: Int,
            name: String?,
            value: Int?,
            email: String?,
            op: String,
            topic: String,
            partition: Int,
            offset: Long,
        ): SinkRecord {
            val after =
                Struct(dataSchemaV2)
                    .put("id", id)
                    .put("name", name)
                    .put("value", value)
                    .put("email", email)
            val envelope =
                Struct(envelopeSchemaV2)
                    .put("before", null as Struct?)
                    .put("after", after)
                    .put("op", op)
                    .put("ts_ms", System.currentTimeMillis())
            return SinkRecord(topic, partition, null, null, envelopeSchemaV2, envelope, offset)
        }
    }

    private fun startTask(props: Map<String, String>): DeltaSinkTask {
        task = DeltaSinkTask()
        task.initialize(mockContext)
        task.logStoreFactory = { logStore }
        task.clock = { currentTime }
        task.start(props)
        return task
    }

    private fun forceFlush(task: DeltaSinkTask) {
        currentTime += 120_000L
        task.preCommit(emptyMap())
    }

    private fun tablePath(topic: String): String = "$tempDir/$topic"

    private fun readAllRows(topic: String): List<GenericRecord> {
        val table = DeltaTable.forPath(logStore, tablePath(topic))
        val snapshot = table.snapshot()
        if (snapshot.version < 0) return emptyList()
        val reader = ParquetFileReader(logStore)
        return snapshot.activeFiles.flatMap { reader.read(it.path) }
    }

    private fun readAllRowsWithTableSchema(topic: String): List<GenericRecord> {
        val table = DeltaTable.forPath(logStore, tablePath(topic))
        val snapshot = table.snapshot()
        if (snapshot.version < 0) return emptyList()
        val reader = ParquetFileReader(logStore)
        val deltaSchema = DeltaSchema.fromJson(snapshot.metaData!!.schemaString)
        return snapshot.activeFiles.flatMap { reader.read(it.path, deltaSchema) }
    }

    private fun tp(
        topic: String,
        partition: Int,
    ): TopicPartition = TopicPartition(topic, partition)

    private fun cdcProps(
        batchSize: Int = 10,
        intervalMs: Long = 60_000,
    ): Map<String, String> =
        mapOf(
            DeltaSinkConfig.DELTA_STORAGE_PATH to tempDir.toString(),
            DeltaSinkConfig.DELTA_MERGE_KEYS to "id",
            DeltaSinkConfig.DELTA_WRITE_MODE to "cdc",
            DeltaSinkConfig.DELTA_MERGE_BATCH_SIZE to batchSize.toString(),
            DeltaSinkConfig.DELTA_MERGE_INTERVAL_MS to intervalMs.toString(),
        )

    private fun appendProps(): Map<String, String> =
        mapOf(
            DeltaSinkConfig.DELTA_STORAGE_PATH to tempDir.toString(),
            DeltaSinkConfig.DELTA_MERGE_KEYS to "",
            DeltaSinkConfig.DELTA_WRITE_MODE to "append",
            DeltaSinkConfig.DELTA_MERGE_BATCH_SIZE to "100",
            DeltaSinkConfig.DELTA_MERGE_INTERVAL_MS to "60000",
        )

    private fun dataStruct(
        id: Int,
        name: String?,
        value: Int?,
    ): Struct =
        Struct(dataSchema)
            .put("id", id)
            .put("name", name)
            .put("value", value)

    /**
     * Build a Debezium full-envelope SinkRecord for insert/update/read ops.
     */
    private fun debeziumRecord(
        id: Int,
        name: String?,
        value: Int?,
        op: String,
        topic: String,
        partition: Int,
        offset: Long,
        before: Struct? = null,
    ): SinkRecord {
        val after = dataStruct(id, name, value)
        val envelope =
            Struct(envelopeSchema)
                .put("before", before)
                .put("after", after)
                .put("op", op)
                .put("ts_ms", System.currentTimeMillis())
        return SinkRecord(topic, partition, null, null, envelopeSchema, envelope, offset)
    }

    /**
     * Build a Debezium full-envelope SinkRecord for delete ops.
     */
    private fun debeziumDelete(
        id: Int,
        name: String?,
        value: Int?,
        topic: String,
        partition: Int,
        offset: Long,
    ): SinkRecord {
        val before = dataStruct(id, name, value)
        val envelope =
            Struct(envelopeSchema)
                .put("before", before)
                .put("after", null as Struct?)
                .put("op", "d")
                .put("ts_ms", System.currentTimeMillis())
        return SinkRecord(topic, partition, null, null, envelopeSchema, envelope, offset)
    }

    /**
     * Build a plain SinkRecord for append mode.
     */
    private fun plainRecord(
        id: Int,
        name: String?,
        value: Int?,
        topic: String,
        partition: Int,
        offset: Long,
    ): SinkRecord {
        val struct =
            Struct(plainSchema)
                .put("id", id)
                .put("name", name)
                .put("value", value)
        return SinkRecord(topic, partition, null, null, plainSchema, struct, offset)
    }
}
