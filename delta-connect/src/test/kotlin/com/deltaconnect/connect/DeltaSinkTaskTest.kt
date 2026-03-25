package com.deltaconnect.connect

import com.deltaconnect.connect.storage.LocalStorageProvider
import com.deltaconnect.connect.storage.StorageProviderRegistry
import com.deltaconnect.protocol.DeltaTable
import com.deltaconnect.protocol.merge.MergeOperation
import com.deltaconnect.protocol.merge.SourceRecord
import com.deltaconnect.protocol.storage.LocalFileSystemLogStore
import io.kotest.matchers.longs.shouldBeGreaterThan
import io.kotest.matchers.maps.shouldBeEmpty
import io.kotest.matchers.maps.shouldContainKey
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTaskContext
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path

class DeltaSinkTaskTest {
    @TempDir
    lateinit var tempDir: Path

    private lateinit var logStore: LocalFileSystemLogStore
    private lateinit var task: DeltaSinkTask
    private lateinit var mockContext: SinkTaskContext
    private var currentTime: Long = 1_000_000L

    private val avroSchema =
        SchemaBuilder
            .record("record")
            .namespace("com.deltaconnect")
            .fields()
            .requiredInt("id")
            .optionalString("name")
            .name("value")
            .type()
            .nullable()
            .intType()
            .noDefault()
            .endRecord()

    @BeforeEach
    fun setUp() {
        StorageProviderRegistry.clear()
        StorageProviderRegistry.register(LocalStorageProvider(tempDir))

        logStore = LocalFileSystemLogStore(tempDir)
        mockContext = mockk(relaxed = true)

        task = DeltaSinkTask()
        task.initialize(mockContext)
        task.logStoreFactory = { logStore }
        task.converterFactory = { _ -> stubConverter() }
        task.clock = { currentTime }
    }

    @AfterEach
    fun tearDown() {
        StorageProviderRegistry.clear()
    }

    private fun mergeProps(): Map<String, String> =
        mapOf(
            DeltaSinkConfig.DELTA_STORAGE_PATH to tempDir.toString(),
            DeltaSinkConfig.DELTA_MERGE_KEYS to "id",
            DeltaSinkConfig.DELTA_WRITE_MODE to "cdc",
            DeltaSinkConfig.DELTA_MERGE_BATCH_SIZE to "100",
            DeltaSinkConfig.DELTA_MERGE_INTERVAL_MS to "60000",
        )

    private fun appendProps(): Map<String, String> =
        mapOf(
            DeltaSinkConfig.DELTA_STORAGE_PATH to tempDir.toString(),
            DeltaSinkConfig.DELTA_MERGE_KEYS to "",
            DeltaSinkConfig.DELTA_WRITE_MODE to "append",
            DeltaSinkConfig.DELTA_MERGE_BATCH_SIZE to "100",
            DeltaSinkConfig.DELTA_MERGE_INTERVAL_MS to "60000",
        )

    private fun upsertProps(): Map<String, String> =
        mapOf(
            DeltaSinkConfig.DELTA_STORAGE_PATH to tempDir.toString(),
            DeltaSinkConfig.DELTA_MERGE_KEYS to "id",
            DeltaSinkConfig.DELTA_WRITE_MODE to "upsert",
            DeltaSinkConfig.DELTA_MERGE_BATCH_SIZE to "100",
            DeltaSinkConfig.DELTA_MERGE_INTERVAL_MS to "60000",
        )

    @Test
    fun `version returns connector version`() {
        task.version() shouldBe DeltaSinkConnector.VERSION
    }

    @Nested
    inner class Lifecycle {
        @Test
        fun `start and stop complete without error`() {
            task.start(mergeProps())
            task.stop()
        }

        @Test
        fun `put with empty records does nothing`() {
            task.start(mergeProps())
            task.put(emptyList())
            task.stop()
        }
    }

    @Nested
    inner class BasicPutAndFlush {
        @Test
        fun `put buffers records and batch flush writes to delta`() {
            val props =
                mergeProps().toMutableMap().apply {
                    put(DeltaSinkConfig.DELTA_MERGE_BATCH_SIZE, "2")
                }
            task.start(props)
            task.open(listOf(tp(0)))

            task.put(listOf(sinkRecord("orders", 0, 0L), sinkRecord("orders", 0, 1L)))

            val snapshot = DeltaTable.forPath(logStore, "orders").snapshot()
            snapshot.version shouldBeGreaterThan -1
            task.stop()
        }

        @Test
        fun `time-based flush triggers on put`() {
            task.start(mergeProps())
            task.open(listOf(tp(0)))

            task.put(listOf(sinkRecord("orders", 0, 0L)))

            currentTime += 60_001L
            task.put(listOf(sinkRecord("orders", 0, 1L)))

            val snapshot = DeltaTable.forPath(logStore, "orders").snapshot()
            snapshot.version shouldBeGreaterThan -1
            task.stop()
        }

        @Test
        fun `time-based flush triggers on preCommit`() {
            task.start(mergeProps())
            task.open(listOf(tp(0)))

            task.put(listOf(sinkRecord("orders", 0, 0L)))

            currentTime += 60_001L
            task.preCommit(emptyMap())

            val snapshot = DeltaTable.forPath(logStore, "orders").snapshot()
            snapshot.version shouldBeGreaterThan -1
            task.stop()
        }
    }

    @Nested
    inner class PreCommitOffsets {
        @Test
        fun `preCommit returns empty when no records flushed`() {
            task.start(mergeProps())
            val offsets = task.preCommit(emptyMap())
            offsets.shouldBeEmpty()
            task.stop()
        }

        @Test
        fun `preCommit returns committed offsets plus one`() {
            val props =
                mergeProps().toMutableMap().apply {
                    put(DeltaSinkConfig.DELTA_MERGE_BATCH_SIZE, "2")
                }
            task.start(props)
            task.open(listOf(tp(0)))

            task.put(listOf(sinkRecord("orders", 0, 5L), sinkRecord("orders", 0, 6L)))

            val offsets = task.preCommit(emptyMap())
            offsets shouldContainKey tp(0)
            offsets[tp(0)]!!.offset() shouldBe 7L // 6 + 1
            task.stop()
        }
    }

    @Nested
    inner class OffsetRecovery {
        @Test
        fun `open recovers offsets from delta transaction log`() {
            task.start(mergeProps())
            task.open(listOf(tp(0)))
            val props2 =
                mergeProps().toMutableMap().apply {
                    put(DeltaSinkConfig.DELTA_MERGE_BATCH_SIZE, "1")
                }
            task.stop()

            val task2 = DeltaSinkTask()
            task2.initialize(mockContext)
            task2.logStoreFactory = { logStore }
            task2.converterFactory = { _ -> stubConverter() }
            task2.clock = { currentTime }
            task2.start(props2)

            task2.open(listOf(tp(0)))
            task2.put(listOf(sinkRecord("orders", 0, 42L)))

            task2.stop()

            val task3 = DeltaSinkTask()
            task3.initialize(mockContext)
            task3.logStoreFactory = { logStore }
            task3.converterFactory = { _ -> stubConverter() }
            task3.clock = { currentTime }
            task3.start(props2)

            val seekSlot = slot<Map<TopicPartition, Long>>()
            every { mockContext.offset(capture(seekSlot)) } answers {}

            task3.open(listOf(tp(0)))

            seekSlot.isCaptured shouldBe true
            seekSlot.captured shouldContainKey tp(0)
            seekSlot.captured[tp(0)] shouldBe 43L // 42 + 1
            task3.stop()
        }

        @Test
        fun `open does not seek for new partitions`() {
            task.start(mergeProps())

            val seekSlot = slot<Map<TopicPartition, Long>>()
            every { mockContext.offset(capture(seekSlot)) } answers {}

            task.open(listOf(tp(0)))

            seekSlot.isCaptured shouldBe false
            task.stop()
        }
    }

    @Nested
    inner class Rebalance {
        @Test
        fun `close flushes remaining records for revoked partitions`() {
            val props =
                mergeProps().toMutableMap().apply {
                    put(DeltaSinkConfig.DELTA_MERGE_BATCH_SIZE, "100") // won't auto-flush
                }
            task.start(props)
            task.open(listOf(tp(0)))

            task.put(listOf(sinkRecord("orders", 0, 0L)))

            task.close(listOf(tp(0)))

            val snapshot = DeltaTable.forPath(logStore, "orders").snapshot()
            snapshot.version shouldBeGreaterThan -1
            task.stop()
        }

        @Test
        fun `close removes committed offsets for revoked partitions`() {
            val props =
                mergeProps().toMutableMap().apply {
                    put(DeltaSinkConfig.DELTA_MERGE_BATCH_SIZE, "1")
                }
            task.start(props)
            task.open(listOf(tp(0), tp(1)))

            task.put(listOf(sinkRecord("orders", 0, 0L), sinkRecord("orders", 1, 0L)))

            task.close(listOf(tp(0)))

            val offsets = task.preCommit(emptyMap())
            offsets shouldContainKey tp(1)
            offsets.containsKey(tp(0)) shouldBe false
            task.stop()
        }
    }

    @Nested
    inner class MultiTopic {
        @Test
        fun `put routes records to correct topic writer`() {
            val props =
                mergeProps().toMutableMap().apply {
                    put(DeltaSinkConfig.DELTA_MERGE_BATCH_SIZE, "1")
                }
            task.start(props)
            task.open(
                listOf(
                    TopicPartition("orders", 0),
                    TopicPartition("customers", 0),
                ),
            )

            task.put(
                listOf(
                    sinkRecord("orders", 0, 0L),
                    sinkRecord("customers", 0, 0L),
                ),
            )

            DeltaTable.forPath(logStore, "orders").snapshot().version shouldBeGreaterThan -1
            DeltaTable.forPath(logStore, "customers").snapshot().version shouldBeGreaterThan -1
            task.stop()
        }
    }

    @Nested
    inner class WriteModes {
        @Test
        fun `append mode writes records`() {
            val props =
                appendProps().toMutableMap().apply {
                    put(DeltaSinkConfig.DELTA_MERGE_BATCH_SIZE, "1")
                }
            task.start(props)
            task.open(listOf(tp(0)))

            task.put(listOf(sinkRecord("orders", 0, 0L)))

            val snapshot = DeltaTable.forPath(logStore, "orders").snapshot()
            snapshot.version shouldBeGreaterThan -1
            task.stop()
        }

        @Test
        fun `upsert mode writes records`() {
            val props =
                upsertProps().toMutableMap().apply {
                    put(DeltaSinkConfig.DELTA_MERGE_BATCH_SIZE, "1")
                }
            task.start(props)
            task.open(listOf(tp(0)))

            task.put(listOf(sinkRecord("orders", 0, 0L)))

            val snapshot = DeltaTable.forPath(logStore, "orders").snapshot()
            snapshot.version shouldBeGreaterThan -1
            task.stop()
        }

        @Test
        fun `upsert mode disables delete processing`() {
            var capturedDeleteEnabled: Boolean? = null
            val converter =
                object : RecordConverter {
                    override fun convert(
                        record: SinkRecord,
                        deleteEnabled: Boolean,
                    ): SourceRecord? {
                        capturedDeleteEnabled = deleteEnabled
                        val rec = GenericData.Record(avroSchema)
                        rec.put("id", 1)
                        rec.put("name", "test")
                        rec.put("value", 100)
                        return SourceRecord(rec, MergeOperation.INSERT)
                    }

                    override fun extractSchema(record: SinkRecord) = throw UnsupportedOperationException()
                }

            val props =
                upsertProps().toMutableMap().apply {
                    put(DeltaSinkConfig.DELTA_MERGE_BATCH_SIZE, "1")
                }
            task.converterFactory = { _ -> converter }
            task.start(props)
            task.open(listOf(tp(0)))

            task.put(listOf(sinkRecord("orders", 0, 0L)))

            capturedDeleteEnabled shouldBe false
            task.stop()
        }
    }

    @Nested
    inner class TableNameTemplate {
        @Test
        fun `table name template with prefix`() {
            val props =
                mergeProps().toMutableMap().apply {
                    put(DeltaSinkConfig.DELTA_TABLE_NAME, "cdc_\${topic}")
                    put(DeltaSinkConfig.DELTA_MERGE_BATCH_SIZE, "1")
                }
            task.start(props)
            task.open(listOf(tp(0)))

            task.put(listOf(sinkRecord("orders", 0, 0L)))

            val snapshot = DeltaTable.forPath(logStore, "cdc_orders").snapshot()
            snapshot.version shouldBeGreaterThan -1
            task.stop()
        }

        @Test
        fun `static table name`() {
            val props =
                mergeProps().toMutableMap().apply {
                    put(DeltaSinkConfig.DELTA_TABLE_NAME, "my_fixed_table")
                    put(DeltaSinkConfig.DELTA_MERGE_BATCH_SIZE, "1")
                }
            task.start(props)
            task.open(listOf(tp(0)))

            task.put(listOf(sinkRecord("orders", 0, 0L)))

            val snapshot = DeltaTable.forPath(logStore, "my_fixed_table").snapshot()
            snapshot.version shouldBeGreaterThan -1
            task.stop()
        }
    }

    @Nested
    inner class BasePath {
        @Test
        fun `resolveBasePath trims trailing slash for plain path`() {
            val config =
                DeltaSinkConfig(
                    mergeProps().toMutableMap().apply {
                        put(DeltaSinkConfig.DELTA_STORAGE_PATH, "/some/path/")
                    },
                )
            DeltaSinkTask.resolveBasePath(config) shouldBe "/some/path"
        }

        @Test
        fun `resolveBasePath handles path without trailing slash`() {
            val config =
                DeltaSinkConfig(
                    mergeProps().toMutableMap().apply {
                        put(DeltaSinkConfig.DELTA_STORAGE_PATH, "/some/path")
                    },
                )
            DeltaSinkTask.resolveBasePath(config) shouldBe "/some/path"
        }
    }

    @Nested
    inner class ExistingTableReuse {
        @Test
        fun `reuses existing table across task restarts`() {
            val props =
                mergeProps().toMutableMap().apply {
                    put(DeltaSinkConfig.DELTA_MERGE_BATCH_SIZE, "1")
                }

            task.start(props)
            task.open(listOf(tp(0)))
            task.put(listOf(sinkRecord("orders", 0, 0L)))
            task.stop()

            val task2 = DeltaSinkTask()
            task2.initialize(mockContext)
            task2.logStoreFactory = { logStore }
            task2.converterFactory = { _ -> stubConverter() }
            task2.clock = { currentTime }
            task2.start(props)
            task2.open(listOf(tp(0)))
            task2.put(listOf(sinkRecord("orders", 0, 1L)))

            val snapshot = DeltaTable.forPath(logStore, "orders").snapshot()
            snapshot.version shouldBeGreaterThan 1
            task2.stop()
        }
    }

    private fun tp(partition: Int): TopicPartition = TopicPartition("orders", partition)

    private fun sinkRecord(
        topic: String,
        partition: Int,
        offset: Long,
    ): SinkRecord = SinkRecord(topic, partition, null, null, null, null, offset)

    private fun stubConverter(): RecordConverter =
        object : RecordConverter {
            override fun convert(
                record: SinkRecord,
                deleteEnabled: Boolean,
            ): SourceRecord {
                val rec = GenericData.Record(avroSchema)
                rec.put("id", record.kafkaOffset().toInt())
                rec.put("name", "name_${record.kafkaOffset()}")
                rec.put("value", record.kafkaOffset().toInt() * 100)
                return SourceRecord(rec, MergeOperation.INSERT)
            }

            override fun extractSchema(record: SinkRecord) = throw UnsupportedOperationException()
        }
}
