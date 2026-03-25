package com.deltaconnect.connect

import com.deltaconnect.connect.storage.LocalStorageProvider
import com.deltaconnect.connect.storage.StorageProviderRegistry
import com.deltaconnect.protocol.merge.MergeOperation
import com.deltaconnect.protocol.merge.SourceRecord
import com.deltaconnect.protocol.storage.LocalFileSystemLogStore
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.ErrantRecordReporter
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTaskContext
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import java.util.concurrent.Future

class ErrorHandlingTest {
    @TempDir
    lateinit var tempDir: Path

    private lateinit var logStore: LocalFileSystemLogStore
    private lateinit var mockContext: SinkTaskContext
    private var currentTime: Long = 1_000_000L

    @BeforeEach
    fun setUp() {
        StorageProviderRegistry.clear()
        logStore = LocalFileSystemLogStore(tempDir)
    }

    @AfterEach
    fun tearDown() {
        StorageProviderRegistry.clear()
    }

    @Nested
    inner class DlqErrorReporting {
        @Test
        fun `reports bad records to DLQ when reporter is available`() {
            val mockReporter = mockk<ErrantRecordReporter>()
            val mockFuture = mockk<Future<Void>>(relaxed = true)
            every { mockReporter.report(any(), any()) } returns mockFuture

            mockContext = mockk(relaxed = true)
            every { mockContext.errantRecordReporter() } returns mockReporter

            StorageProviderRegistry.register(LocalStorageProvider(tempDir))

            val task = DeltaSinkTask()
            task.initialize(mockContext)
            task.logStoreFactory = { logStore }
            task.converterFactory = { _ -> FailingConverter() }
            task.clock = { currentTime }
            task.start(cdcProps())
            task.open(listOf(TopicPartition("orders", 0)))

            val record = SinkRecord("orders", 0, null, null, null, "bad_data", 0)
            task.put(listOf(record))

            // Record should be reported to DLQ, not thrown
            verify { mockReporter.report(record, any()) }
            task.stop()
        }

        @Test
        fun `throws ConnectException when no DLQ and conversion fails`() {
            mockContext = mockk(relaxed = true)
            every { mockContext.errantRecordReporter() } returns null

            StorageProviderRegistry.register(LocalStorageProvider(tempDir))

            val task = DeltaSinkTask()
            task.initialize(mockContext)
            task.logStoreFactory = { logStore }
            task.converterFactory = { _ -> FailingConverter() }
            task.clock = { currentTime }
            task.start(cdcProps())
            task.open(listOf(TopicPartition("orders", 0)))

            val record = SinkRecord("orders", 0, null, null, null, "bad_data", 0)

            assertThrows<ConnectException> {
                task.put(listOf(record))
            }
            task.stop()
        }

        @Test
        fun `good records still processed when bad record sent to DLQ`() {
            val mockReporter = mockk<ErrantRecordReporter>()
            val mockFuture = mockk<Future<Void>>(relaxed = true)
            every { mockReporter.report(any(), any()) } returns mockFuture

            mockContext = mockk(relaxed = true)
            every { mockContext.errantRecordReporter() } returns mockReporter

            StorageProviderRegistry.register(LocalStorageProvider(tempDir))

            val avroSchema =
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

            val selectiveConverter =
                object : RecordConverter {
                    override fun convert(
                        record: SinkRecord,
                        deleteEnabled: Boolean,
                    ): SourceRecord? {
                        if (record.kafkaOffset() % 2 != 0L) {
                            throw RuntimeException("Bad record at offset ${record.kafkaOffset()}")
                        }
                        val rec = GenericData.Record(avroSchema)
                        rec.put("id", record.kafkaOffset().toInt())
                        rec.put("name", "name_${record.kafkaOffset()}")
                        rec.put("value", record.kafkaOffset().toInt() * 100)
                        return SourceRecord(rec, MergeOperation.INSERT)
                    }

                    override fun extractSchema(record: SinkRecord) = throw UnsupportedOperationException()
                }

            val task = DeltaSinkTask()
            task.initialize(mockContext)
            task.logStoreFactory = { logStore }
            task.converterFactory = { _ -> selectiveConverter }
            task.clock = { currentTime }
            task.start(appendProps())
            task.open(listOf(TopicPartition("orders", 0)))

            val records =
                (0L..4L).map { offset ->
                    SinkRecord("orders", 0, null, null, null, "data", offset)
                }
            task.put(records)

            currentTime += 120_000L
            task.preCommit(emptyMap())

            // 3 good records (offsets 0,2,4) should be flushed
            // 2 bad records (offsets 1,3) should be reported to DLQ
            verify(exactly = 2) { mockReporter.report(any(), any()) }
            task.stop()
        }
    }

    @Nested
    inner class ErrorClassification {
        @Test
        fun `handles NoSuchMethodError for older Connect runtimes`() {
            mockContext = mockk(relaxed = true)
            every { mockContext.errantRecordReporter() } throws NoSuchMethodError()

            StorageProviderRegistry.register(LocalStorageProvider(tempDir))

            val avroSchema =
                SchemaBuilder
                    .record("record")
                    .namespace("com.deltaconnect")
                    .fields()
                    .requiredInt("id")
                    .optionalString("name")
                    .endRecord()

            val task = DeltaSinkTask()
            task.initialize(mockContext)
            task.logStoreFactory = { logStore }
            task.converterFactory = { _ ->
                object : RecordConverter {
                    override fun convert(
                        record: SinkRecord,
                        deleteEnabled: Boolean,
                    ): SourceRecord {
                        val rec = GenericData.Record(avroSchema)
                        rec.put("id", 1)
                        rec.put("name", "test")
                        return SourceRecord(rec, MergeOperation.INSERT)
                    }

                    override fun extractSchema(record: SinkRecord) = throw UnsupportedOperationException()
                }
            }
            task.clock = { currentTime }

            task.start(appendProps())
            task.open(listOf(TopicPartition("orders", 0)))
            task.put(listOf(SinkRecord("orders", 0, null, null, null, "data", 0)))
            task.stop()
        }
    }

    private fun cdcProps(): Map<String, String> =
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

    /**
     * Converter that always throws - simulates malformed records.
     */
    private class FailingConverter : RecordConverter {
        override fun convert(
            record: SinkRecord,
            deleteEnabled: Boolean,
        ): SourceRecord? = throw RuntimeException("Simulated conversion failure")

        override fun extractSchema(record: SinkRecord) = throw UnsupportedOperationException()
    }
}
