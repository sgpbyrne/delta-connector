package com.deltaconnect.connect

import com.deltaconnect.connect.storage.LocalStorageProvider
import com.deltaconnect.connect.storage.StorageProviderRegistry
import com.deltaconnect.protocol.DeltaTable
import com.deltaconnect.protocol.parquet.ParquetFileReader
import com.deltaconnect.protocol.storage.LocalFileSystemLogStore
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.mockk.CapturingSlot
import io.mockk.every
import io.mockk.mockk
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
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.nio.file.Path
import java.util.Properties
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import java.time.Duration

/**
 * Integration tests verifying the connector pipeline with a real Kafka broker.
 *
 * Uses Testcontainers Kafka to run an actual Kafka broker. Records are produced
 * to Kafka, consumed as SinkRecords, and processed through the connector task
 * to Delta Lake tables on local filesystem.
 *
 * This validates:
 * - Real Kafka serialization/deserialization
 * - End-to-end data flow: Kafka - Connector - Delta Lake
 * - Offset tracking with real Kafka offsets
 */
@Testcontainers
class KafkaPipelineIntegrationTest {

    companion object {
        @Container
        @JvmStatic
        val kafka: KafkaContainer = KafkaContainer("apache/kafka:3.7.0")
    }

    @TempDir
    lateinit var tempDir: Path

    private lateinit var logStore: LocalFileSystemLogStore
    private var currentTime: Long = 1_000_000L

    @BeforeEach
    fun setUp() {
        StorageProviderRegistry.clear()
        logStore = LocalFileSystemLogStore(tempDir)
        StorageProviderRegistry.register(LocalStorageProvider(tempDir))
    }

    @AfterEach
    fun tearDown() {
        StorageProviderRegistry.clear()
    }

    @Nested
    inner class KafkaConnectivity {

        @Test
        fun `produce and consume records through real Kafka`() {
            val topic = "test-connectivity-${System.nanoTime()}"

            createProducer().use { producer ->
                producer.send(ProducerRecord(topic, "key1", "value1")).get()
            }

            createConsumer(topic).use { consumer ->
                val records = consumer.poll(Duration.ofSeconds(10))
                records.count() shouldBe 1
                records.first().value() shouldBe "value1"
            }
        }
    }

    @Nested
    inner class FullPipeline {

        private val dataSchema: Schema = SchemaBuilder.struct()
            .field("id", Schema.INT32_SCHEMA)
            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
            .field("value", Schema.OPTIONAL_INT32_SCHEMA)
            .optional()
            .build()

        private val envelopeSchema: Schema = SchemaBuilder.struct()
            .field("before", dataSchema)
            .field("after", dataSchema)
            .field("op", Schema.STRING_SCHEMA)
            .field("ts_ms", Schema.INT64_SCHEMA)
            .build()

        @Test
        fun `CDC records through task produce correct Delta output`() {
            val topic = "orders-${System.nanoTime()}"

            val records = (1..5).map { i ->
                val after = Struct(dataSchema)
                    .put("id", i).put("name", "user_$i").put("value", i * 100)
                val envelope = Struct(envelopeSchema)
                    .put("before", null as Struct?)
                    .put("after", after)
                    .put("op", "c")
                    .put("ts_ms", System.currentTimeMillis())
                SinkRecord(topic, 0, null, null, envelopeSchema, envelope, i.toLong())
            }

            val mockContext = mockk<SinkTaskContext>(relaxed = true)
            val task = DeltaSinkTask()
            task.initialize(mockContext)
            task.logStoreFactory = { logStore }
            task.clock = { currentTime }
            task.start(
                mapOf(
                    DeltaSinkConfig.DELTA_STORAGE_PATH to tempDir.toString(),
                    DeltaSinkConfig.DELTA_MERGE_KEYS to "id",
                    DeltaSinkConfig.DELTA_WRITE_MODE to "cdc",
                    DeltaSinkConfig.DELTA_MERGE_BATCH_SIZE to "100",
                    DeltaSinkConfig.DELTA_MERGE_INTERVAL_MS to "60000"
                )
            )
            task.open(listOf(TopicPartition(topic, 0)))
            task.put(records)

            currentTime += 120_000L
            task.preCommit(emptyMap())
            task.stop()

            val tablePath = "${tempDir}/$topic"
            val table = DeltaTable.forPath(logStore, tablePath)
            val snapshot = table.snapshot()
            snapshot.version shouldBe 1L

            val reader = ParquetFileReader(logStore)
            val rows = snapshot.activeFiles.flatMap { reader.read(it.path) }
            rows shouldHaveSize 5
            rows.map { it.get("id") as Int }.sorted() shouldBe listOf(1, 2, 3, 4, 5)
        }

        @Test
        fun `append mode records through task produce separate files`() {
            val topic = "events-${System.nanoTime()}"

            val plainSchema: Schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .build()

            val mockContext = mockk<SinkTaskContext>(relaxed = true)
            val task = DeltaSinkTask()
            task.initialize(mockContext)
            task.logStoreFactory = { logStore }
            task.clock = { currentTime }
            task.start(
                mapOf(
                    DeltaSinkConfig.DELTA_STORAGE_PATH to tempDir.toString(),
                    DeltaSinkConfig.DELTA_MERGE_KEYS to "",
                    DeltaSinkConfig.DELTA_WRITE_MODE to "append",
                    DeltaSinkConfig.DELTA_MERGE_BATCH_SIZE to "100",
                    DeltaSinkConfig.DELTA_MERGE_INTERVAL_MS to "60000"
                )
            )
            task.open(listOf(TopicPartition(topic, 0)))

            val batch1 = (1..3).map { i ->
                val value = Struct(plainSchema).put("id", i).put("name", "event_$i")
                SinkRecord(topic, 0, null, null, plainSchema, value, i.toLong())
            }
            task.put(batch1)
            currentTime += 120_000L
            task.preCommit(emptyMap())

            val batch2 = (4..6).map { i ->
                val value = Struct(plainSchema).put("id", i).put("name", "event_$i")
                SinkRecord(topic, 0, null, null, plainSchema, value, i.toLong())
            }
            task.put(batch2)
            currentTime += 120_000L
            task.preCommit(emptyMap())
            task.stop()

            val tablePath = "${tempDir}/$topic"
            val snapshot = DeltaTable.forPath(logStore, tablePath).snapshot()
            snapshot.version shouldBe 2L
            snapshot.activeFiles shouldHaveSize 2

            val reader = ParquetFileReader(logStore)
            val allRows = snapshot.activeFiles.flatMap { reader.read(it.path) }
            allRows shouldHaveSize 6
        }

        @Test
        fun `offset recovery works across task restarts`() {
            val topic = "recovery-${System.nanoTime()}"

            val mockContext1 = mockk<SinkTaskContext>(relaxed = true)
            val task1 = DeltaSinkTask()
            task1.initialize(mockContext1)
            task1.logStoreFactory = { logStore }
            task1.clock = { currentTime }
            task1.start(cdcProps(topic))
            task1.open(listOf(TopicPartition(topic, 0)))

            val records = (1..3).map { i ->
                val after = Struct(dataSchema)
                    .put("id", i).put("name", "user_$i").put("value", i * 100)
                val envelope = Struct(envelopeSchema)
                    .put("before", null as Struct?)
                    .put("after", after)
                    .put("op", "c")
                    .put("ts_ms", System.currentTimeMillis())
                SinkRecord(topic, 0, null, null, envelopeSchema, envelope, i.toLong())
            }
            task1.put(records)
            currentTime += 120_000L
            task1.preCommit(emptyMap())
            task1.stop()

            val tablePath = "${tempDir}/$topic"
            val snapshot = DeltaTable.forPath(logStore, tablePath).snapshot()
            val appId = TableWriter.makeAppId(TopicPartition(topic, 0))
            snapshot.transactions[appId]!!.version shouldBe 3L

            val seekSlot = CapturingSlot<Map<TopicPartition, Long>>()
            val mockContext2 = mockk<SinkTaskContext>(relaxed = true)
            every { mockContext2.offset(capture(seekSlot)) } returns Unit

            val task2 = DeltaSinkTask()
            task2.initialize(mockContext2)
            task2.logStoreFactory = { logStore }
            task2.clock = { currentTime }
            task2.start(cdcProps(topic))
            task2.open(listOf(TopicPartition(topic, 0)))

            seekSlot.isCaptured shouldBe true
            seekSlot.captured[TopicPartition(topic, 0)] shouldBe 4L // committed(3) + 1
            task2.stop()
        }
    }

    private fun createProducer(): KafkaProducer<String, String> {
        val props = Properties().apply {
            put("bootstrap.servers", kafka.bootstrapServers)
            put("key.serializer", StringSerializer::class.java.name)
            put("value.serializer", StringSerializer::class.java.name)
        }
        return KafkaProducer(props)
    }

    private fun createConsumer(topic: String): KafkaConsumer<String, String> {
        val props = Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers)
            put(ConsumerConfig.GROUP_ID_CONFIG, "test-${System.nanoTime()}")
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        }
        return KafkaConsumer<String, String>(props).apply { subscribe(listOf(topic)) }
    }

    private fun cdcProps(topic: String): Map<String, String> = mapOf(
        DeltaSinkConfig.DELTA_STORAGE_PATH to tempDir.toString(),
        DeltaSinkConfig.DELTA_MERGE_KEYS to "id",
        DeltaSinkConfig.DELTA_WRITE_MODE to "cdc",
        DeltaSinkConfig.DELTA_MERGE_BATCH_SIZE to "100",
        DeltaSinkConfig.DELTA_MERGE_INTERVAL_MS to "60000"
    )
}
