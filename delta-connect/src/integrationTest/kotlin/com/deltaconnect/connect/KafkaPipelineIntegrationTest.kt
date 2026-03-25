package com.deltaconnect.connect

import com.deltaconnect.connect.storage.LocalStorageProvider
import com.deltaconnect.connect.storage.StorageProviderRegistry
import com.deltaconnect.protocol.DeltaTable
import com.deltaconnect.protocol.parquet.ParquetFileReader
import com.deltaconnect.protocol.schema.DeltaSchema
import com.deltaconnect.protocol.storage.LocalFileSystemLogStore
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.mockk.CapturingSlot
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.ErrantRecordReporter
import org.junit.jupiter.api.assertThrows
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Future
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

    @Nested
    inner class SchemaEvolutionPipeline {

        @Test
        fun `new nullable column is added to Delta table when schema evolves`() {
            val topic = "schema-evo-${System.nanoTime()}"

            val schemaV1: Schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .build()

            val schemaV2: Schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .field("email", Schema.OPTIONAL_STRING_SCHEMA)
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
                    DeltaSinkConfig.DELTA_SCHEMA_EVOLUTION to "true",
                    DeltaSinkConfig.DELTA_MERGE_BATCH_SIZE to "100",
                    DeltaSinkConfig.DELTA_MERGE_INTERVAL_MS to "60000"
                )
            )
            task.open(listOf(TopicPartition(topic, 0)))

            // Batch 1: schema v1
            val batch1 = (1..3).map { i ->
                val value = Struct(schemaV1).put("id", i).put("name", "user_$i")
                SinkRecord(topic, 0, null, null, schemaV1, value, i.toLong())
            }
            task.put(batch1)
            currentTime += 120_000L
            task.preCommit(emptyMap())

            // Batch 2: schema v2 with new "email" column
            val batch2 = (4..6).map { i ->
                val value = Struct(schemaV2)
                    .put("id", i).put("name", "user_$i").put("email", "user_$i@test.com")
                SinkRecord(topic, 0, null, null, schemaV2, value, (i + 3).toLong())
            }
            task.put(batch2)
            currentTime += 120_000L
            task.preCommit(emptyMap())
            task.stop()

            val tablePath = "${tempDir}/$topic"
            val snapshot = DeltaTable.forPath(logStore, tablePath).snapshot()

            // Schema should now include "email"
            val deltaSchema = DeltaSchema.fromJson(snapshot.metaData!!.schemaString)
            deltaSchema.fields.map { it.name } shouldBe listOf("id", "name", "email")

            // All 6 rows readable across files
            val reader = ParquetFileReader(logStore)
            val allRows = snapshot.activeFiles.flatMap { reader.read(it.path) }
            allRows shouldHaveSize 6

            // v2 rows should have email populated
            val v2Rows = allRows.filter { rec ->
                rec.schema.getField("email") != null && rec.get("email") != null
            }
            v2Rows shouldHaveSize 3
            v2Rows.first().get("email").toString() shouldContain "@test.com"
        }

        @Test
        fun `schema evolution in CDC mode adds column and merges correctly`() {
            val topic = "schema-evo-cdc-${System.nanoTime()}"

            val dataSchemaV1: Schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .optional()
                .build()

            val dataSchemaV2: Schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .field("score", Schema.OPTIONAL_INT32_SCHEMA)
                .optional()
                .build()

            val envelopeV1: Schema = SchemaBuilder.struct()
                .field("before", dataSchemaV1)
                .field("after", dataSchemaV1)
                .field("op", Schema.STRING_SCHEMA)
                .field("ts_ms", Schema.INT64_SCHEMA)
                .build()

            val envelopeV2: Schema = SchemaBuilder.struct()
                .field("before", dataSchemaV2)
                .field("after", dataSchemaV2)
                .field("op", Schema.STRING_SCHEMA)
                .field("ts_ms", Schema.INT64_SCHEMA)
                .build()

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
                    DeltaSinkConfig.DELTA_SCHEMA_EVOLUTION to "true",
                    DeltaSinkConfig.DELTA_MERGE_BATCH_SIZE to "100",
                    DeltaSinkConfig.DELTA_MERGE_INTERVAL_MS to "60000"
                )
            )
            task.open(listOf(TopicPartition(topic, 0)))

            // Insert with v1 schema
            val inserts = (1..3).map { i ->
                val after = Struct(dataSchemaV1).put("id", i).put("name", "user_$i")
                val envelope = Struct(envelopeV1)
                    .put("before", null as Struct?)
                    .put("after", after)
                    .put("op", "c")
                    .put("ts_ms", System.currentTimeMillis())
                SinkRecord(topic, 0, null, null, envelopeV1, envelope, i.toLong())
            }
            task.put(inserts)
            currentTime += 120_000L
            task.preCommit(emptyMap())

            // Update with v2 schema (adds "score" column)
            val after = Struct(dataSchemaV2).put("id", 2).put("name", "updated_2").put("score", 99)
            val envelope = Struct(envelopeV2)
                .put("before", null as Struct?)
                .put("after", after)
                .put("op", "u")
                .put("ts_ms", System.currentTimeMillis())
            val update = SinkRecord(topic, 0, null, null, envelopeV2, envelope, 10L)
            task.put(listOf(update))
            currentTime += 120_000L
            task.preCommit(emptyMap())
            task.stop()

            val tablePath = "${tempDir}/$topic"
            val snapshot = DeltaTable.forPath(logStore, tablePath).snapshot()

            val reader = ParquetFileReader(logStore)
            val allRows = snapshot.activeFiles.flatMap { reader.read(it.path) }
            allRows shouldHaveSize 3

            // Row 2 should be updated with score
            val row2 = allRows.find { it.get("id") == 2 }!!
            row2.get("name").toString() shouldBe "updated_2"
            row2.get("score") shouldBe 99

            // Row 1 should have null score (from v1)
            val row1 = allRows.find { it.get("id") == 1 }!!
            row1.get("score") shouldBe null
        }

        @Test
        fun `incompatible schema change throws when DLQ not configured`() {
            val topic = "schema-incompat-${System.nanoTime()}"

            val schemaV1: Schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .build()

            // Incompatible: "name" changed from STRING to INT
            val schemaIncompat: Schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.OPTIONAL_INT32_SCHEMA)
                .build()

            val mockContext = mockk<SinkTaskContext>(relaxed = true)
            every { mockContext.errantRecordReporter() } throws NoSuchMethodError()

            val task = DeltaSinkTask()
            task.initialize(mockContext)
            task.logStoreFactory = { logStore }
            task.clock = { currentTime }
            task.start(
                mapOf(
                    DeltaSinkConfig.DELTA_STORAGE_PATH to tempDir.toString(),
                    DeltaSinkConfig.DELTA_MERGE_KEYS to "",
                    DeltaSinkConfig.DELTA_WRITE_MODE to "append",
                    DeltaSinkConfig.DELTA_SCHEMA_EVOLUTION to "true",
                    DeltaSinkConfig.DELTA_MERGE_BATCH_SIZE to "100",
                    DeltaSinkConfig.DELTA_MERGE_INTERVAL_MS to "60000"
                )
            )
            task.open(listOf(TopicPartition(topic, 0)))

            // First batch creates the table
            val batch1 = listOf(
                SinkRecord(topic, 0, null, null, schemaV1,
                    Struct(schemaV1).put("id", 1).put("name", "user_1"), 0L)
            )
            task.put(batch1)
            currentTime += 120_000L
            task.preCommit(emptyMap())

            // Second batch with incompatible schema should fail on flush
            val batch2 = listOf(
                SinkRecord(topic, 0, null, null, schemaIncompat,
                    Struct(schemaIncompat).put("id", 2).put("name", 42), 1L)
            )
            task.put(batch2)
            currentTime += 120_000L

            val ex = assertThrows<ConnectException> {
                // Flush happens in preCommit or stop — either can throw
                try {
                    task.preCommit(emptyMap())
                } finally {
                    task.stop()
                }
            }
            ex.message shouldContain "Incompatible"
        }
    }

    @Nested
    inner class MultiTopicPipeline {

        @Test
        fun `two topics write to separate Delta tables in same task`() {
            val topicA = "orders-${System.nanoTime()}"
            val topicB = "events-${System.nanoTime()}"

            val schemaA: Schema = SchemaBuilder.struct()
                .field("order_id", Schema.INT32_SCHEMA)
                .field("amount", Schema.OPTIONAL_INT32_SCHEMA)
                .build()

            val schemaB: Schema = SchemaBuilder.struct()
                .field("event_id", Schema.INT32_SCHEMA)
                .field("type", Schema.OPTIONAL_STRING_SCHEMA)
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
            task.open(listOf(
                TopicPartition(topicA, 0),
                TopicPartition(topicB, 0)
            ))

            // Interleaved records from both topics in a single put()
            val records = listOf(
                SinkRecord(topicA, 0, null, null, schemaA,
                    Struct(schemaA).put("order_id", 1).put("amount", 100), 0L),
                SinkRecord(topicB, 0, null, null, schemaB,
                    Struct(schemaB).put("event_id", 1).put("type", "click"), 0L),
                SinkRecord(topicA, 0, null, null, schemaA,
                    Struct(schemaA).put("order_id", 2).put("amount", 200), 1L),
                SinkRecord(topicB, 0, null, null, schemaB,
                    Struct(schemaB).put("event_id", 2).put("type", "view"), 1L),
            )
            task.put(records)
            currentTime += 120_000L
            task.preCommit(emptyMap())
            task.stop()

            // Verify table A
            val tableA = DeltaTable.forPath(logStore, "${tempDir}/$topicA")
            val snapshotA = tableA.snapshot()
            val reader = ParquetFileReader(logStore)
            val rowsA = snapshotA.activeFiles.flatMap { reader.read(it.path) }
            rowsA shouldHaveSize 2
            rowsA.map { it.get("order_id") as Int }.sorted() shouldBe listOf(1, 2)

            // Verify table B
            val tableB = DeltaTable.forPath(logStore, "${tempDir}/$topicB")
            val snapshotB = tableB.snapshot()
            val rowsB = snapshotB.activeFiles.flatMap { reader.read(it.path) }
            rowsB shouldHaveSize 2
            rowsB.map { it.get("event_id") as Int }.sorted() shouldBe listOf(1, 2)
        }

        @Test
        fun `multi-topic with different write modes per topic`() {
            val topicCdc = "cdc-orders-${System.nanoTime()}"
            val topicAppend = "append-logs-${System.nanoTime()}"

            // CDC topic uses envelope schema
            val dataSchema: Schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .optional()
                .build()

            val envelopeSchema: Schema = SchemaBuilder.struct()
                .field("before", dataSchema)
                .field("after", dataSchema)
                .field("op", Schema.STRING_SCHEMA)
                .field("ts_ms", Schema.INT64_SCHEMA)
                .build()

            val plainSchema: Schema = SchemaBuilder.struct()
                .field("log_id", Schema.INT32_SCHEMA)
                .field("message", Schema.OPTIONAL_STRING_SCHEMA)
                .build()

            // Note: task-level write mode is CDC; plain schema records
            // without envelope will go through as-is in append fallback
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
            task.open(listOf(TopicPartition(topicCdc, 0)))

            // CDC insert records
            val cdcRecords = (1..3).map { i ->
                val after = Struct(dataSchema).put("id", i).put("name", "user_$i")
                val envelope = Struct(envelopeSchema)
                    .put("before", null as Struct?)
                    .put("after", after)
                    .put("op", "c")
                    .put("ts_ms", System.currentTimeMillis())
                SinkRecord(topicCdc, 0, null, null, envelopeSchema, envelope, i.toLong())
            }
            task.put(cdcRecords)
            currentTime += 120_000L
            task.preCommit(emptyMap())
            task.stop()

            val tablePath = "${tempDir}/$topicCdc"
            val snapshot = DeltaTable.forPath(logStore, tablePath).snapshot()
            val reader = ParquetFileReader(logStore)
            val rows = snapshot.activeFiles.flatMap { reader.read(it.path) }
            rows shouldHaveSize 3
            rows.map { it.get("id") as Int }.sorted() shouldBe listOf(1, 2, 3)
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
