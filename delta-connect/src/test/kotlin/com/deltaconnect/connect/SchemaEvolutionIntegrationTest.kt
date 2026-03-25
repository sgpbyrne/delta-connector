package com.deltaconnect.connect

import com.deltaconnect.protocol.DeltaTable
import com.deltaconnect.protocol.merge.MergeOperation
import com.deltaconnect.protocol.merge.SourceRecord
import com.deltaconnect.protocol.parquet.ParquetFileReader
import com.deltaconnect.protocol.schema.DeltaSchema
import com.deltaconnect.protocol.schema.typeName
import com.deltaconnect.protocol.storage.LocalFileSystemLogStore
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.errors.ConnectException
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path

class SchemaEvolutionIntegrationTest {
    @TempDir
    lateinit var tempDir: Path

    private lateinit var logStore: LocalFileSystemLogStore

    private val schemaV1 =
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

    private val schemaV2 =
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
            .optionalString("email")
            .endRecord()

    private val schemaV1Wide =
        SchemaBuilder
            .record("record")
            .namespace("com.deltaconnect")
            .fields()
            .requiredInt("id")
            .optionalString("name")
            .name("value")
            .type()
            .nullable()
            .longType()
            .noDefault()
            .endRecord()

    private val schemaIncompat =
        SchemaBuilder
            .record("record")
            .namespace("com.deltaconnect")
            .fields()
            .requiredInt("id")
            .name("name")
            .type()
            .nullable()
            .intType()
            .noDefault()
            .name("value")
            .type()
            .nullable()
            .intType()
            .noDefault()
            .endRecord()

    @BeforeEach
    fun setUp() {
        logStore = LocalFileSystemLogStore(tempDir)
    }

    @Nested
    inner class NewNullableColumn {
        @Test
        fun `adds new nullable column via schema evolution in append mode`() {
            val writer = appendWriter("test_table", schemaEvolutionEnabled = true)

            writer.buffer(recordV1(1, "alice", 100), tp(), 0L)
            writer.flush()

            writer.buffer(recordV2(2, "bob", 200, "bob@test.com"), tp(), 1L)
            writer.flush()

            val snapshot = DeltaTable.forPath(logStore, "test_table").snapshot()
            snapshot.version shouldBe 2L

            val deltaSchema = DeltaSchema.fromJson(snapshot.metaData!!.schemaString)
            deltaSchema.fields shouldHaveSize 4
            deltaSchema.fields[3].name shouldBe "email"
            deltaSchema.fields[3].nullable shouldBe true

            val reader = ParquetFileReader(logStore)
            val allRows = snapshot.activeFiles.flatMap { reader.read(it.path) }
            allRows shouldHaveSize 2
        }

        @Test
        fun `adds new nullable column via schema evolution in CDC mode`() {
            val writer = mergeWriter("test_table", schemaEvolutionEnabled = true)

            writer.buffer(recordV1(1, "alice", 100), tp(), 0L)
            writer.buffer(recordV1(2, "bob", 200), tp(), 1L)
            writer.flush()

            writer.buffer(
                SourceRecord(
                    recordV2Data(1, "alice_updated", 100, "alice@test.com"),
                    MergeOperation.UPDATE,
                ),
                tp(),
                2L,
            )
            writer.flush()

            val snapshot = DeltaTable.forPath(logStore, "test_table").snapshot()
            snapshot.version shouldBe 2L

            val reader = ParquetFileReader(logStore)
            val allRows = snapshot.activeFiles.flatMap { reader.read(it.path) }
            allRows shouldHaveSize 2

            val rowMap = allRows.associate { (it.get("id") as Int) to it }
            rowMap[1]!!.get("name").toString() shouldBe "alice_updated"
            rowMap[2]!!.get("email") shouldBe null
        }
    }

    @Nested
    inner class TypeWidening {
        @Test
        fun `widens INT to LONG via schema evolution`() {
            val writer = appendWriter("test_table", schemaEvolutionEnabled = true)

            writer.buffer(recordV1(1, "alice", 100), tp(), 0L)
            writer.flush()

            val rec = GenericData.Record(schemaV1Wide)
            rec.put("id", 2)
            rec.put("name", "bob")
            rec.put("value", 200L)
            writer.buffer(SourceRecord(rec, MergeOperation.INSERT), tp(), 1L)
            writer.flush()

            val snapshot = DeltaTable.forPath(logStore, "test_table").snapshot()
            snapshot.version shouldBe 2L

            val deltaSchema = DeltaSchema.fromJson(snapshot.metaData!!.schemaString)
            deltaSchema.fields
                .find { it.name == "value" }!!
                .type.typeName shouldBe "long"
        }
    }

    @Nested
    inner class IncompatibleSchema {
        @Test
        fun `throws ConnectException for incompatible schema change`() {
            val writer = appendWriter("test_table", schemaEvolutionEnabled = true)

            writer.buffer(recordV1(1, "alice", 100), tp(), 0L)
            writer.flush()

            val rec = GenericData.Record(schemaIncompat)
            rec.put("id", 2)
            rec.put("name", 42)
            rec.put("value", 200)
            writer.buffer(SourceRecord(rec, MergeOperation.INSERT), tp(), 1L)

            val ex = assertThrows<ConnectException> { writer.flush() }
            ex.message shouldContain "Incompatible"
        }
    }

    @Nested
    inner class EvolutionDisabled {
        @Test
        fun `throws ConnectException when evolution disabled and schema changes`() {
            val writer = appendWriter("test_table", schemaEvolutionEnabled = false)

            writer.buffer(recordV1(1, "alice", 100), tp(), 0L)
            writer.flush()

            writer.buffer(recordV2(2, "bob", 200, "bob@test.com"), tp(), 1L)

            val ex = assertThrows<ConnectException> { writer.flush() }
            ex.message shouldContain "schema evolution is disabled"
        }

        @Test
        fun `same schema works fine when evolution disabled`() {
            val writer = appendWriter("test_table", schemaEvolutionEnabled = false)

            writer.buffer(recordV1(1, "alice", 100), tp(), 0L)
            writer.flush()

            writer.buffer(recordV1(2, "bob", 200), tp(), 1L)
            writer.flush()

            val snapshot = DeltaTable.forPath(logStore, "test_table").snapshot()
            snapshot.version shouldBe 2L
        }
    }

    @Nested
    inner class RecordProjection {
        @Test
        fun `projectRecord fills missing fields with null`() {
            val record = GenericData.Record(schemaV1)
            record.put("id", 1)
            record.put("name", "alice")
            record.put("value", 100)

            val projected = TableWriter.projectRecord(record, schemaV2)

            projected.get("id") shouldBe 1
            projected.get("name").toString() shouldBe "alice"
            projected.get("value") shouldBe 100
            projected.get("email") shouldBe null
        }

        @Test
        fun `projectRecord returns same record when schemas match`() {
            val record = GenericData.Record(schemaV1)
            record.put("id", 1)
            record.put("name", "alice")
            record.put("value", 100)

            val projected = TableWriter.projectRecord(record, schemaV1)

            (projected === record) shouldBe true
        }
    }

    private fun tp(): TopicPartition = TopicPartition("test_topic", 0)

    private fun mergeWriter(
        tablePath: String,
        schemaEvolutionEnabled: Boolean = true,
    ): TableWriter =
        TableWriter(
            logStore = logStore,
            tablePath = tablePath,
            mergeKeys = listOf("id"),
            writeMode = DeltaSinkConfig.WriteMode.CDC,
            schemaEvolutionEnabled = schemaEvolutionEnabled,
        )

    private fun appendWriter(
        tablePath: String,
        schemaEvolutionEnabled: Boolean = true,
    ): TableWriter =
        TableWriter(
            logStore = logStore,
            tablePath = tablePath,
            mergeKeys = emptyList(),
            writeMode = DeltaSinkConfig.WriteMode.APPEND,
            schemaEvolutionEnabled = schemaEvolutionEnabled,
        )

    private fun recordV1(
        id: Int,
        name: String,
        value: Int,
    ): SourceRecord {
        val rec = GenericData.Record(schemaV1)
        rec.put("id", id)
        rec.put("name", name)
        rec.put("value", value)
        return SourceRecord(rec, MergeOperation.INSERT)
    }

    private fun recordV2(
        id: Int,
        name: String,
        value: Int,
        email: String?,
    ): SourceRecord = SourceRecord(recordV2Data(id, name, value, email), MergeOperation.INSERT)

    private fun recordV2Data(
        id: Int,
        name: String,
        value: Int,
        email: String?,
    ): GenericRecord {
        val rec = GenericData.Record(schemaV2)
        rec.put("id", id)
        rec.put("name", name)
        rec.put("value", value)
        rec.put("email", email)
        return rec
    }
}
