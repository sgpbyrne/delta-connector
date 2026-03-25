package com.deltaconnect.connect.cdc

import com.deltaconnect.connect.DeltaSinkConfig.CdcEnvelopeFormat
import com.deltaconnect.protocol.merge.MergeOperation
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.header.ConnectHeaders
import org.apache.kafka.connect.sink.SinkRecord
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import org.apache.kafka.connect.data.Decimal as ConnectDecimal

class DebeziumRecordConverterTest {
    private val dataSchema =
        SchemaBuilder
            .struct()
            .field("id", Schema.INT32_SCHEMA)
            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
            .field("value", Schema.OPTIONAL_INT32_SCHEMA)
            .optional()
            .build()

    private val envelopeSchema =
        SchemaBuilder
            .struct()
            .field("before", dataSchema)
            .field("after", dataSchema)
            .field("op", Schema.STRING_SCHEMA)
            .field("ts_ms", Schema.INT64_SCHEMA)
            .build()

    @Nested
    inner class FullEnvelope {
        private val converter = DebeziumRecordConverter(CdcEnvelopeFormat.DEBEZIUM_FULL)

        @Test
        fun `converts create (op=c) to INSERT`() {
            val after =
                Struct(dataSchema)
                    .put("id", 1)
                    .put("name", "Alice")
                    .put("value", 100)
            val envelope = makeEnvelope(before = null, after = after, op = "c")

            val result = converter.convert(sinkRecord(envelope), deleteEnabled = true)

            result.shouldNotBeNull()
            result.operation shouldBe MergeOperation.INSERT
            result.record.get("id") shouldBe 1
            result.record.get("name").toString() shouldBe "Alice"
            result.record.get("value") shouldBe 100
        }

        @Test
        fun `converts read (op=r) to INSERT`() {
            val after =
                Struct(dataSchema)
                    .put("id", 2)
                    .put("name", "Bob")
                    .put("value", 200)
            val envelope = makeEnvelope(before = null, after = after, op = "r")

            val result = converter.convert(sinkRecord(envelope), deleteEnabled = true)

            result.shouldNotBeNull()
            result.operation shouldBe MergeOperation.INSERT
            result.record.get("id") shouldBe 2
        }

        @Test
        fun `converts update (op=u) to UPDATE`() {
            val before =
                Struct(dataSchema)
                    .put("id", 1)
                    .put("name", "Alice")
                    .put("value", 100)
            val after =
                Struct(dataSchema)
                    .put("id", 1)
                    .put("name", "Alice")
                    .put("value", 200)
            val envelope = makeEnvelope(before = before, after = after, op = "u")

            val result = converter.convert(sinkRecord(envelope), deleteEnabled = true)

            result.shouldNotBeNull()
            result.operation shouldBe MergeOperation.UPDATE
            result.record.get("id") shouldBe 1
            result.record.get("value") shouldBe 200
        }

        @Test
        fun `converts delete (op=d) to DELETE using before`() {
            val before =
                Struct(dataSchema)
                    .put("id", 1)
                    .put("name", "Alice")
                    .put("value", 100)
            val envelope = makeEnvelope(before = before, after = null, op = "d")

            val result = converter.convert(sinkRecord(envelope), deleteEnabled = true)

            result.shouldNotBeNull()
            result.operation shouldBe MergeOperation.DELETE
            result.record.get("id") shouldBe 1
        }

        @Test
        fun `skips delete when deleteEnabled is false`() {
            val before =
                Struct(dataSchema)
                    .put("id", 1)
                    .put("name", "Alice")
                    .put("value", 100)
            val envelope = makeEnvelope(before = before, after = null, op = "d")

            val result = converter.convert(sinkRecord(envelope), deleteEnabled = false)

            result.shouldBeNull()
        }

        @Test
        fun `skips tombstone (null value)`() {
            val record = SinkRecord("topic", 0, null, null, null, null, 0)

            val result = converter.convert(record, deleteEnabled = true)

            result.shouldBeNull()
        }

        @Test
        fun `skips unknown op code`() {
            val after =
                Struct(dataSchema)
                    .put("id", 1)
                    .put("name", "Alice")
                    .put("value", 100)
            val envelope = makeEnvelope(before = null, after = after, op = "x")

            val result = converter.convert(sinkRecord(envelope), deleteEnabled = true)

            result.shouldBeNull()
        }

        @Test
        fun `handles null optional fields`() {
            val after =
                Struct(dataSchema)
                    .put("id", 5)
                    .put("name", null as String?)
                    .put("value", null as Int?)
            val envelope = makeEnvelope(before = null, after = after, op = "c")

            val result = converter.convert(sinkRecord(envelope), deleteEnabled = true)

            result.shouldNotBeNull()
            result.record.get("id") shouldBe 5
            result.record.get("name") shouldBe null
            result.record.get("value") shouldBe null
        }

        @Test
        fun `extracts schema from after field`() {
            val after =
                Struct(dataSchema)
                    .put("id", 1)
                    .put("name", "Alice")
                    .put("value", 100)
            val envelope = makeEnvelope(before = null, after = after, op = "c")

            val schema = converter.extractSchema(sinkRecord(envelope))

            schema.fields.size shouldBe 3
            schema.fields[0].name shouldBe "id"
            schema.fields[1].name shouldBe "name"
            schema.fields[2].name shouldBe "value"
        }

        @Test
        fun `PK update produces DELETE plus INSERT`() {
            // In Debezium, a PK change produces two records:
            // 1. DELETE with old key
            val deleteBefore =
                Struct(dataSchema)
                    .put("id", 1)
                    .put("name", "Alice")
                    .put("value", 100)
            val deleteEnvelope =
                makeEnvelope(
                    before = deleteBefore,
                    after = null,
                    op = "d",
                )

            val deleteResult =
                converter.convert(
                    sinkRecord(deleteEnvelope, offset = 0),
                    deleteEnabled = true,
                )
            deleteResult.shouldNotBeNull()
            deleteResult.operation shouldBe MergeOperation.DELETE
            deleteResult.record.get("id") shouldBe 1

            // 2. INSERT with new key
            val insertAfter =
                Struct(dataSchema)
                    .put("id", 999)
                    .put("name", "Alice")
                    .put("value", 100)
            val insertEnvelope =
                makeEnvelope(
                    before = null,
                    after = insertAfter,
                    op = "c",
                )

            val insertResult =
                converter.convert(
                    sinkRecord(insertEnvelope, offset = 1),
                    deleteEnabled = true,
                )
            insertResult.shouldNotBeNull()
            insertResult.operation shouldBe MergeOperation.INSERT
            insertResult.record.get("id") shouldBe 999
        }

        @Test
        fun `batch dedup handled by merge engine ordering`() {
            // Multiple ops for same key in order - converter produces all,
            // merge engine deduplicates by keeping the last per key
            val ops =
                listOf(
                    "c" to 100, // insert id=1 value=100
                    "u" to 200, // update id=1 value=200
                    "u" to 300, // update id=1 value=300
                )

            val results =
                ops.mapIndexed { i, (op, v) ->
                    val after =
                        Struct(dataSchema)
                            .put("id", 1)
                            .put("name", "Alice")
                            .put("value", v)
                    val before =
                        if (op == "u") {
                            Struct(dataSchema).put("id", 1).put("name", "Alice").put("value", v - 100)
                        } else {
                            null
                        }
                    val envelope = makeEnvelope(before = before, after = after, op = op)
                    converter.convert(sinkRecord(envelope, offset = i.toLong()), deleteEnabled = true)
                }

            // All 3 records converted — dedup is merge engine's responsibility
            results.size shouldBe 3
            results.all { it != null } shouldBe true
            results[0]!!.operation shouldBe MergeOperation.INSERT
            results[1]!!.operation shouldBe MergeOperation.UPDATE
            results[2]!!.operation shouldBe MergeOperation.UPDATE
            results[2]!!.record.get("value") shouldBe 300
        }
    }

    @Nested
    inner class FlattenedEnvelope {
        private val converter = DebeziumRecordConverter(CdcEnvelopeFormat.DEBEZIUM_FLATTENED)

        private val flatSchema =
            SchemaBuilder
                .struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .field("value", Schema.OPTIONAL_INT32_SCHEMA)
                .build()

        private val flatSchemaWithMeta =
            SchemaBuilder
                .struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .field("value", Schema.OPTIONAL_INT32_SCHEMA)
                .field("__deleted", Schema.OPTIONAL_STRING_SCHEMA)
                .build()

        @Test
        fun `converts flat record to INSERT by default`() {
            val value =
                Struct(flatSchema)
                    .put("id", 1)
                    .put("name", "Alice")
                    .put("value", 100)

            val result = converter.convert(flatSinkRecord(value, flatSchema), true)

            result.shouldNotBeNull()
            result.operation shouldBe MergeOperation.INSERT
            result.record.get("id") shouldBe 1
        }

        @Test
        fun `detects DELETE from __deleted field`() {
            val value =
                Struct(flatSchemaWithMeta)
                    .put("id", 1)
                    .put("name", "Alice")
                    .put("value", 100)
                    .put("__deleted", "true")

            val result =
                converter.convert(
                    flatSinkRecord(value, flatSchemaWithMeta),
                    true,
                )

            result.shouldNotBeNull()
            result.operation shouldBe MergeOperation.DELETE
            result.record.schema.getField("__deleted") shouldBe null
        }

        @Test
        fun `non-deleted record with __deleted=false is INSERT`() {
            val value =
                Struct(flatSchemaWithMeta)
                    .put("id", 1)
                    .put("name", "Alice")
                    .put("value", 100)
                    .put("__deleted", "false")

            val result =
                converter.convert(
                    flatSinkRecord(value, flatSchemaWithMeta),
                    true,
                )

            result.shouldNotBeNull()
            result.operation shouldBe MergeOperation.INSERT
        }

        @Test
        fun `detects operation from __op header`() {
            val value =
                Struct(flatSchema)
                    .put("id", 1)
                    .put("name", "Alice")
                    .put("value", 200)
            val headers = ConnectHeaders().addString("__op", "u")
            val record =
                SinkRecord(
                    "topic",
                    0,
                    null,
                    null,
                    flatSchema,
                    value,
                    0,
                    null,
                    null,
                    headers,
                )

            val result = converter.convert(record, deleteEnabled = true)

            result.shouldNotBeNull()
            result.operation shouldBe MergeOperation.UPDATE
        }

        @Test
        fun `detects DELETE from __op header`() {
            val value =
                Struct(flatSchema)
                    .put("id", 1)
                    .put("name", "Alice")
                    .put("value", 100)
            val headers = ConnectHeaders().addString("__op", "d")
            val record =
                SinkRecord(
                    "topic",
                    0,
                    null,
                    null,
                    flatSchema,
                    value,
                    0,
                    null,
                    null,
                    headers,
                )

            val result = converter.convert(record, deleteEnabled = true)

            result.shouldNotBeNull()
            result.operation shouldBe MergeOperation.DELETE
        }

        @Test
        fun `header __op takes priority over __deleted field`() {
            val value =
                Struct(flatSchemaWithMeta)
                    .put("id", 1)
                    .put("name", "Alice")
                    .put("value", 100)
                    .put("__deleted", "true")
            val headers = ConnectHeaders().addString("__op", "u")
            val record =
                SinkRecord(
                    "topic",
                    0,
                    null,
                    null,
                    flatSchemaWithMeta,
                    value,
                    0,
                    null,
                    null,
                    headers,
                )

            val result = converter.convert(record, deleteEnabled = true)

            result.shouldNotBeNull()
            result.operation shouldBe MergeOperation.UPDATE
        }

        @Test
        fun `skips DELETE when deleteEnabled is false`() {
            val value =
                Struct(flatSchemaWithMeta)
                    .put("id", 1)
                    .put("name", "Alice")
                    .put("value", 100)
                    .put("__deleted", "true")

            val result =
                converter.convert(
                    flatSinkRecord(value, flatSchemaWithMeta),
                    false,
                )

            result.shouldBeNull()
        }

        @Test
        fun `skips tombstone`() {
            val record = SinkRecord("topic", 0, null, null, null, null, 0)

            val result = converter.convert(record, deleteEnabled = true)

            result.shouldBeNull()
        }

        @Test
        fun `extracts schema filtering metadata fields`() {
            val value =
                Struct(flatSchemaWithMeta)
                    .put("id", 1)
                    .put("name", "Alice")
                    .put("value", 100)
                    .put("__deleted", "false")

            val schema =
                converter.extractSchema(
                    flatSinkRecord(value, flatSchemaWithMeta),
                )

            schema.fields.size shouldBe 3
            schema.fields.none { it.name.startsWith("__") } shouldBe true
        }

        private fun flatSinkRecord(
            value: Struct,
            schema: Schema,
        ): SinkRecord = SinkRecord("topic", 0, null, null, schema, value, 0)
    }

    @Nested
    inner class OperationMapping {
        @Test
        fun `maps all Debezium op codes`() {
            DebeziumRecordConverter.mapOperation("r") shouldBe MergeOperation.INSERT
            DebeziumRecordConverter.mapOperation("c") shouldBe MergeOperation.INSERT
            DebeziumRecordConverter.mapOperation("u") shouldBe MergeOperation.UPDATE
            DebeziumRecordConverter.mapOperation("d") shouldBe MergeOperation.DELETE
        }

        @Test
        fun `returns null for unknown op`() {
            DebeziumRecordConverter.mapOperation("x").shouldBeNull()
            DebeziumRecordConverter.mapOperation(null).shouldBeNull()
        }
    }

    @Nested
    inner class MetadataFiltering {
        @Test
        fun `filterMetadataFields removes __ prefixed fields`() {
            val schema =
                SchemaBuilder
                    .struct()
                    .field("id", Schema.INT32_SCHEMA)
                    .field("name", Schema.STRING_SCHEMA)
                    .field("__deleted", Schema.STRING_SCHEMA)
                    .field("__op", Schema.STRING_SCHEMA)
                    .field("__source_ts_ms", Schema.INT64_SCHEMA)
                    .build()

            val filtered = DebeziumRecordConverter.filterMetadataFields(schema)

            filtered.fields().size shouldBe 2
            filtered.field("id").shouldNotBeNull()
            filtered.field("name").shouldNotBeNull()
            filtered.field("__deleted") shouldBe null
        }

        @Test
        fun `filterMetadataFields returns same schema when no metadata`() {
            val schema =
                SchemaBuilder
                    .struct()
                    .field("id", Schema.INT32_SCHEMA)
                    .field("name", Schema.STRING_SCHEMA)
                    .build()

            val filtered = DebeziumRecordConverter.filterMetadataFields(schema)

            filtered shouldBe schema // same reference
        }
    }

    @Nested
    inner class SqlServerTypes {
        private val converter = DebeziumRecordConverter(CdcEnvelopeFormat.DEBEZIUM_FULL)

        @Test
        fun `converts SQL Server decimal type`() {
            val schema =
                SchemaBuilder
                    .struct()
                    .field("id", Schema.INT32_SCHEMA)
                    .field("amount", ConnectDecimal.schema(2))
                    .optional()
                    .build()
            val envSchema = makeEnvelopeSchema(schema)

            val after =
                Struct(schema)
                    .put("id", 1)
                    .put("amount", BigDecimal("99.99"))
            val envelope =
                Struct(envSchema)
                    .put("before", null as Struct?)
                    .put("after", after)
                    .put("op", "c")
                    .put("ts_ms", System.currentTimeMillis())

            val result =
                converter.convert(
                    SinkRecord("topic", 0, null, null, envSchema, envelope, 0),
                    deleteEnabled = true,
                )

            result.shouldNotBeNull()
            val bytes = result.record.get("amount") as java.nio.ByteBuffer
            val unscaled = java.math.BigInteger(bytes.array())
            unscaled shouldBe java.math.BigInteger.valueOf(9999)
        }

        @Test
        fun `converts SQL Server datetime2 (MicroTimestamp)`() {
            val tsField =
                SchemaBuilder
                    .int64()
                    .name("io.debezium.time.MicroTimestamp")
                    .build()
            val schema =
                SchemaBuilder
                    .struct()
                    .field("id", Schema.INT32_SCHEMA)
                    .field("created_at", tsField)
                    .optional()
                    .build()
            val envSchema = makeEnvelopeSchema(schema)

            val after =
                Struct(schema)
                    .put("id", 1)
                    .put("created_at", 1609459200000000L)
            val envelope =
                Struct(envSchema)
                    .put("before", null as Struct?)
                    .put("after", after)
                    .put("op", "c")
                    .put("ts_ms", System.currentTimeMillis())

            val result =
                converter.convert(
                    SinkRecord("topic", 0, null, null, envSchema, envelope, 0),
                    deleteEnabled = true,
                )

            result.shouldNotBeNull()
            result.record.get("created_at") shouldBe 1609459200000000L
        }

        @Test
        fun `converts SQL Server date type`() {
            val dateField =
                SchemaBuilder
                    .int32()
                    .name("io.debezium.time.Date")
                    .build()
            val schema =
                SchemaBuilder
                    .struct()
                    .field("id", Schema.INT32_SCHEMA)
                    .field("event_date", dateField)
                    .optional()
                    .build()
            val envSchema = makeEnvelopeSchema(schema)

            val after =
                Struct(schema)
                    .put("id", 1)
                    .put("event_date", 18628)
            val envelope =
                Struct(envSchema)
                    .put("before", null as Struct?)
                    .put("after", after)
                    .put("op", "c")
                    .put("ts_ms", System.currentTimeMillis())

            val result =
                converter.convert(
                    SinkRecord("topic", 0, null, null, envSchema, envelope, 0),
                    deleteEnabled = true,
                )

            result.shouldNotBeNull()
            result.record.get("event_date") shouldBe 18628
        }

        @Test
        fun `converts SQL Server datetime (Timestamp ms to us)`() {
            val tsField =
                SchemaBuilder
                    .int64()
                    .name("io.debezium.time.Timestamp")
                    .build()
            val schema =
                SchemaBuilder
                    .struct()
                    .field("id", Schema.INT32_SCHEMA)
                    .field("modified_at", tsField)
                    .optional()
                    .build()
            val envSchema = makeEnvelopeSchema(schema)

            val after =
                Struct(schema)
                    .put("id", 1)
                    .put("modified_at", 1609459200000L)
            val envelope =
                Struct(envSchema)
                    .put("before", null as Struct?)
                    .put("after", after)
                    .put("op", "c")
                    .put("ts_ms", System.currentTimeMillis())

            val result =
                converter.convert(
                    SinkRecord("topic", 0, null, null, envSchema, envelope, 0),
                    deleteEnabled = true,
                )

            result.shouldNotBeNull()
            result.record.get("modified_at") shouldBe 1609459200000000L
        }

        @Test
        fun `converts SQL Server datetimeoffset (ZonedTimestamp)`() {
            val tsField =
                SchemaBuilder
                    .string()
                    .name("io.debezium.time.ZonedTimestamp")
                    .build()
            val schema =
                SchemaBuilder
                    .struct()
                    .field("id", Schema.INT32_SCHEMA)
                    .field("event_time", tsField)
                    .optional()
                    .build()
            val envSchema = makeEnvelopeSchema(schema)

            val after =
                Struct(schema)
                    .put("id", 1)
                    .put("event_time", "2021-01-01T00:00:00+00:00")
            val envelope =
                Struct(envSchema)
                    .put("before", null as Struct?)
                    .put("after", after)
                    .put("op", "c")
                    .put("ts_ms", System.currentTimeMillis())

            val result =
                converter.convert(
                    SinkRecord("topic", 0, null, null, envSchema, envelope, 0),
                    deleteEnabled = true,
                )

            result.shouldNotBeNull()
            result.record.get("event_time").toString() shouldBe "2021-01-01T00:00:00+00:00"
        }

        private fun makeEnvelopeSchema(dataSchema: Schema): Schema =
            SchemaBuilder
                .struct()
                .field("before", dataSchema)
                .field("after", dataSchema)
                .field("op", Schema.STRING_SCHEMA)
                .field("ts_ms", Schema.INT64_SCHEMA)
                .build()
    }

    private fun makeEnvelope(
        before: Struct?,
        after: Struct?,
        op: String,
    ): Struct =
        Struct(envelopeSchema)
            .put("before", before)
            .put("after", after)
            .put("op", op)
            .put("ts_ms", System.currentTimeMillis())

    private fun sinkRecord(
        envelope: Struct,
        offset: Long = 0,
    ): SinkRecord = SinkRecord("topic", 0, null, null, envelopeSchema, envelope, offset)
}
