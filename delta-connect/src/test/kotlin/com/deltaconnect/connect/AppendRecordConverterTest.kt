package com.deltaconnect.connect

import com.deltaconnect.protocol.merge.MergeOperation
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.junit.jupiter.api.Test

class AppendRecordConverterTest {
    private val converter = AppendRecordConverter()

    private val schema =
        SchemaBuilder
            .struct()
            .field("id", Schema.INT32_SCHEMA)
            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
            .field("value", Schema.OPTIONAL_INT32_SCHEMA)
            .build()

    @Test
    fun `converts record to INSERT`() {
        val value = Struct(schema).put("id", 1).put("name", "Alice").put("value", 100)
        val record = SinkRecord("topic", 0, null, null, schema, value, 0)

        val result = converter.convert(record, deleteEnabled = true)

        result.shouldNotBeNull()
        result.operation shouldBe MergeOperation.INSERT
        result.record.get("id") shouldBe 1
        result.record.get("name").toString() shouldBe "Alice"
        result.record.get("value") shouldBe 100
    }

    @Test
    fun `always produces INSERT regardless of deleteEnabled`() {
        val value = Struct(schema).put("id", 1).put("name", "Bob").put("value", 200)
        val record = SinkRecord("topic", 0, null, null, schema, value, 0)

        val result = converter.convert(record, deleteEnabled = false)

        result.shouldNotBeNull()
        result.operation shouldBe MergeOperation.INSERT
    }

    @Test
    fun `skips tombstone (null value)`() {
        val record = SinkRecord("topic", 0, null, null, null, null, 0)

        val result = converter.convert(record, deleteEnabled = true)

        result.shouldBeNull()
    }

    @Test
    fun `extracts schema correctly`() {
        val value = Struct(schema).put("id", 1).put("name", "Charlie").put("value", 300)
        val record = SinkRecord("topic", 0, null, null, schema, value, 0)

        val deltaSchema = converter.extractSchema(record)

        deltaSchema.fields.size shouldBe 3
        deltaSchema.fields[0].name shouldBe "id"
        deltaSchema.fields[1].name shouldBe "name"
        deltaSchema.fields[2].name shouldBe "value"
    }

    @Test
    fun `handles null optional fields`() {
        val value =
            Struct(schema)
                .put("id", 1)
                .put("name", null as String?)
                .put("value", null as Int?)
        val record = SinkRecord("topic", 0, null, null, schema, value, 0)

        val result = converter.convert(record, deleteEnabled = true)

        result.shouldNotBeNull()
        result.record.get("id") shouldBe 1
        result.record.get("name") shouldBe null
        result.record.get("value") shouldBe null
    }

    @Test
    fun `handles multiple records with same schema`() {
        val records =
            (1..5).map { i ->
                val value = Struct(schema).put("id", i).put("name", "name_$i").put("value", i * 100)
                SinkRecord("topic", 0, null, null, schema, value, i.toLong())
            }

        val results = records.map { converter.convert(it, deleteEnabled = true) }

        results.size shouldBe 5
        results.all { it != null && it.operation == MergeOperation.INSERT } shouldBe true
        results[0]!!.record.get("id") shouldBe 1
        results[4]!!.record.get("id") shouldBe 5
    }
}
