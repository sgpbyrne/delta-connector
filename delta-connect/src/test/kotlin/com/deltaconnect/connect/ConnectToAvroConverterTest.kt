package com.deltaconnect.connect

import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import org.apache.avro.LogicalTypes
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.nio.ByteBuffer
import org.apache.avro.Schema as AvroSchema
import org.apache.kafka.connect.data.Date as ConnectDate
import org.apache.kafka.connect.data.Decimal as ConnectDecimal
import org.apache.kafka.connect.data.Timestamp as ConnectTimestamp

class ConnectToAvroConverterTest {
    @Nested
    inner class SchemaConversion {
        @Test
        fun `converts simple struct schema`() {
            val connectSchema =
                SchemaBuilder
                    .struct()
                    .field("id", Schema.INT32_SCHEMA)
                    .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                    .field("active", Schema.BOOLEAN_SCHEMA)
                    .build()

            val avro = ConnectToAvroConverter.toAvroSchema(connectSchema)

            avro.type shouldBe AvroSchema.Type.RECORD
            avro.fields.size shouldBe 3
            avro.getField("id").schema().type shouldBe AvroSchema.Type.INT
            avro.getField("name").schema().type shouldBe AvroSchema.Type.UNION
            avro.getField("active").schema().type shouldBe AvroSchema.Type.BOOLEAN
        }

        @Test
        fun `converts all numeric types`() {
            val connectSchema =
                SchemaBuilder
                    .struct()
                    .field("byte_col", Schema.INT8_SCHEMA)
                    .field("short_col", Schema.INT16_SCHEMA)
                    .field("int_col", Schema.INT32_SCHEMA)
                    .field("long_col", Schema.INT64_SCHEMA)
                    .field("float_col", Schema.FLOAT32_SCHEMA)
                    .field("double_col", Schema.FLOAT64_SCHEMA)
                    .build()

            val avro = ConnectToAvroConverter.toAvroSchema(connectSchema)

            avro.getField("byte_col").schema().type shouldBe AvroSchema.Type.INT
            avro.getField("short_col").schema().type shouldBe AvroSchema.Type.INT
            avro.getField("int_col").schema().type shouldBe AvroSchema.Type.INT
            avro.getField("long_col").schema().type shouldBe AvroSchema.Type.LONG
            avro.getField("float_col").schema().type shouldBe AvroSchema.Type.FLOAT
            avro.getField("double_col").schema().type shouldBe AvroSchema.Type.DOUBLE
        }

        @Test
        fun `converts Connect Decimal with default precision`() {
            val connectSchema =
                SchemaBuilder
                    .struct()
                    .field("amount", ConnectDecimal.schema(2))
                    .build()

            val avro = ConnectToAvroConverter.toAvroSchema(connectSchema)
            val field = avro.getField("amount")

            field.schema().type shouldBe AvroSchema.Type.BYTES
            field.schema().logicalType.name shouldBe "decimal"
            val decimal = field.schema().logicalType as LogicalTypes.Decimal
            decimal.scale shouldBe 2
            decimal.precision shouldBe 38
        }

        @Test
        fun `converts Connect Date to Avro date`() {
            val connectSchema =
                SchemaBuilder
                    .struct()
                    .field("d", ConnectDate.SCHEMA)
                    .build()

            val avro = ConnectToAvroConverter.toAvroSchema(connectSchema)

            avro
                .getField("d")
                .schema()
                .logicalType.name shouldBe "date"
        }

        @Test
        fun `converts Connect Timestamp to timestamp-micros`() {
            val connectSchema =
                SchemaBuilder
                    .struct()
                    .field("ts", ConnectTimestamp.SCHEMA)
                    .build()

            val avro = ConnectToAvroConverter.toAvroSchema(connectSchema)

            avro
                .getField("ts")
                .schema()
                .logicalType.name shouldBe "timestamp-micros"
        }

        @Test
        fun `converts Debezium MicroTimestamp to timestamp-micros`() {
            val tsSchema =
                SchemaBuilder
                    .int64()
                    .name("io.debezium.time.MicroTimestamp")
                    .build()
            val connectSchema =
                SchemaBuilder
                    .struct()
                    .field("ts", tsSchema)
                    .build()

            val avro = ConnectToAvroConverter.toAvroSchema(connectSchema)

            avro
                .getField("ts")
                .schema()
                .logicalType.name shouldBe "timestamp-micros"
        }

        @Test
        fun `converts Debezium Date to Avro date`() {
            val dateSchema =
                SchemaBuilder
                    .int32()
                    .name("io.debezium.time.Date")
                    .build()
            val connectSchema =
                SchemaBuilder
                    .struct()
                    .field("d", dateSchema)
                    .build()

            val avro = ConnectToAvroConverter.toAvroSchema(connectSchema)

            avro
                .getField("d")
                .schema()
                .logicalType.name shouldBe "date"
        }

        @Test
        fun `converts Debezium Timestamp (ms) to timestamp-micros`() {
            val tsSchema =
                SchemaBuilder
                    .int64()
                    .name("io.debezium.time.Timestamp")
                    .build()
            val connectSchema =
                SchemaBuilder
                    .struct()
                    .field("ts", tsSchema)
                    .build()

            val avro = ConnectToAvroConverter.toAvroSchema(connectSchema)

            avro
                .getField("ts")
                .schema()
                .logicalType.name shouldBe "timestamp-micros"
        }

        @Test
        fun `converts Debezium NanoTimestamp to timestamp-micros`() {
            val tsSchema =
                SchemaBuilder
                    .int64()
                    .name("io.debezium.time.NanoTimestamp")
                    .build()
            val connectSchema =
                SchemaBuilder
                    .struct()
                    .field("ts", tsSchema)
                    .build()

            val avro = ConnectToAvroConverter.toAvroSchema(connectSchema)

            avro
                .getField("ts")
                .schema()
                .logicalType.name shouldBe "timestamp-micros"
        }

        @Test
        fun `converts Debezium ZonedTimestamp to string`() {
            val tsSchema =
                SchemaBuilder
                    .string()
                    .name("io.debezium.time.ZonedTimestamp")
                    .build()
            val connectSchema =
                SchemaBuilder
                    .struct()
                    .field("ts", tsSchema)
                    .build()

            val avro = ConnectToAvroConverter.toAvroSchema(connectSchema)

            avro.getField("ts").schema().type shouldBe AvroSchema.Type.STRING
            avro.getField("ts").schema().logicalType shouldBe null
        }

        @Test
        fun `converts Debezium time types to long`() {
            val microTime =
                SchemaBuilder
                    .int64()
                    .name("io.debezium.time.MicroTime")
                    .build()
            val time =
                SchemaBuilder
                    .int32()
                    .name("io.debezium.time.Time")
                    .build()
            val nanoTime =
                SchemaBuilder
                    .int64()
                    .name("io.debezium.time.NanoTime")
                    .build()
            val connectSchema =
                SchemaBuilder
                    .struct()
                    .field("t1", microTime)
                    .field("t2", time)
                    .field("t3", nanoTime)
                    .build()

            val avro = ConnectToAvroConverter.toAvroSchema(connectSchema)

            avro.getField("t1").schema().type shouldBe AvroSchema.Type.LONG
            avro.getField("t2").schema().type shouldBe AvroSchema.Type.LONG
            avro.getField("t3").schema().type shouldBe AvroSchema.Type.LONG
        }

        @Test
        fun `converts Debezium VariableScaleDecimal to string`() {
            val vsdSchema =
                SchemaBuilder
                    .struct()
                    .name("io.debezium.data.VariableScaleDecimal")
                    .field("scale", Schema.INT32_SCHEMA)
                    .field("value", Schema.BYTES_SCHEMA)
                    .build()
            val connectSchema =
                SchemaBuilder
                    .struct()
                    .field("amount", vsdSchema)
                    .build()

            val avro = ConnectToAvroConverter.toAvroSchema(connectSchema)

            avro.getField("amount").schema().type shouldBe AvroSchema.Type.STRING
        }

        @Test
        fun `converts bytes type`() {
            val connectSchema =
                SchemaBuilder
                    .struct()
                    .field("data", Schema.BYTES_SCHEMA)
                    .build()

            val avro = ConnectToAvroConverter.toAvroSchema(connectSchema)

            avro.getField("data").schema().type shouldBe AvroSchema.Type.BYTES
        }
    }

    @Nested
    inner class DataConversion {
        @Test
        fun `converts simple struct values`() {
            val schema =
                SchemaBuilder
                    .struct()
                    .field("id", Schema.INT32_SCHEMA)
                    .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                    .field("active", Schema.BOOLEAN_SCHEMA)
                    .build()
            val struct = Struct(schema).put("id", 42).put("name", "Alice").put("active", true)
            val avro = ConnectToAvroConverter.toAvroSchema(schema)

            val record = ConnectToAvroConverter.toGenericRecord(struct, avro)

            record.get("id") shouldBe 42
            record.get("name").toString() shouldBe "Alice"
            record.get("active") shouldBe true
        }

        @Test
        fun `converts null optional fields`() {
            val schema =
                SchemaBuilder
                    .struct()
                    .field("id", Schema.INT32_SCHEMA)
                    .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                    .build()
            val struct = Struct(schema).put("id", 1).put("name", null as String?)
            val avro = ConnectToAvroConverter.toAvroSchema(schema)

            val record = ConnectToAvroConverter.toGenericRecord(struct, avro)

            record.get("id") shouldBe 1
            record.get("name") shouldBe null
        }

        @Test
        fun `widens INT8 and INT16 to INT`() {
            val schema =
                SchemaBuilder
                    .struct()
                    .field("b", Schema.INT8_SCHEMA)
                    .field("s", Schema.INT16_SCHEMA)
                    .build()
            val struct = Struct(schema).put("b", 42.toByte()).put("s", 1000.toShort())
            val avro = ConnectToAvroConverter.toAvroSchema(schema)

            val record = ConnectToAvroConverter.toGenericRecord(struct, avro)

            record.get("b") shouldBe 42
            record.get("s") shouldBe 1000
        }

        @Test
        fun `converts Connect Decimal to ByteBuffer`() {
            val schema =
                SchemaBuilder
                    .struct()
                    .field("amount", ConnectDecimal.schema(2))
                    .build()
            val struct = Struct(schema).put("amount", BigDecimal("123.45"))
            val avro = ConnectToAvroConverter.toAvroSchema(schema)

            val record = ConnectToAvroConverter.toGenericRecord(struct, avro)

            val bytes = record.get("amount") as ByteBuffer
            bytes shouldNotBe null
            val unscaled = java.math.BigInteger(bytes.array())
            unscaled shouldBe java.math.BigInteger.valueOf(12345)
        }

        @Test
        fun `converts Debezium MicroTimestamp unchanged`() {
            val tsSchema =
                SchemaBuilder
                    .int64()
                    .name("io.debezium.time.MicroTimestamp")
                    .build()
            val schema = SchemaBuilder.struct().field("ts", tsSchema).build()
            val struct = Struct(schema).put("ts", 1234567890123456L)
            val avro = ConnectToAvroConverter.toAvroSchema(schema)

            val record = ConnectToAvroConverter.toGenericRecord(struct, avro)

            record.get("ts") shouldBe 1234567890123456L
        }

        @Test
        fun `converts Debezium Timestamp ms to us`() {
            val tsSchema =
                SchemaBuilder
                    .int64()
                    .name("io.debezium.time.Timestamp")
                    .build()
            val schema = SchemaBuilder.struct().field("ts", tsSchema).build()
            val struct = Struct(schema).put("ts", 1609459200000L)
            val avro = ConnectToAvroConverter.toAvroSchema(schema)

            val record = ConnectToAvroConverter.toGenericRecord(struct, avro)

            record.get("ts") shouldBe 1609459200000000L
        }

        @Test
        fun `converts Debezium NanoTimestamp ns to us`() {
            val tsSchema =
                SchemaBuilder
                    .int64()
                    .name("io.debezium.time.NanoTimestamp")
                    .build()
            val schema = SchemaBuilder.struct().field("ts", tsSchema).build()
            val struct = Struct(schema).put("ts", 1234567890123456789L)
            val avro = ConnectToAvroConverter.toAvroSchema(schema)

            val record = ConnectToAvroConverter.toGenericRecord(struct, avro)

            record.get("ts") shouldBe 1234567890123456L
        }

        @Test
        fun `converts Debezium Date days since epoch unchanged`() {
            val dateSchema =
                SchemaBuilder
                    .int32()
                    .name("io.debezium.time.Date")
                    .build()
            val schema = SchemaBuilder.struct().field("d", dateSchema).build()
            val struct = Struct(schema).put("d", 19000)
            val avro = ConnectToAvroConverter.toAvroSchema(schema)

            val record = ConnectToAvroConverter.toGenericRecord(struct, avro)

            record.get("d") shouldBe 19000
        }

        @Test
        fun `converts Debezium ZonedTimestamp as string`() {
            val tsSchema =
                SchemaBuilder
                    .string()
                    .name("io.debezium.time.ZonedTimestamp")
                    .build()
            val schema = SchemaBuilder.struct().field("ts", tsSchema).build()
            val struct = Struct(schema).put("ts", "2021-01-01T00:00:00Z")
            val avro = ConnectToAvroConverter.toAvroSchema(schema)

            val record = ConnectToAvroConverter.toGenericRecord(struct, avro)

            record.get("ts").toString() shouldBe "2021-01-01T00:00:00Z"
        }

        @Test
        fun `converts byte array to ByteBuffer`() {
            val schema =
                SchemaBuilder
                    .struct()
                    .field("data", Schema.BYTES_SCHEMA)
                    .build()
            val data = byteArrayOf(0x01, 0x02, 0x03)
            val struct = Struct(schema).put("data", data)
            val avro = ConnectToAvroConverter.toAvroSchema(schema)

            val record = ConnectToAvroConverter.toGenericRecord(struct, avro)

            val result = record.get("data") as ByteBuffer
            val arr = ByteArray(result.remaining())
            result.get(arr)
            arr shouldBe data
        }

        @Test
        fun `converts Debezium VariableScaleDecimal to string`() {
            val vsdSchema =
                SchemaBuilder
                    .struct()
                    .name("io.debezium.data.VariableScaleDecimal")
                    .field("scale", Schema.INT32_SCHEMA)
                    .field("value", Schema.BYTES_SCHEMA)
                    .build()
            val schema =
                SchemaBuilder
                    .struct()
                    .field("amount", vsdSchema)
                    .build()

            val vsd =
                Struct(vsdSchema)
                    .put("scale", 3)
                    .put(
                        "value",
                        java.math.BigInteger
                            .valueOf(12345L)
                            .toByteArray(),
                    )
            val struct = Struct(schema).put("amount", vsd)
            val avro = ConnectToAvroConverter.toAvroSchema(schema)

            val record = ConnectToAvroConverter.toGenericRecord(struct, avro)

            record.get("amount").toString() shouldBe "12.345"
        }

        @Test
        fun `skips fields not in Avro schema`() {
            val fullSchema =
                SchemaBuilder
                    .struct()
                    .field("id", Schema.INT32_SCHEMA)
                    .field("__deleted", Schema.OPTIONAL_STRING_SCHEMA)
                    .build()
            val filteredConnect =
                SchemaBuilder
                    .struct()
                    .field("id", Schema.INT32_SCHEMA)
                    .build()
            val avro = ConnectToAvroConverter.toAvroSchema(filteredConnect)

            val struct = Struct(fullSchema).put("id", 42).put("__deleted", "false")
            val record = ConnectToAvroConverter.toGenericRecord(struct, avro)

            record.get("id") shouldBe 42
            record.schema.getField("__deleted") shouldBe null
        }
    }
}
