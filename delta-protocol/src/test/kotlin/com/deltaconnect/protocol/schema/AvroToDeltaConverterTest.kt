package com.deltaconnect.protocol.schema

import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema as AvroSchema
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class AvroToDeltaConverterTest {

    @Nested
    inner class DeltaToAvro {

        @Test
        fun `converts primitive types to Avro`() {
            val struct = DeltaType.StructType(listOf(
                StructField("s", DeltaType.StringType, nullable = false),
                StructField("l", DeltaType.LongType, nullable = false),
                StructField("i", DeltaType.IntegerType, nullable = false),
                StructField("f", DeltaType.FloatType, nullable = false),
                StructField("d", DeltaType.DoubleType, nullable = false),
                StructField("b", DeltaType.BooleanType, nullable = false),
                StructField("bin", DeltaType.BinaryType, nullable = false)
            ))

            val avro = AvroToDeltaConverter.toAvroSchema(struct)
            avro.type shouldBe AvroSchema.Type.RECORD
            avro.fields.size shouldBe 7
            avro.getField("s").schema().type shouldBe AvroSchema.Type.STRING
            avro.getField("l").schema().type shouldBe AvroSchema.Type.LONG
            avro.getField("i").schema().type shouldBe AvroSchema.Type.INT
            avro.getField("f").schema().type shouldBe AvroSchema.Type.FLOAT
            avro.getField("d").schema().type shouldBe AvroSchema.Type.DOUBLE
            avro.getField("b").schema().type shouldBe AvroSchema.Type.BOOLEAN
            avro.getField("bin").schema().type shouldBe AvroSchema.Type.BYTES
        }

        @Test
        fun `short and byte map to Avro INT`() {
            val struct = DeltaType.StructType(listOf(
                StructField("sh", DeltaType.ShortType, nullable = false),
                StructField("by", DeltaType.ByteType, nullable = false)
            ))

            val avro = AvroToDeltaConverter.toAvroSchema(struct)
            avro.getField("sh").schema().type shouldBe AvroSchema.Type.INT
            avro.getField("by").schema().type shouldBe AvroSchema.Type.INT
        }

        @Test
        fun `nullable fields become Avro unions`() {
            val struct = DeltaType.StructType(listOf(
                StructField("name", DeltaType.StringType, nullable = true)
            ))

            val avro = AvroToDeltaConverter.toAvroSchema(struct)
            val fieldSchema = avro.getField("name").schema()
            fieldSchema.type shouldBe AvroSchema.Type.UNION
            fieldSchema.types[0].type shouldBe AvroSchema.Type.NULL
            fieldSchema.types[1].type shouldBe AvroSchema.Type.STRING
        }

        @Test
        fun `non-nullable fields are not unions`() {
            val struct = DeltaType.StructType(listOf(
                StructField("id", DeltaType.IntegerType, nullable = false)
            ))

            val avro = AvroToDeltaConverter.toAvroSchema(struct)
            avro.getField("id").schema().type shouldBe AvroSchema.Type.INT
        }

        @Test
        fun `converts date to Avro logical date`() {
            val struct = DeltaType.StructType(listOf(
                StructField("dt", DeltaType.DateType, nullable = false)
            ))

            val avro = AvroToDeltaConverter.toAvroSchema(struct)
            val fieldSchema = avro.getField("dt").schema()
            fieldSchema.type shouldBe AvroSchema.Type.INT
            fieldSchema.logicalType.name shouldBe "date"
        }

        @Test
        fun `converts timestamp to Avro timestamp-micros`() {
            val struct = DeltaType.StructType(listOf(
                StructField("ts", DeltaType.TimestampType, nullable = false)
            ))

            val avro = AvroToDeltaConverter.toAvroSchema(struct)
            val fieldSchema = avro.getField("ts").schema()
            fieldSchema.type shouldBe AvroSchema.Type.LONG
            fieldSchema.logicalType.name shouldBe "timestamp-micros"
        }

        @Test
        fun `converts decimal to Avro logical decimal`() {
            val struct = DeltaType.StructType(listOf(
                StructField("price", DeltaType.DecimalType(19, 4), nullable = false)
            ))

            val avro = AvroToDeltaConverter.toAvroSchema(struct)
            val fieldSchema = avro.getField("price").schema()
            fieldSchema.type shouldBe AvroSchema.Type.BYTES
            val logicalDecimal = fieldSchema.logicalType as LogicalTypes.Decimal
            logicalDecimal.precision shouldBe 19
            logicalDecimal.scale shouldBe 4
        }

        @Test
        fun `converts array type`() {
            val struct = DeltaType.StructType(listOf(
                StructField("tags", DeltaType.ArrayType(DeltaType.StringType, containsNull = false), nullable = false)
            ))

            val avro = AvroToDeltaConverter.toAvroSchema(struct)
            val fieldSchema = avro.getField("tags").schema()
            fieldSchema.type shouldBe AvroSchema.Type.ARRAY
            fieldSchema.elementType.type shouldBe AvroSchema.Type.STRING
        }

        @Test
        fun `converts array with nullable elements`() {
            val struct = DeltaType.StructType(listOf(
                StructField("tags", DeltaType.ArrayType(DeltaType.StringType, containsNull = true), nullable = false)
            ))

            val avro = AvroToDeltaConverter.toAvroSchema(struct)
            val elementSchema = avro.getField("tags").schema().elementType
            elementSchema.type shouldBe AvroSchema.Type.UNION
            elementSchema.types[0].type shouldBe AvroSchema.Type.NULL
            elementSchema.types[1].type shouldBe AvroSchema.Type.STRING
        }

        @Test
        fun `converts map type`() {
            val struct = DeltaType.StructType(listOf(
                StructField("props", DeltaType.MapType(
                    DeltaType.StringType, DeltaType.IntegerType, valueContainsNull = false
                ), nullable = false)
            ))

            val avro = AvroToDeltaConverter.toAvroSchema(struct)
            val fieldSchema = avro.getField("props").schema()
            fieldSchema.type shouldBe AvroSchema.Type.MAP
            fieldSchema.valueType.type shouldBe AvroSchema.Type.INT
        }

        @Test
        fun `converts nested struct`() {
            val struct = DeltaType.StructType(listOf(
                StructField("address", DeltaType.StructType(listOf(
                    StructField("city", DeltaType.StringType, nullable = false),
                    StructField("zip", DeltaType.IntegerType, nullable = false)
                )), nullable = false)
            ))

            val avro = AvroToDeltaConverter.toAvroSchema(struct)
            val nested = avro.getField("address").schema()
            nested.type shouldBe AvroSchema.Type.RECORD
            nested.fields.size shouldBe 2
            nested.getField("city").schema().type shouldBe AvroSchema.Type.STRING
        }

        @Test
        fun `uses custom record name`() {
            val struct = DeltaType.StructType(listOf(
                StructField("id", DeltaType.IntegerType, nullable = false)
            ))

            val avro = AvroToDeltaConverter.toAvroSchema(struct, "MyTable")
            avro.name shouldBe "MyTable"
        }
    }

    @Nested
    inner class AvroToDelta {

        @Test
        fun `converts Avro primitives to Delta types`() {
            val avro = AvroSchema.createRecord("test", null, "ns", false, listOf(
                AvroSchema.Field("s", AvroSchema.create(AvroSchema.Type.STRING)),
                AvroSchema.Field("l", AvroSchema.create(AvroSchema.Type.LONG)),
                AvroSchema.Field("i", AvroSchema.create(AvroSchema.Type.INT)),
                AvroSchema.Field("f", AvroSchema.create(AvroSchema.Type.FLOAT)),
                AvroSchema.Field("d", AvroSchema.create(AvroSchema.Type.DOUBLE)),
                AvroSchema.Field("b", AvroSchema.create(AvroSchema.Type.BOOLEAN)),
                AvroSchema.Field("bin", AvroSchema.create(AvroSchema.Type.BYTES))
            ))

            val delta = AvroToDeltaConverter.toDeltaType(avro)
            delta.fields.size shouldBe 7
            delta.fields[0].type shouldBe DeltaType.StringType
            delta.fields[1].type shouldBe DeltaType.LongType
            delta.fields[2].type shouldBe DeltaType.IntegerType
            delta.fields[3].type shouldBe DeltaType.FloatType
            delta.fields[4].type shouldBe DeltaType.DoubleType
            delta.fields[5].type shouldBe DeltaType.BooleanType
            delta.fields[6].type shouldBe DeltaType.BinaryType
        }

        @Test
        fun `converts Avro union to nullable Delta field`() {
            val nullableString = AvroSchema.createUnion(
                AvroSchema.create(AvroSchema.Type.NULL),
                AvroSchema.create(AvroSchema.Type.STRING)
            )
            val avro = AvroSchema.createRecord("test", null, "ns", false, listOf(
                AvroSchema.Field("name", nullableString, null, AvroSchema.Field.NULL_DEFAULT_VALUE)
            ))

            val delta = AvroToDeltaConverter.toDeltaType(avro)
            delta.fields[0].type shouldBe DeltaType.StringType
            delta.fields[0].nullable shouldBe true
        }

        @Test
        fun `non-union Avro field is not nullable`() {
            val avro = AvroSchema.createRecord("test", null, "ns", false, listOf(
                AvroSchema.Field("id", AvroSchema.create(AvroSchema.Type.INT))
            ))

            val delta = AvroToDeltaConverter.toDeltaType(avro)
            delta.fields[0].nullable shouldBe false
        }

        @Test
        fun `converts Avro logical date`() {
            val dateSchema = LogicalTypes.date().addToSchema(AvroSchema.create(AvroSchema.Type.INT))
            val avro = AvroSchema.createRecord("test", null, "ns", false, listOf(
                AvroSchema.Field("dt", dateSchema)
            ))

            val delta = AvroToDeltaConverter.toDeltaType(avro)
            delta.fields[0].type shouldBe DeltaType.DateType
        }

        @Test
        fun `converts Avro logical timestamp-micros`() {
            val tsSchema = LogicalTypes.timestampMicros().addToSchema(AvroSchema.create(AvroSchema.Type.LONG))
            val avro = AvroSchema.createRecord("test", null, "ns", false, listOf(
                AvroSchema.Field("ts", tsSchema)
            ))

            val delta = AvroToDeltaConverter.toDeltaType(avro)
            delta.fields[0].type shouldBe DeltaType.TimestampType
        }

        @Test
        fun `converts Avro logical timestamp-millis`() {
            val tsSchema = LogicalTypes.timestampMillis().addToSchema(AvroSchema.create(AvroSchema.Type.LONG))
            val avro = AvroSchema.createRecord("test", null, "ns", false, listOf(
                AvroSchema.Field("ts", tsSchema)
            ))

            val delta = AvroToDeltaConverter.toDeltaType(avro)
            delta.fields[0].type shouldBe DeltaType.TimestampType
        }

        @Test
        fun `converts Avro logical decimal`() {
            val decSchema = LogicalTypes.decimal(19, 4).addToSchema(AvroSchema.create(AvroSchema.Type.BYTES))
            val avro = AvroSchema.createRecord("test", null, "ns", false, listOf(
                AvroSchema.Field("price", decSchema)
            ))

            val delta = AvroToDeltaConverter.toDeltaType(avro)
            val dec = delta.fields[0].type.shouldBeInstanceOf<DeltaType.DecimalType>()
            dec.precision shouldBe 19
            dec.scale shouldBe 4
        }

        @Test
        fun `converts Avro array`() {
            val arraySchema = AvroSchema.createArray(AvroSchema.create(AvroSchema.Type.STRING))
            val avro = AvroSchema.createRecord("test", null, "ns", false, listOf(
                AvroSchema.Field("tags", arraySchema)
            ))

            val delta = AvroToDeltaConverter.toDeltaType(avro)
            val arr = delta.fields[0].type.shouldBeInstanceOf<DeltaType.ArrayType>()
            arr.elementType shouldBe DeltaType.StringType
            arr.containsNull shouldBe false
        }

        @Test
        fun `converts Avro array with nullable elements`() {
            val nullableString = AvroSchema.createUnion(
                AvroSchema.create(AvroSchema.Type.NULL),
                AvroSchema.create(AvroSchema.Type.STRING)
            )
            val arraySchema = AvroSchema.createArray(nullableString)
            val avro = AvroSchema.createRecord("test", null, "ns", false, listOf(
                AvroSchema.Field("tags", arraySchema)
            ))

            val delta = AvroToDeltaConverter.toDeltaType(avro)
            val arr = delta.fields[0].type.shouldBeInstanceOf<DeltaType.ArrayType>()
            arr.elementType shouldBe DeltaType.StringType
            arr.containsNull shouldBe true
        }

        @Test
        fun `converts Avro map`() {
            val mapSchema = AvroSchema.createMap(AvroSchema.create(AvroSchema.Type.INT))
            val avro = AvroSchema.createRecord("test", null, "ns", false, listOf(
                AvroSchema.Field("props", mapSchema)
            ))

            val delta = AvroToDeltaConverter.toDeltaType(avro)
            val map = delta.fields[0].type.shouldBeInstanceOf<DeltaType.MapType>()
            map.keyType shouldBe DeltaType.StringType
            map.valueType shouldBe DeltaType.IntegerType
            map.valueContainsNull shouldBe false
        }

        @Test
        fun `converts Avro nested record`() {
            val inner = AvroSchema.createRecord("address", null, "ns", false, listOf(
                AvroSchema.Field("city", AvroSchema.create(AvroSchema.Type.STRING)),
                AvroSchema.Field("zip", AvroSchema.create(AvroSchema.Type.INT))
            ))
            val avro = AvroSchema.createRecord("test", null, "ns", false, listOf(
                AvroSchema.Field("addr", inner)
            ))

            val delta = AvroToDeltaConverter.toDeltaType(avro)
            val nested = delta.fields[0].type.shouldBeInstanceOf<DeltaType.StructType>()
            nested.fields.size shouldBe 2
            nested.fields[0].name shouldBe "city"
            nested.fields[1].name shouldBe "zip"
        }

        @Test
        fun `rejects non-record top-level schema`() {
            assertThrows<IllegalArgumentException> {
                AvroToDeltaConverter.toDeltaType(AvroSchema.create(AvroSchema.Type.STRING))
            }
        }
    }

    @Nested
    inner class RoundTrip {

        @Test
        fun `simple schema round-trips through Avro`() {
            val original = DeltaType.StructType(listOf(
                StructField("id", DeltaType.IntegerType, nullable = false),
                StructField("name", DeltaType.StringType, nullable = true),
                StructField("score", DeltaType.DoubleType, nullable = true)
            ))

            val avro = AvroToDeltaConverter.toAvroSchema(original)
            val roundTripped = AvroToDeltaConverter.toDeltaType(avro)
            roundTripped shouldBe original
        }

        @Test
        fun `complex schema round-trips through Avro`() {
            val original = DeltaType.StructType(listOf(
                StructField("id", DeltaType.LongType, nullable = false),
                StructField("amount", DeltaType.DecimalType(19, 4), nullable = true),
                StructField("created", DeltaType.DateType, nullable = false),
                StructField("updated", DeltaType.TimestampType, nullable = true)
            ))

            val avro = AvroToDeltaConverter.toAvroSchema(original)
            val roundTripped = AvroToDeltaConverter.toDeltaType(avro)
            roundTripped shouldBe original
        }

        @Test
        fun `schema with collections round-trips`() {
            val original = DeltaType.StructType(listOf(
                StructField("tags", DeltaType.ArrayType(DeltaType.StringType, containsNull = false)),
                StructField("props", DeltaType.MapType(
                    DeltaType.StringType, DeltaType.IntegerType, valueContainsNull = true
                ))
            ))

            val avro = AvroToDeltaConverter.toAvroSchema(original)
            val roundTripped = AvroToDeltaConverter.toDeltaType(avro)
            roundTripped shouldBe original
        }
    }
}
