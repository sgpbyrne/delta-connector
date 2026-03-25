package com.deltaconnect.protocol.schema

import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class DeltaSchemaTest {
    @Nested
    inner class PrimitiveTypes {
        @Test
        fun `parses all primitive types`() {
            val json = """{"type":"struct","fields":[
                {"name":"s","type":"string","nullable":true,"metadata":{}},
                {"name":"l","type":"long","nullable":false,"metadata":{}},
                {"name":"i","type":"integer","nullable":false,"metadata":{}},
                {"name":"sh","type":"short","nullable":false,"metadata":{}},
                {"name":"b","type":"byte","nullable":false,"metadata":{}},
                {"name":"f","type":"float","nullable":false,"metadata":{}},
                {"name":"d","type":"double","nullable":false,"metadata":{}},
                {"name":"bool","type":"boolean","nullable":false,"metadata":{}},
                {"name":"bin","type":"binary","nullable":false,"metadata":{}},
                {"name":"dt","type":"date","nullable":false,"metadata":{}},
                {"name":"ts","type":"timestamp","nullable":false,"metadata":{}}
            ]}"""

            val struct = DeltaSchema.fromJson(json)
            struct.fields.size shouldBe 11
            struct.fields[0].type shouldBe DeltaType.StringType
            struct.fields[1].type shouldBe DeltaType.LongType
            struct.fields[2].type shouldBe DeltaType.IntegerType
            struct.fields[3].type shouldBe DeltaType.ShortType
            struct.fields[4].type shouldBe DeltaType.ByteType
            struct.fields[5].type shouldBe DeltaType.FloatType
            struct.fields[6].type shouldBe DeltaType.DoubleType
            struct.fields[7].type shouldBe DeltaType.BooleanType
            struct.fields[8].type shouldBe DeltaType.BinaryType
            struct.fields[9].type shouldBe DeltaType.DateType
            struct.fields[10].type shouldBe DeltaType.TimestampType
        }

        @Test
        fun `parses decimal type`() {
            val json = """{"type":"struct","fields":[
                {"name":"price","type":"decimal(19,4)","nullable":true,"metadata":{}}
            ]}"""

            val struct = DeltaSchema.fromJson(json)
            val decimalType = struct.fields[0].type.shouldBeInstanceOf<DeltaType.DecimalType>()
            decimalType.precision shouldBe 19
            decimalType.scale shouldBe 4
        }
    }

    @Nested
    inner class ComplexTypes {
        @Test
        fun `parses array type`() {
            val json = """{"type":"struct","fields":[
                {"name":"tags","type":{"type":"array","elementType":"string","containsNull":false},"nullable":true,"metadata":{}}
            ]}"""

            val struct = DeltaSchema.fromJson(json)
            val arrayType = struct.fields[0].type.shouldBeInstanceOf<DeltaType.ArrayType>()
            arrayType.elementType shouldBe DeltaType.StringType
            arrayType.containsNull shouldBe false
        }

        @Test
        fun `parses map type`() {
            val json = """{"type":"struct","fields":[
                {"name":"props","type":{"type":"map","keyType":"string","valueType":"integer","valueContainsNull":true},"nullable":true,"metadata":{}}
            ]}"""

            val struct = DeltaSchema.fromJson(json)
            val mapType = struct.fields[0].type.shouldBeInstanceOf<DeltaType.MapType>()
            mapType.keyType shouldBe DeltaType.StringType
            mapType.valueType shouldBe DeltaType.IntegerType
            mapType.valueContainsNull shouldBe true
        }

        @Test
        fun `parses nested struct`() {
            val json = """{"type":"struct","fields":[
                {"name":"address","type":{"type":"struct","fields":[
                    {"name":"city","type":"string","nullable":true,"metadata":{}},
                    {"name":"zip","type":"integer","nullable":false,"metadata":{}}
                ]},"nullable":true,"metadata":{}}
            ]}"""

            val struct = DeltaSchema.fromJson(json)
            val nested = struct.fields[0].type.shouldBeInstanceOf<DeltaType.StructType>()
            nested.fields.size shouldBe 2
            nested.fields[0].name shouldBe "city"
            nested.fields[1].name shouldBe "zip"
        }

        @Test
        fun `parses array of structs`() {
            val json = """{"type":"struct","fields":[
                {"name":"items","type":{"type":"array","elementType":{"type":"struct","fields":[
                    {"name":"name","type":"string","nullable":false,"metadata":{}}
                ]},"containsNull":true},"nullable":true,"metadata":{}}
            ]}"""

            val struct = DeltaSchema.fromJson(json)
            val arrayType = struct.fields[0].type.shouldBeInstanceOf<DeltaType.ArrayType>()
            val elementStruct = arrayType.elementType.shouldBeInstanceOf<DeltaType.StructType>()
            elementStruct.fields[0].name shouldBe "name"
        }
    }

    @Nested
    inner class RoundTrip {
        @Test
        fun `simple schema round-trips`() {
            val original =
                DeltaType.StructType(
                    listOf(
                        StructField("id", DeltaType.IntegerType, nullable = false),
                        StructField("name", DeltaType.StringType, nullable = true),
                        StructField("score", DeltaType.DoubleType, nullable = true),
                    ),
                )

            val json = DeltaSchema.toJson(original)
            val parsed = DeltaSchema.fromJson(json)
            parsed shouldBe original
        }

        @Test
        fun `complex schema round-trips`() {
            val original =
                DeltaType.StructType(
                    listOf(
                        StructField("id", DeltaType.LongType, nullable = false),
                        StructField("amount", DeltaType.DecimalType(19, 4), nullable = true),
                        StructField("tags", DeltaType.ArrayType(DeltaType.StringType, containsNull = false)),
                        StructField(
                            "props",
                            DeltaType.MapType(DeltaType.StringType, DeltaType.IntegerType, valueContainsNull = true),
                        ),
                        StructField(
                            "address",
                            DeltaType.StructType(
                                listOf(
                                    StructField("city", DeltaType.StringType),
                                    StructField("zip", DeltaType.IntegerType, nullable = false),
                                ),
                            ),
                        ),
                    ),
                )

            val json = DeltaSchema.toJson(original)
            val parsed = DeltaSchema.fromJson(json)
            parsed shouldBe original
        }

        @Test
        fun `empty struct round-trips`() {
            val original = DeltaType.StructType(emptyList())
            val json = DeltaSchema.toJson(original)
            val parsed = DeltaSchema.fromJson(json)
            parsed shouldBe original
        }
    }

    @Nested
    inner class FieldMetadata {
        @Test
        fun `preserves nullable flag`() {
            val json = """{"type":"struct","fields":[
                {"name":"required","type":"integer","nullable":false,"metadata":{}},
                {"name":"optional","type":"integer","nullable":true,"metadata":{}}
            ]}"""

            val struct = DeltaSchema.fromJson(json)
            struct.fields[0].nullable shouldBe false
            struct.fields[1].nullable shouldBe true
        }
    }

    @Nested
    inner class Errors {
        @Test
        fun `rejects unknown type`() {
            val json = """{"type":"struct","fields":[
                {"name":"x","type":"foobar","nullable":true,"metadata":{}}
            ]}"""

            assertThrows<IllegalArgumentException> {
                DeltaSchema.fromJson(json)
            }
        }

        @Test
        fun `rejects non-struct top level`() {
            val json = """"string""""

            assertThrows<IllegalArgumentException> {
                DeltaSchema.fromJson(json)
            }
        }
    }
}
