package com.deltaconnect.protocol.parquet

import com.deltaconnect.protocol.schema.AvroToDeltaConverter
import com.deltaconnect.protocol.schema.DeltaType
import com.deltaconnect.protocol.schema.StructField
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import org.apache.avro.generic.GenericRecordBuilder
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.nio.ByteBuffer

class StatsCollectorTest {

    private val objectMapper = jacksonObjectMapper()

    @Nested
    inner class NumRecords {

        @Test
        fun `zero records`() {
            val schema = DeltaType.StructType(
                listOf(StructField("id", DeltaType.IntegerType, nullable = false))
            )
            val collector = StatsCollector(schema)

            val stats = parseStats(collector.toJson())
            stats.numRecords shouldBe 0
        }

        @Test
        fun `counts records correctly`() {
            val schema = DeltaType.StructType(
                listOf(StructField("id", DeltaType.IntegerType, nullable = false))
            )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)
            val collector = StatsCollector(schema)

            repeat(5) { i ->
                val record = GenericRecordBuilder(avroSchema).set("id", i).build()
                collector.update(record)
            }

            val stats = parseStats(collector.toJson())
            stats.numRecords shouldBe 5
        }
    }

    @Nested
    inner class IntegerStats {

        @Test
        fun `tracks min max null count for integers`() {
            val schema = DeltaType.StructType(
                listOf(StructField("value", DeltaType.IntegerType, nullable = true))
            )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)
            val collector = StatsCollector(schema)

            listOf(10, null, 5, 20, null, 15).forEach { v ->
                val record = GenericRecordBuilder(avroSchema).set("value", v).build()
                collector.update(record)
            }

            val stats = parseStats(collector.toJson())
            stats.numRecords shouldBe 6
            stats.minValues["value"] shouldBe 5
            stats.maxValues["value"] shouldBe 20
            stats.nullCount["value"] shouldBe 2L
        }

        @Test
        fun `single value gives same min and max`() {
            val schema = DeltaType.StructType(
                listOf(StructField("v", DeltaType.IntegerType, nullable = false))
            )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)
            val collector = StatsCollector(schema)

            collector.update(GenericRecordBuilder(avroSchema).set("v", 42).build())

            val stats = parseStats(collector.toJson())
            stats.minValues["v"] shouldBe 42
            stats.maxValues["v"] shouldBe 42
        }
    }

    @Nested
    inner class LongStats {

        @Test
        fun `handles Long MIN_VALUE and Long MAX_VALUE`() {
            val schema = DeltaType.StructType(
                listOf(StructField("big", DeltaType.LongType, nullable = false))
            )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)
            val collector = StatsCollector(schema)

            collector.update(GenericRecordBuilder(avroSchema).set("big", Long.MAX_VALUE).build())
            collector.update(GenericRecordBuilder(avroSchema).set("big", Long.MIN_VALUE).build())

            val stats = parseStats(collector.toJson())
            stats.minValues["big"] shouldBe Long.MIN_VALUE
            stats.maxValues["big"] shouldBe Long.MAX_VALUE
        }
    }

    @Nested
    inner class StringStats {

        @Test
        fun `tracks string min max by lexicographic order`() {
            val schema = DeltaType.StructType(
                listOf(StructField("name", DeltaType.StringType, nullable = false))
            )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)
            val collector = StatsCollector(schema)

            listOf("Charlie", "Alice", "Bob").forEach { name ->
                collector.update(GenericRecordBuilder(avroSchema).set("name", name).build())
            }

            val stats = parseStats(collector.toJson())
            stats.minValues["name"] shouldBe "Alice"
            stats.maxValues["name"] shouldBe "Charlie"
        }

        @Test
        fun `min truncates strings longer than 32 chars with plain prefix`() {
            val schema = DeltaType.StructType(
                listOf(StructField("text", DeltaType.StringType, nullable = false))
            )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)
            val collector = StatsCollector(schema)

            val longStr = "a".repeat(100)
            collector.update(GenericRecordBuilder(avroSchema).set("text", longStr).build())

            val stats = parseStats(collector.toJson())
            val min = stats.minValues["text"] as String
            min shouldBe "a".repeat(32)
        }

        @Test
        fun `max truncates and increments last char for valid upper bound`() {
            val schema = DeltaType.StructType(
                listOf(StructField("text", DeltaType.StringType, nullable = false))
            )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)
            val collector = StatsCollector(schema)

            val longStr = "x".repeat(100)
            collector.update(GenericRecordBuilder(avroSchema).set("text", longStr).build())

            val stats = parseStats(collector.toJson())
            val max = stats.maxValues["text"] as String
            max.length shouldBe 32
            max shouldBe "x".repeat(31) + "y"
        }

        @Test
        fun `max truncation handles overflow by trimming trailing max-value chars`() {
            val schema = DeltaType.StructType(
                listOf(StructField("text", DeltaType.StringType, nullable = false))
            )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)
            val collector = StatsCollector(schema)

            val prefix = "abc"
            val longStr = prefix + Char.MAX_VALUE.toString().repeat(100)
            collector.update(GenericRecordBuilder(avroSchema).set("text", longStr).build())

            val stats = parseStats(collector.toJson())
            val max = stats.maxValues["text"] as String
            max shouldBe "abd"
        }

        @Test
        fun `does not truncate strings at or below 32 chars`() {
            val schema = DeltaType.StructType(
                listOf(StructField("text", DeltaType.StringType, nullable = false))
            )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)
            val collector = StatsCollector(schema)

            val exactly32 = "a".repeat(32)
            collector.update(GenericRecordBuilder(avroSchema).set("text", exactly32).build())

            val stats = parseStats(collector.toJson())
            stats.minValues["text"] shouldBe exactly32
            stats.maxValues["text"] shouldBe exactly32
        }

        @Test
        fun `max stat omitted when all chars are max value and string is long`() {
            val schema = DeltaType.StructType(
                listOf(StructField("text", DeltaType.StringType, nullable = false))
            )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)
            val collector = StatsCollector(schema)

            val allMax = Char.MAX_VALUE.toString().repeat(100)
            collector.update(GenericRecordBuilder(avroSchema).set("text", allMax).build())

            val stats = parseStats(collector.toJson())
            val min = stats.minValues["text"] as String
            min.length shouldBe 32
            stats.maxValues.containsKey("text") shouldBe false
        }
    }

    @Nested
    inner class DateAndTimestampStats {

        @Test
        fun `date stats serialized as ISO date strings`() {
            val schema = DeltaType.StructType(
                listOf(StructField("d", DeltaType.DateType, nullable = false))
            )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)
            val collector = StatsCollector(schema)

            // 2024-01-15 = epoch day 19737, 2024-06-01 = epoch day 19875
            collector.update(GenericRecordBuilder(avroSchema).set("d", 19875).build())
            collector.update(GenericRecordBuilder(avroSchema).set("d", 19737).build())

            val stats = parseStats(collector.toJson())
            stats.minValues["d"] shouldBe "2024-01-15"
            stats.maxValues["d"] shouldBe "2024-06-01"
        }

        @Test
        fun `timestamp stats serialized as ISO 8601 UTC`() {
            val schema = DeltaType.StructType(
                listOf(StructField("ts", DeltaType.TimestampType, nullable = false))
            )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)
            val collector = StatsCollector(schema)

            // 2024-01-15T12:00:00Z in micros
            val micros = 1705320000000000L
            collector.update(GenericRecordBuilder(avroSchema).set("ts", micros).build())

            val stats = parseStats(collector.toJson())
            val tsStr = stats.minValues["ts"] as String
            tsStr shouldContain "2024-01-15"
            tsStr shouldContain "12:00"
            tsStr shouldContain "Z"
        }

        @Test
        fun `negative timestamp (pre-epoch) serialized correctly`() {
            val schema = DeltaType.StructType(
                listOf(StructField("ts", DeltaType.TimestampType, nullable = false))
            )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)
            val collector = StatsCollector(schema)

            // 1969-12-31T23:59:59.5Z = -0.5 seconds = -500_000 micros
            val micros = -500_000L
            collector.update(GenericRecordBuilder(avroSchema).set("ts", micros).build())

            val stats = parseStats(collector.toJson())
            val tsStr = stats.minValues["ts"] as String
            tsStr shouldContain "1969-12-31"
            tsStr shouldContain "23:59:59"
            tsStr shouldContain "Z"
        }
    }

    @Nested
    inner class DecimalStats {

        @Test
        fun `decimal stats serialized as plain strings`() {
            val schema = DeltaType.StructType(
                listOf(StructField("amount", DeltaType.DecimalType(10, 2), nullable = false))
            )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)
            val collector = StatsCollector(schema)

            val dec1 = BigDecimal("10.50")
            val dec2 = BigDecimal("99.99")
            collector.update(
                GenericRecordBuilder(avroSchema)
                    .set("amount", ByteBuffer.wrap(dec2.unscaledValue().toByteArray()))
                    .build()
            )
            collector.update(
                GenericRecordBuilder(avroSchema)
                    .set("amount", ByteBuffer.wrap(dec1.unscaledValue().toByteArray()))
                    .build()
            )

            val stats = parseStats(collector.toJson())
            stats.minValues["amount"] shouldBe "10.50"
            stats.maxValues["amount"] shouldBe "99.99"
        }

        @Test
        fun `negative decimal values ordered correctly`() {
            val schema = DeltaType.StructType(
                listOf(StructField("amount", DeltaType.DecimalType(10, 2), nullable = false))
            )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)
            val collector = StatsCollector(schema)

            val neg = BigDecimal("-25.50")
            val pos = BigDecimal("10.00")
            collector.update(
                GenericRecordBuilder(avroSchema)
                    .set("amount", ByteBuffer.wrap(pos.unscaledValue().toByteArray()))
                    .build()
            )
            collector.update(
                GenericRecordBuilder(avroSchema)
                    .set("amount", ByteBuffer.wrap(neg.unscaledValue().toByteArray()))
                    .build()
            )

            val stats = parseStats(collector.toJson())
            stats.minValues["amount"] shouldBe "-25.50"
            stats.maxValues["amount"] shouldBe "10.00"
        }
    }

    @Nested
    inner class BooleanStats {

        @Test
        fun `boolean min is false, max is true`() {
            val schema = DeltaType.StructType(
                listOf(StructField("flag", DeltaType.BooleanType, nullable = false))
            )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)
            val collector = StatsCollector(schema)

            collector.update(GenericRecordBuilder(avroSchema).set("flag", true).build())
            collector.update(GenericRecordBuilder(avroSchema).set("flag", false).build())

            val stats = parseStats(collector.toJson())
            stats.minValues["flag"] shouldBe false
            stats.maxValues["flag"] shouldBe true
        }
    }

    @Nested
    inner class ComplexTypeExclusion {

        @Test
        fun `binary column excluded from min max`() {
            val schema = DeltaType.StructType(
                listOf(
                    StructField("id", DeltaType.IntegerType, nullable = false),
                    StructField("data", DeltaType.BinaryType, nullable = true)
                )
            )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)
            val collector = StatsCollector(schema)

            collector.update(
                GenericRecordBuilder(avroSchema)
                    .set("id", 1)
                    .set("data", ByteBuffer.wrap(byteArrayOf(1, 2, 3)))
                    .build()
            )

            val stats = parseStats(collector.toJson())
            stats.minValues.containsKey("id") shouldBe true
            stats.minValues.containsKey("data") shouldBe false
            stats.maxValues.containsKey("data") shouldBe false
        }

        @Test
        fun `array and map columns excluded from stats entirely`() {
            val schema = DeltaType.StructType(
                listOf(
                    StructField("id", DeltaType.IntegerType, nullable = false),
                    StructField("tags", DeltaType.ArrayType(DeltaType.StringType, containsNull = false)),
                    StructField("props", DeltaType.MapType(
                        DeltaType.StringType, DeltaType.StringType, valueContainsNull = true
                    ))
                )
            )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)
            val collector = StatsCollector(schema)

            collector.update(
                GenericRecordBuilder(avroSchema)
                    .set("id", 1)
                    .set("tags", listOf("a"))
                    .set("props", mapOf("k" to "v"))
                    .build()
            )

            val stats = parseStats(collector.toJson())
            stats.nullCount.containsKey("tags") shouldBe false
            stats.nullCount.containsKey("props") shouldBe false
        }
    }

    @Nested
    inner class MaxColumnsLimit {

        @Test
        fun `limits stats to first 32 columns`() {
            val fields = (1..40).map { i ->
                StructField("col_$i", DeltaType.IntegerType, nullable = false)
            }
            val schema = DeltaType.StructType(fields)
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)
            val collector = StatsCollector(schema)

            val builder = GenericRecordBuilder(avroSchema)
            for (i in 1..40) builder.set("col_$i", i)
            collector.update(builder.build())

            val stats = parseStats(collector.toJson())
            stats.nullCount.size shouldBe 32
            stats.nullCount.containsKey("col_32") shouldBe true
            stats.nullCount.containsKey("col_33") shouldBe false
        }
    }

    @Nested
    inner class AllNullColumn {

        @Test
        fun `all-null column has null count but no min max`() {
            val schema = DeltaType.StructType(
                listOf(StructField("value", DeltaType.IntegerType, nullable = true))
            )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)
            val collector = StatsCollector(schema)

            repeat(3) {
                collector.update(GenericRecordBuilder(avroSchema).set("value", null).build())
            }

            val stats = parseStats(collector.toJson())
            stats.nullCount["value"] shouldBe 3L
            stats.minValues.containsKey("value") shouldBe false
            stats.maxValues.containsKey("value") shouldBe false
        }
    }

    @Nested
    inner class MultipleColumns {

        @Test
        fun `tracks independent stats per column`() {
            val schema = DeltaType.StructType(
                listOf(
                    StructField("a", DeltaType.IntegerType, nullable = true),
                    StructField("b", DeltaType.StringType, nullable = true)
                )
            )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)
            val collector = StatsCollector(schema)

            collector.update(GenericRecordBuilder(avroSchema).set("a", 10).set("b", "x").build())
            collector.update(GenericRecordBuilder(avroSchema).set("a", null).set("b", "y").build())
            collector.update(GenericRecordBuilder(avroSchema).set("a", 5).set("b", null).build())

            val stats = parseStats(collector.toJson())
            stats.numRecords shouldBe 3
            stats.minValues["a"] shouldBe 5
            stats.maxValues["a"] shouldBe 10
            stats.nullCount["a"] shouldBe 1L
            stats.minValues["b"] shouldBe "x"
            stats.maxValues["b"] shouldBe "y"
            stats.nullCount["b"] shouldBe 1L
        }
    }

    @Nested
    inner class NestedStructStats {

        @Test
        fun `collects stats on nested struct leaf columns with dot paths`() {
            val schema = DeltaType.StructType(
                listOf(
                    StructField("id", DeltaType.IntegerType, nullable = false),
                    StructField("address", DeltaType.StructType(
                        listOf(
                            StructField("city", DeltaType.StringType, nullable = true),
                            StructField("zip", DeltaType.IntegerType, nullable = true)
                        )
                    ))
                )
            )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)
            val addressSchema = avroSchema.getField("address").schema()
                .types.first { it.type != org.apache.avro.Schema.Type.NULL }
            val collector = StatsCollector(schema)

            val addr1 = GenericRecordBuilder(addressSchema)
                .set("city", "Austin")
                .set("zip", 78701)
                .build()
            val addr2 = GenericRecordBuilder(addressSchema)
                .set("city", "Denver")
                .set("zip", 80202)
                .build()

            collector.update(
                GenericRecordBuilder(avroSchema).set("id", 1).set("address", addr1).build()
            )
            collector.update(
                GenericRecordBuilder(avroSchema).set("id", 2).set("address", addr2).build()
            )

            val json = collector.toJson()
            val raw: Map<String, Any> = objectMapper.readValue(json)

            (raw["numRecords"] as Number).toLong() shouldBe 2

            @Suppress("UNCHECKED_CAST")
            val minValues = raw["minValues"] as Map<String, Any>
            minValues["id"] shouldBe 1
            @Suppress("UNCHECKED_CAST")
            val minAddr = minValues["address"] as Map<String, Any>
            minAddr["city"] shouldBe "Austin"
            minAddr["zip"] shouldBe 78701

            @Suppress("UNCHECKED_CAST")
            val maxValues = raw["maxValues"] as Map<String, Any>
            maxValues["id"] shouldBe 2
            @Suppress("UNCHECKED_CAST")
            val maxAddr = maxValues["address"] as Map<String, Any>
            maxAddr["city"] shouldBe "Denver"
            maxAddr["zip"] shouldBe 80202
        }

        @Test
        fun `handles null nested struct`() {
            val schema = DeltaType.StructType(
                listOf(
                    StructField("id", DeltaType.IntegerType, nullable = false),
                    StructField("address", DeltaType.StructType(
                        listOf(
                            StructField("city", DeltaType.StringType, nullable = true)
                        )
                    ), nullable = true)
                )
            )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)
            val collector = StatsCollector(schema)

            collector.update(
                GenericRecordBuilder(avroSchema).set("id", 1).set("address", null).build()
            )

            val json = collector.toJson()
            val raw: Map<String, Any> = objectMapper.readValue(json)

            @Suppress("UNCHECKED_CAST")
            val nullCount = raw["nullCount"] as Map<String, Any>
            nullCount["id"] shouldBe 0
            @Suppress("UNCHECKED_CAST")
            val nullAddr = nullCount["address"] as Map<String, Any>
            nullAddr["city"] shouldBe 1
        }
    }

    @Nested
    inner class JsonOutput {

        @Test
        fun `produces valid JSON`() {
            val schema = DeltaType.StructType(
                listOf(StructField("id", DeltaType.IntegerType, nullable = false))
            )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)
            val collector = StatsCollector(schema)

            collector.update(GenericRecordBuilder(avroSchema).set("id", 1).build())

            val json = collector.toJson()
            val map: Map<String, Any> = objectMapper.readValue(json)
            map.containsKey("numRecords") shouldBe true
            map.containsKey("minValues") shouldBe true
            map.containsKey("maxValues") shouldBe true
            map.containsKey("nullCount") shouldBe true
        }
    }

    private fun parseStats(json: String): ParsedStats = ParsedStats.fromJson(json)
}
