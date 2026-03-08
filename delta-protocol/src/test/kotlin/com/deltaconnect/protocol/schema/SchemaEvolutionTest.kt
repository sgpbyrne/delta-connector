package com.deltaconnect.protocol.schema

import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.types.shouldBeInstanceOf
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

class SchemaEvolutionTest {

    @Nested
    inner class NoChange {

        @Test
        fun `identical schemas produce NoChange`() {
            val schema = DeltaType.StructType(listOf(
                StructField("id", DeltaType.IntegerType, nullable = false),
                StructField("name", DeltaType.StringType, nullable = true)
            ))

            val result = SchemaEvolution.merge(schema, schema)
            val noChange = result.shouldBeInstanceOf<SchemaEvolution.MergeResult.NoChange>()
            noChange.schema shouldBe schema
        }

        @Test
        fun `subset of columns produces NoChange`() {
            val existing = DeltaType.StructType(listOf(
                StructField("id", DeltaType.IntegerType, nullable = false),
                StructField("name", DeltaType.StringType, nullable = true),
                StructField("age", DeltaType.IntegerType, nullable = true)
            ))
            val incoming = DeltaType.StructType(listOf(
                StructField("id", DeltaType.IntegerType, nullable = false),
                StructField("name", DeltaType.StringType, nullable = true)
            ))

            val result = SchemaEvolution.merge(existing, incoming)
            val noChange = result.shouldBeInstanceOf<SchemaEvolution.MergeResult.NoChange>()
            noChange.schema.fields.size shouldBe 3
        }
    }

    @Nested
    inner class AddColumn {

        @Test
        fun `adds new nullable column`() {
            val existing = DeltaType.StructType(listOf(
                StructField("id", DeltaType.IntegerType, nullable = false)
            ))
            val incoming = DeltaType.StructType(listOf(
                StructField("id", DeltaType.IntegerType, nullable = false),
                StructField("email", DeltaType.StringType, nullable = true)
            ))

            val result = SchemaEvolution.merge(existing, incoming)
            val evolved = result.shouldBeInstanceOf<SchemaEvolution.MergeResult.Evolved>()
            evolved.schema.fields.size shouldBe 2
            evolved.schema.fields[1].name shouldBe "email"
            evolved.schema.fields[1].nullable shouldBe true
        }

        @Test
        fun `adds multiple new columns`() {
            val existing = DeltaType.StructType(listOf(
                StructField("id", DeltaType.IntegerType, nullable = false)
            ))
            val incoming = DeltaType.StructType(listOf(
                StructField("id", DeltaType.IntegerType, nullable = false),
                StructField("email", DeltaType.StringType, nullable = true),
                StructField("phone", DeltaType.StringType, nullable = true)
            ))

            val result = SchemaEvolution.merge(existing, incoming)
            val evolved = result.shouldBeInstanceOf<SchemaEvolution.MergeResult.Evolved>()
            evolved.schema.fields.size shouldBe 3
        }

        @Test
        fun `rejects new non-nullable column`() {
            val existing = DeltaType.StructType(listOf(
                StructField("id", DeltaType.IntegerType, nullable = false)
            ))
            val incoming = DeltaType.StructType(listOf(
                StructField("id", DeltaType.IntegerType, nullable = false),
                StructField("required_col", DeltaType.StringType, nullable = false)
            ))

            val result = SchemaEvolution.merge(existing, incoming)
            val incompatible = result.shouldBeInstanceOf<SchemaEvolution.MergeResult.Incompatible>()
            incompatible.reason shouldContain "required_col"
            incompatible.reason shouldContain "nullable"
        }
    }

    @Nested
    inner class TypeWidening {

        @Test
        fun `widens byte to short`() {
            val existing = DeltaType.StructType(listOf(
                StructField("x", DeltaType.ByteType, nullable = false)
            ))
            val incoming = DeltaType.StructType(listOf(
                StructField("x", DeltaType.ShortType, nullable = false)
            ))

            val result = SchemaEvolution.merge(existing, incoming)
            val evolved = result.shouldBeInstanceOf<SchemaEvolution.MergeResult.Evolved>()
            evolved.schema.fields[0].type shouldBe DeltaType.ShortType
        }

        @Test
        fun `widens byte to integer`() {
            val existing = DeltaType.StructType(listOf(
                StructField("x", DeltaType.ByteType, nullable = false)
            ))
            val incoming = DeltaType.StructType(listOf(
                StructField("x", DeltaType.IntegerType, nullable = false)
            ))

            val result = SchemaEvolution.merge(existing, incoming)
            val evolved = result.shouldBeInstanceOf<SchemaEvolution.MergeResult.Evolved>()
            evolved.schema.fields[0].type shouldBe DeltaType.IntegerType
        }

        @Test
        fun `widens byte to long`() {
            val existing = DeltaType.StructType(listOf(
                StructField("x", DeltaType.ByteType, nullable = false)
            ))
            val incoming = DeltaType.StructType(listOf(
                StructField("x", DeltaType.LongType, nullable = false)
            ))

            val result = SchemaEvolution.merge(existing, incoming)
            val evolved = result.shouldBeInstanceOf<SchemaEvolution.MergeResult.Evolved>()
            evolved.schema.fields[0].type shouldBe DeltaType.LongType
        }

        @Test
        fun `widens short to integer`() {
            val existing = DeltaType.StructType(listOf(
                StructField("x", DeltaType.ShortType, nullable = false)
            ))
            val incoming = DeltaType.StructType(listOf(
                StructField("x", DeltaType.IntegerType, nullable = false)
            ))

            val result = SchemaEvolution.merge(existing, incoming)
            val evolved = result.shouldBeInstanceOf<SchemaEvolution.MergeResult.Evolved>()
            evolved.schema.fields[0].type shouldBe DeltaType.IntegerType
        }

        @Test
        fun `widens short to long`() {
            val existing = DeltaType.StructType(listOf(
                StructField("x", DeltaType.ShortType, nullable = false)
            ))
            val incoming = DeltaType.StructType(listOf(
                StructField("x", DeltaType.LongType, nullable = false)
            ))

            val result = SchemaEvolution.merge(existing, incoming)
            val evolved = result.shouldBeInstanceOf<SchemaEvolution.MergeResult.Evolved>()
            evolved.schema.fields[0].type shouldBe DeltaType.LongType
        }

        @Test
        fun `widens integer to long`() {
            val existing = DeltaType.StructType(listOf(
                StructField("x", DeltaType.IntegerType, nullable = false)
            ))
            val incoming = DeltaType.StructType(listOf(
                StructField("x", DeltaType.LongType, nullable = false)
            ))

            val result = SchemaEvolution.merge(existing, incoming)
            val evolved = result.shouldBeInstanceOf<SchemaEvolution.MergeResult.Evolved>()
            evolved.schema.fields[0].type shouldBe DeltaType.LongType
        }

        @Test
        fun `widens float to double`() {
            val existing = DeltaType.StructType(listOf(
                StructField("x", DeltaType.FloatType, nullable = false)
            ))
            val incoming = DeltaType.StructType(listOf(
                StructField("x", DeltaType.DoubleType, nullable = false)
            ))

            val result = SchemaEvolution.merge(existing, incoming)
            val evolved = result.shouldBeInstanceOf<SchemaEvolution.MergeResult.Evolved>()
            evolved.schema.fields[0].type shouldBe DeltaType.DoubleType
        }

        @Test
        fun `widens decimal precision`() {
            val existing = DeltaType.StructType(listOf(
                StructField("amount", DeltaType.DecimalType(10, 2), nullable = false)
            ))
            val incoming = DeltaType.StructType(listOf(
                StructField("amount", DeltaType.DecimalType(19, 4), nullable = false)
            ))

            val result = SchemaEvolution.merge(existing, incoming)
            val evolved = result.shouldBeInstanceOf<SchemaEvolution.MergeResult.Evolved>()
            val dec = evolved.schema.fields[0].type.shouldBeInstanceOf<DeltaType.DecimalType>()
            dec.precision shouldBe 19
            dec.scale shouldBe 4
        }

        @Test
        fun `rejects decimal scale reduction`() {
            val existing = DeltaType.StructType(listOf(
                StructField("amount", DeltaType.DecimalType(19, 4), nullable = false)
            ))
            val incoming = DeltaType.StructType(listOf(
                StructField("amount", DeltaType.DecimalType(19, 2), nullable = false)
            ))

            val result = SchemaEvolution.merge(existing, incoming)
            result.shouldBeInstanceOf<SchemaEvolution.MergeResult.Incompatible>()
        }
    }

    @Nested
    inner class NullabilityEvolution {

        @Test
        fun `non-nullable to nullable evolves`() {
            val existing = DeltaType.StructType(listOf(
                StructField("x", DeltaType.IntegerType, nullable = false)
            ))
            val incoming = DeltaType.StructType(listOf(
                StructField("x", DeltaType.IntegerType, nullable = true)
            ))

            val result = SchemaEvolution.merge(existing, incoming)
            val evolved = result.shouldBeInstanceOf<SchemaEvolution.MergeResult.Evolved>()
            evolved.schema.fields[0].nullable shouldBe true
        }

        @Test
        fun `nullable stays nullable when incoming is non-nullable`() {
            val existing = DeltaType.StructType(listOf(
                StructField("x", DeltaType.IntegerType, nullable = true)
            ))
            val incoming = DeltaType.StructType(listOf(
                StructField("x", DeltaType.IntegerType, nullable = false)
            ))

            val result = SchemaEvolution.merge(existing, incoming)
            val noChange = result.shouldBeInstanceOf<SchemaEvolution.MergeResult.NoChange>()
            noChange.schema.fields[0].nullable shouldBe true
        }
    }

    @Nested
    inner class IncompatibleChanges {

        @Test
        fun `rejects string to integer`() {
            val existing = DeltaType.StructType(listOf(
                StructField("x", DeltaType.StringType, nullable = false)
            ))
            val incoming = DeltaType.StructType(listOf(
                StructField("x", DeltaType.IntegerType, nullable = false)
            ))

            val result = SchemaEvolution.merge(existing, incoming)
            val incompatible = result.shouldBeInstanceOf<SchemaEvolution.MergeResult.Incompatible>()
            incompatible.reason shouldContain "x"
            incompatible.reason shouldContain "string"
            incompatible.reason shouldContain "integer"
        }

        @Test
        fun `rejects integer to string`() {
            val existing = DeltaType.StructType(listOf(
                StructField("x", DeltaType.IntegerType, nullable = false)
            ))
            val incoming = DeltaType.StructType(listOf(
                StructField("x", DeltaType.StringType, nullable = false)
            ))

            val result = SchemaEvolution.merge(existing, incoming)
            result.shouldBeInstanceOf<SchemaEvolution.MergeResult.Incompatible>()
        }

        @Test
        fun `rejects narrowing long to integer`() {
            val existing = DeltaType.StructType(listOf(
                StructField("x", DeltaType.LongType, nullable = false)
            ))
            val incoming = DeltaType.StructType(listOf(
                StructField("x", DeltaType.IntegerType, nullable = false)
            ))

            val result = SchemaEvolution.merge(existing, incoming)
            result.shouldBeInstanceOf<SchemaEvolution.MergeResult.Incompatible>()
        }

        @Test
        fun `rejects double to float`() {
            val existing = DeltaType.StructType(listOf(
                StructField("x", DeltaType.DoubleType, nullable = false)
            ))
            val incoming = DeltaType.StructType(listOf(
                StructField("x", DeltaType.FloatType, nullable = false)
            ))

            val result = SchemaEvolution.merge(existing, incoming)
            result.shouldBeInstanceOf<SchemaEvolution.MergeResult.Incompatible>()
        }
    }

    @Nested
    inner class CombinedEvolution {

        @Test
        fun `adds column and widens type simultaneously`() {
            val existing = DeltaType.StructType(listOf(
                StructField("id", DeltaType.IntegerType, nullable = false),
                StructField("name", DeltaType.StringType, nullable = true)
            ))
            val incoming = DeltaType.StructType(listOf(
                StructField("id", DeltaType.LongType, nullable = false),
                StructField("name", DeltaType.StringType, nullable = true),
                StructField("email", DeltaType.StringType, nullable = true)
            ))

            val result = SchemaEvolution.merge(existing, incoming)
            val evolved = result.shouldBeInstanceOf<SchemaEvolution.MergeResult.Evolved>()
            evolved.schema.fields.size shouldBe 3
            evolved.schema.fields[0].type shouldBe DeltaType.LongType
            evolved.schema.fields[2].name shouldBe "email"
        }

        @Test
        fun `preserves existing columns not in incoming`() {
            val existing = DeltaType.StructType(listOf(
                StructField("id", DeltaType.IntegerType, nullable = false),
                StructField("legacy", DeltaType.StringType, nullable = true)
            ))
            val incoming = DeltaType.StructType(listOf(
                StructField("id", DeltaType.IntegerType, nullable = false),
                StructField("new_col", DeltaType.StringType, nullable = true)
            ))

            val result = SchemaEvolution.merge(existing, incoming)
            val evolved = result.shouldBeInstanceOf<SchemaEvolution.MergeResult.Evolved>()
            evolved.schema.fields.size shouldBe 3
            evolved.schema.fields[0].name shouldBe "id"
            evolved.schema.fields[1].name shouldBe "legacy"
            evolved.schema.fields[2].name shouldBe "new_col"
        }

        @Test
        fun `empty existing schema accepts all nullable columns`() {
            val existing = DeltaType.StructType(emptyList())
            val incoming = DeltaType.StructType(listOf(
                StructField("id", DeltaType.IntegerType, nullable = true),
                StructField("name", DeltaType.StringType, nullable = true)
            ))

            val result = SchemaEvolution.merge(existing, incoming)
            val evolved = result.shouldBeInstanceOf<SchemaEvolution.MergeResult.Evolved>()
            evolved.schema.fields.size shouldBe 2
        }
    }
}
