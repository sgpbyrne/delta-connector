package com.deltaconnect.protocol.merge

import com.deltaconnect.protocol.actions.AddFile
import com.deltaconnect.protocol.schema.DeltaType
import com.deltaconnect.protocol.schema.StructField
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.math.BigDecimal

class FilePrunerTest {
    private lateinit var pruner: FilePruner

    private fun addFile(
        path: String,
        stats: String? = null,
        size: Long = 1024L,
    ): AddFile =
        AddFile(
            path = path,
            size = size,
            modificationTime = System.currentTimeMillis(),
            dataChange = true,
            stats = stats,
        )

    private fun intStats(
        column: String,
        min: Int,
        max: Int,
        numRecords: Int = 10,
    ): String =
        """{"numRecords":$numRecords,""" +
            """"minValues":{"$column":$min},""" +
            """"maxValues":{"$column":$max},""" +
            """"nullCount":{"$column":0}}"""

    private fun longStats(
        column: String,
        min: Long,
        max: Long,
    ): String =
        """{"numRecords":10,""" +
            """"minValues":{"$column":$min},""" +
            """"maxValues":{"$column":$max},""" +
            """"nullCount":{"$column":0}}"""

    private fun stringStats(
        column: String,
        min: String,
        max: String,
    ): String =
        """{"numRecords":10,""" +
            """"minValues":{"$column":"$min"},""" +
            """"maxValues":{"$column":"$max"},""" +
            """"nullCount":{"$column":0}}"""

    @Nested
    inner class SingleIntegerKeyPruning {
        @BeforeEach
        fun setUp() {
            pruner =
                FilePruner(
                    listOf(StructField("id", DeltaType.IntegerType, nullable = false)),
                )
        }

        @Test
        fun `prunes file when file max is below source min`() {
            val file = addFile("f1.parquet", intStats("id", 1, 10))
            val bounds = mapOf("id" to KeyBounds(20, 30))

            val result = pruner.prune(setOf(file), bounds)

            result.matchingFiles.shouldBeEmpty()
            result.prunedCount shouldBe 1
        }

        @Test
        fun `prunes file when file min is above source max`() {
            val file = addFile("f1.parquet", intStats("id", 50, 100))
            val bounds = mapOf("id" to KeyBounds(20, 30))

            val result = pruner.prune(setOf(file), bounds)

            result.matchingFiles.shouldBeEmpty()
            result.prunedCount shouldBe 1
        }

        @Test
        fun `matches file when ranges overlap`() {
            val file = addFile("f1.parquet", intStats("id", 1, 25))
            val bounds = mapOf("id" to KeyBounds(20, 30))

            val result = pruner.prune(setOf(file), bounds)

            result.matchingFiles shouldHaveSize 1
            result.prunedCount shouldBe 0
        }

        @Test
        fun `matches file when ranges exactly touch at boundary`() {
            val file = addFile("f1.parquet", intStats("id", 1, 20))
            val bounds = mapOf("id" to KeyBounds(20, 30))

            val result = pruner.prune(setOf(file), bounds)

            result.matchingFiles shouldHaveSize 1
            result.prunedCount shouldBe 0
        }

        @Test
        fun `matches file when source range is contained within file range`() {
            val file = addFile("f1.parquet", intStats("id", 1, 100))
            val bounds = mapOf("id" to KeyBounds(20, 30))

            val result = pruner.prune(setOf(file), bounds)

            result.matchingFiles shouldHaveSize 1
            result.prunedCount shouldBe 0
        }

        @Test
        fun `matches file when file range is contained within source range`() {
            val file = addFile("f1.parquet", intStats("id", 25, 28))
            val bounds = mapOf("id" to KeyBounds(20, 30))

            val result = pruner.prune(setOf(file), bounds)

            result.matchingFiles shouldHaveSize 1
            result.prunedCount shouldBe 0
        }
    }

    @Nested
    inner class NullAndMissingStats {
        @BeforeEach
        fun setUp() {
            pruner =
                FilePruner(
                    listOf(StructField("id", DeltaType.IntegerType, nullable = false)),
                )
        }

        @Test
        fun `matches file when stats is null`() {
            val file = addFile("f1.parquet", stats = null)
            val bounds = mapOf("id" to KeyBounds(20, 30))

            val result = pruner.prune(setOf(file), bounds)

            result.matchingFiles shouldHaveSize 1
            result.prunedCount shouldBe 0
        }

        @Test
        fun `matches file when stats JSON is malformed`() {
            val file = addFile("f1.parquet", stats = "{invalid json")
            val bounds = mapOf("id" to KeyBounds(20, 30))

            val result = pruner.prune(setOf(file), bounds)

            result.matchingFiles shouldHaveSize 1
            result.prunedCount shouldBe 0
        }

        @Test
        fun `matches file when minValues is missing for key column`() {
            val stats = """{"numRecords":10,"minValues":{},"maxValues":{"id":50},"nullCount":{"id":0}}"""
            val file = addFile("f1.parquet", stats = stats)
            val bounds = mapOf("id" to KeyBounds(20, 30))

            val result = pruner.prune(setOf(file), bounds)

            result.matchingFiles shouldHaveSize 1
            result.prunedCount shouldBe 0
        }

        @Test
        fun `matches file when maxValues is missing for key column`() {
            val stats = """{"numRecords":10,"minValues":{"id":1},"maxValues":{},"nullCount":{"id":0}}"""
            val file = addFile("f1.parquet", stats = stats)
            val bounds = mapOf("id" to KeyBounds(20, 30))

            val result = pruner.prune(setOf(file), bounds)

            result.matchingFiles shouldHaveSize 1
            result.prunedCount shouldBe 0
        }
    }

    @Nested
    inner class CompositeKeyPruning {
        @BeforeEach
        fun setUp() {
            pruner =
                FilePruner(
                    listOf(
                        StructField("tenant_id", DeltaType.StringType, nullable = false),
                        StructField("id", DeltaType.IntegerType, nullable = false),
                    ),
                )
        }

        @Test
        fun `prunes when any key column has no overlap`() {
            val stats =
                """{"numRecords":10,""" +
                    """"minValues":{"tenant_id":"A","id":1},""" +
                    """"maxValues":{"tenant_id":"C","id":10},""" +
                    """"nullCount":{"tenant_id":0,"id":0}}"""
            val file = addFile("f1.parquet", stats = stats)
            val bounds =
                mapOf(
                    "tenant_id" to KeyBounds("B" as Comparable<*>, "B" as Comparable<*>),
                    "id" to KeyBounds(50, 100),
                )

            val result = pruner.prune(setOf(file), bounds)

            result.matchingFiles.shouldBeEmpty()
            result.prunedCount shouldBe 1
        }

        @Test
        fun `matches when all key columns overlap`() {
            val stats =
                """{"numRecords":10,""" +
                    """"minValues":{"tenant_id":"A","id":1},""" +
                    """"maxValues":{"tenant_id":"C","id":10},""" +
                    """"nullCount":{"tenant_id":0,"id":0}}"""
            val file = addFile("f1.parquet", stats = stats)
            val bounds =
                mapOf(
                    "tenant_id" to KeyBounds("B" as Comparable<*>, "B" as Comparable<*>),
                    "id" to KeyBounds(5, 8),
                )

            val result = pruner.prune(setOf(file), bounds)

            result.matchingFiles shouldHaveSize 1
            result.prunedCount shouldBe 0
        }
    }

    @Nested
    inner class TypeSpecificComparison {
        @Test
        fun `prunes correctly for string keys`() {
            pruner =
                FilePruner(
                    listOf(StructField("name", DeltaType.StringType, nullable = false)),
                )
            val file = addFile("f1.parquet", stringStats("name", "aaa", "ccc"))
            val bounds = mapOf("name" to KeyBounds("ddd" as Comparable<*>, "fff" as Comparable<*>))

            val result = pruner.prune(setOf(file), bounds)

            result.prunedCount shouldBe 1
        }

        @Test
        fun `prunes correctly for long keys`() {
            pruner =
                FilePruner(
                    listOf(StructField("ts", DeltaType.LongType, nullable = false)),
                )
            val file = addFile("f1.parquet", longStats("ts", 100L, 200L))
            val bounds = mapOf("ts" to KeyBounds(300L, 400L))

            val result = pruner.prune(setOf(file), bounds)

            result.prunedCount shouldBe 1
        }

        @Test
        fun `prunes correctly for timestamp keys`() {
            pruner =
                FilePruner(
                    listOf(StructField("event_time", DeltaType.TimestampType, nullable = false)),
                )
            // Stats: 2024-01-01T00:00:00Z to 2024-01-31T23:59:59Z
            val stats =
                """{"numRecords":10,""" +
                    """"minValues":{"event_time":"2024-01-01T00:00:00Z"},""" +
                    """"maxValues":{"event_time":"2024-01-31T23:59:59Z"},""" +
                    """"nullCount":{"event_time":0}}"""
            val file = addFile("f1.parquet", stats = stats)
            // Source: March 2024 (epoch micros)
            val marchStartMicros = 1709251200000000L // 2024-03-01T00:00:00Z
            val marchEndMicros = 1711929599000000L // 2024-03-31T23:59:59Z
            val bounds = mapOf("event_time" to KeyBounds(marchStartMicros, marchEndMicros))

            val result = pruner.prune(setOf(file), bounds)

            result.prunedCount shouldBe 1
        }

        @Test
        fun `prunes correctly for decimal keys`() {
            pruner =
                FilePruner(
                    listOf(StructField("price", DeltaType.DecimalType(10, 2), nullable = false)),
                )
            val stats =
                """{"numRecords":10,""" +
                    """"minValues":{"price":"10.00"},""" +
                    """"maxValues":{"price":"50.00"},""" +
                    """"nullCount":{"price":0}}"""
            val file = addFile("f1.parquet", stats = stats)
            val bounds =
                mapOf(
                    "price" to
                        KeyBounds(
                            BigDecimal("100.00") as Comparable<*>,
                            BigDecimal("200.00") as Comparable<*>,
                        ),
                )

            val result = pruner.prune(setOf(file), bounds)

            result.prunedCount shouldBe 1
        }

        @Test
        fun `prunes correctly for float keys`() {
            pruner =
                FilePruner(
                    listOf(StructField("score", DeltaType.FloatType, nullable = false)),
                )
            val stats =
                """{"numRecords":10,""" +
                    """"minValues":{"score":1.0},""" +
                    """"maxValues":{"score":5.0},""" +
                    """"nullCount":{"score":0}}"""
            val file = addFile("f1.parquet", stats = stats)
            val bounds = mapOf("score" to KeyBounds(10.0f, 20.0f))

            val result = pruner.prune(setOf(file), bounds)

            result.prunedCount shouldBe 1
        }

        @Test
        fun `prunes correctly for double keys`() {
            pruner =
                FilePruner(
                    listOf(StructField("amount", DeltaType.DoubleType, nullable = false)),
                )
            val stats =
                """{"numRecords":10,""" +
                    """"minValues":{"amount":100.5},""" +
                    """"maxValues":{"amount":200.5},""" +
                    """"nullCount":{"amount":0}}"""
            val file = addFile("f1.parquet", stats = stats)
            val bounds = mapOf("amount" to KeyBounds(300.0, 400.0))

            val result = pruner.prune(setOf(file), bounds)

            result.prunedCount shouldBe 1
        }

        @Test
        fun `prunes correctly for boolean keys`() {
            pruner =
                FilePruner(
                    listOf(StructField("active", DeltaType.BooleanType, nullable = false)),
                )
            val stats =
                """{"numRecords":10,""" +
                    """"minValues":{"active":false},""" +
                    """"maxValues":{"active":false},""" +
                    """"nullCount":{"active":0}}"""
            val file = addFile("f1.parquet", stats = stats)
            val bounds = mapOf("active" to KeyBounds(true, true))

            val result = pruner.prune(setOf(file), bounds)

            result.prunedCount shouldBe 1
        }

        @Test
        fun `prunes correctly for date keys`() {
            pruner =
                FilePruner(
                    listOf(StructField("birth_date", DeltaType.DateType, nullable = false)),
                )
            // Stats: 2020-01-01 to 2020-12-31
            val stats =
                """{"numRecords":10,""" +
                    """"minValues":{"birth_date":"2020-01-01"},""" +
                    """"maxValues":{"birth_date":"2020-12-31"},""" +
                    """"nullCount":{"birth_date":0}}"""
            val file = addFile("f1.parquet", stats = stats)
            // Source: 2023 epoch days
            val jan2023 = 19358 // 2023-01-01
            val dec2023 = 19722 // 2023-12-31
            val bounds = mapOf("birth_date" to KeyBounds(jan2023, dec2023))

            val result = pruner.prune(setOf(file), bounds)

            result.prunedCount shouldBe 1
        }
    }

    @Nested
    inner class MultipleFiles {
        @BeforeEach
        fun setUp() {
            pruner =
                FilePruner(
                    listOf(StructField("id", DeltaType.IntegerType, nullable = false)),
                )
        }

        @Test
        fun `correctly partitions matching and pruned files`() {
            val f1 = addFile("f1.parquet", intStats("id", 1, 10)) // pruned
            val f2 = addFile("f2.parquet", intStats("id", 15, 25)) // matches
            val f3 = addFile("f3.parquet", intStats("id", 30, 40)) // pruned
            val f4 = addFile("f4.parquet", intStats("id", 20, 35)) // matches
            val f5 = addFile("f5.parquet", stats = null) // matches (no stats)

            val bounds = mapOf("id" to KeyBounds(18, 22))

            val result = pruner.prune(setOf(f1, f2, f3, f4, f5), bounds)

            result.matchingFiles shouldHaveSize 3
            result.matchingFiles
                .map { it.path }
                .shouldContainExactlyInAnyOrder("f2.parquet", "f4.parquet", "f5.parquet")
            result.prunedCount shouldBe 2
        }

        @Test
        fun `all files pruned when no overlap exists`() {
            val f1 = addFile("f1.parquet", intStats("id", 1, 10))
            val f2 = addFile("f2.parquet", intStats("id", 11, 20))

            val bounds = mapOf("id" to KeyBounds(100, 200))

            val result = pruner.prune(setOf(f1, f2), bounds)

            result.matchingFiles.shouldBeEmpty()
            result.prunedCount shouldBe 2
        }

        @Test
        fun `empty file set returns empty result`() {
            val bounds = mapOf("id" to KeyBounds(1, 10))

            val result = pruner.prune(emptySet(), bounds)

            result.matchingFiles.shouldBeEmpty()
            result.prunedCount shouldBe 0
        }
    }

    @Nested
    inner class NestedColumnStats {
        @Test
        fun `handles nested column paths in stats`() {
            pruner =
                FilePruner(
                    listOf(StructField("address.city", DeltaType.StringType, nullable = true)),
                )
            val stats =
                """{"numRecords":10,""" +
                    """"minValues":{"address":{"city":"Vancouver"}},""" +
                    """"maxValues":{"address":{"city":"Toronto"}},""" +
                    """"nullCount":{"address":{"city":0}}}"""
            val file = addFile("f1.parquet", stats = stats)
            val bounds =
                mapOf(
                    "address.city" to KeyBounds("Montreal" as Comparable<*>, "Edmonton" as Comparable<*>),
                )

            val result = pruner.prune(setOf(file), bounds)

            result.prunedCount shouldBe 1
        }
    }
}
