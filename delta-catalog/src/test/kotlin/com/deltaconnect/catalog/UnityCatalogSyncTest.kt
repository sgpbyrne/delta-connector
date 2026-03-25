package com.deltaconnect.catalog

import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

class UnityCatalogSyncTest {
    private lateinit var client: DatabricksSqlClient
    private var currentTime: Long = 1_000_000L

    @BeforeEach
    fun setUp() {
        client = mockk(relaxed = true)
        every { client.registerTable(any(), any(), any(), any()) } returns
            StatementResult(
                statementId = "stmt-001",
                state = StatementState.SUCCEEDED,
            )
        every { client.refreshTable(any(), any(), any()) } returns
            StatementResult(
                statementId = "stmt-002",
                state = StatementState.SUCCEEDED,
            )
    }

    @Nested
    inner class Registration {
        @Test
        fun `registers table on first flush`() {
            val sync = createSync()

            sync.onTableFlush("orders", "abfss://c@a.dfs.core.windows.net/delta/orders")

            verify(exactly = 1) {
                client.registerTable(
                    "test_catalog",
                    "test_schema",
                    "orders",
                    "abfss://c@a.dfs.core.windows.net/delta/orders",
                )
            }
            sync.registeredTableCount() shouldBe 1
        }

        @Test
        fun `does not reregister on subsequent flushes`() {
            val sync = createSync()

            sync.onTableFlush("orders", "abfss://c@a.dfs.core.windows.net/delta/orders")
            sync.onTableFlush("orders", "abfss://c@a.dfs.core.windows.net/delta/orders")
            sync.onTableFlush("orders", "abfss://c@a.dfs.core.windows.net/delta/orders")

            verify(exactly = 1) {
                client.registerTable(any(), any(), any(), any())
            }
        }

        @Test
        fun `registers multiple tables independently`() {
            val sync = createSync()

            sync.onTableFlush("orders", "abfss://c@a.dfs.core.windows.net/delta/orders")
            sync.onTableFlush("customers", "abfss://c@a.dfs.core.windows.net/delta/customers")

            verify(exactly = 1) {
                client.registerTable("test_catalog", "test_schema", "orders", any())
            }
            verify(exactly = 1) {
                client.registerTable("test_catalog", "test_schema", "customers", any())
            }
            sync.registeredTableCount() shouldBe 2
        }

        @Test
        fun `continues on registration failure`() {
            every { client.registerTable(any(), any(), any(), any()) } throws
                SqlExecutionException("Warehouse unavailable", "CREATE TABLE ...")

            val sync = createSync()

            sync.onTableFlush("orders", "abfss://c@a.dfs.core.windows.net/delta/orders")

            sync.registeredTableCount() shouldBe 0
        }
    }

    @Nested
    inner class PeriodicRefresh {
        @Test
        fun `refreshes table after sync interval`() {
            val sync = createSync(syncIntervalMs = 5_000)

            sync.onTableFlush("orders", "abfss://c@a.dfs.core.windows.net/delta/orders")

            currentTime += 3_000
            sync.onTableFlush("orders", "abfss://c@a.dfs.core.windows.net/delta/orders")
            verify(exactly = 0) { client.refreshTable(any(), any(), any()) }

            currentTime += 3_000 // total 6s > 5s interval
            sync.onTableFlush("orders", "abfss://c@a.dfs.core.windows.net/delta/orders")
            verify(exactly = 1) {
                client.refreshTable("test_catalog", "test_schema", "orders")
            }
        }

        @Test
        fun `continues on refresh failure`() {
            every { client.refreshTable(any(), any(), any()) } throws
                SqlExecutionException("Timeout", "MSCK REPAIR TABLE ...")

            val sync = createSync(syncIntervalMs = 1_000)

            sync.onTableFlush("orders", "abfss://c@a.dfs.core.windows.net/delta/orders")

            currentTime += 2_000
            sync.onTableFlush("orders", "abfss://c@a.dfs.core.windows.net/delta/orders")

            sync.registeredTableCount() shouldBe 1
        }

        @Test
        fun `shouldSync returns true when interval exceeded`() {
            val sync = createSync(syncIntervalMs = 5_000)

            sync.onTableFlush("orders", "abfss://c@a.dfs.core.windows.net/delta/orders")

            sync.shouldSync("orders") shouldBe false

            currentTime += 6_000
            sync.shouldSync("orders") shouldBe true
        }

        @Test
        fun `shouldSync returns true for unregistered table`() {
            val sync = createSync()

            sync.shouldSync("nonexistent") shouldBe true
        }
    }

    private fun createSync(syncIntervalMs: Long = 300_000): UnityCatalogSync =
        UnityCatalogSync(
            client = client,
            catalog = "test_catalog",
            schema = "test_schema",
            syncIntervalMs = syncIntervalMs,
            clock = { currentTime },
        )
}
