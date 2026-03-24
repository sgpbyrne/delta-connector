package com.deltaconnect.connect

import io.kotest.matchers.doubles.shouldBeGreaterThan
import io.kotest.matchers.shouldBe
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class ConnectorMetricsTest {

    private lateinit var registry: SimpleMeterRegistry
    private lateinit var metrics: ConnectorMetrics

    @BeforeEach
    fun setUp() {
        registry = SimpleMeterRegistry()
        metrics = ConnectorMetrics(registry)
    }

    @Test
    fun `records received counter increments`() {
        metrics.recordsReceived("orders", 10)
        metrics.recordsReceived("orders", 5)

        val counter = registry.find(ConnectorMetrics.RECORDS_RECEIVED)
            .tag("topic", "orders").counter()
        counter!!.count() shouldBe 15.0
    }

    @Test
    fun `records converted counter increments`() {
        metrics.recordsConverted("orders", 8)

        val counter = registry.find(ConnectorMetrics.RECORDS_CONVERTED)
            .tag("topic", "orders").counter()
        counter!!.count() shouldBe 8.0
    }

    @Test
    fun `DLQ counter increments per record`() {
        metrics.recordsDlq("orders")
        metrics.recordsDlq("orders")

        val counter = registry.find(ConnectorMetrics.RECORDS_DLQ)
            .tag("topic", "orders").counter()
        counter!!.count() shouldBe 2.0
    }

    @Test
    fun `records flushed counter`() {
        metrics.recordsFlushed("orders", 100)

        val counter = registry.find(ConnectorMetrics.RECORDS_FLUSHED)
            .tag("topic", "orders").counter()
        counter!!.count() shouldBe 100.0
    }

    @Test
    fun `flush latency timer records duration`() {
        metrics.recordFlushLatency("orders", 250)

        val timer = registry.find(ConnectorMetrics.FLUSH_LATENCY)
            .tag("topic", "orders").timer()
        timer!!.count() shouldBe 1
        timer.totalTime(java.util.concurrent.TimeUnit.MILLISECONDS) shouldBe 250.0
    }

    @Test
    fun `merge counters track operations`() {
        metrics.mergeRecordsInserted("orders", 5)
        metrics.mergeRecordsUpdated("orders", 3)
        metrics.mergeRecordsDeleted("orders", 2)
        metrics.mergeFilesRewritten("orders", 1)
        metrics.mergeFilesSkipped("orders", 4)

        registry.find(ConnectorMetrics.MERGE_INSERTED)
            .tag("topic", "orders").counter()!!.count() shouldBe 5.0
        registry.find(ConnectorMetrics.MERGE_UPDATED)
            .tag("topic", "orders").counter()!!.count() shouldBe 3.0
        registry.find(ConnectorMetrics.MERGE_DELETED)
            .tag("topic", "orders").counter()!!.count() shouldBe 2.0
        registry.find(ConnectorMetrics.MERGE_FILES_REWRITTEN)
            .tag("topic", "orders").counter()!!.count() shouldBe 1.0
        registry.find(ConnectorMetrics.MERGE_FILES_SKIPPED)
            .tag("topic", "orders").counter()!!.count() shouldBe 4.0
    }

    @Test
    fun `schema evolution counter`() {
        metrics.schemaEvolutions("orders")
        metrics.schemaEvolutions("orders")

        registry.find(ConnectorMetrics.SCHEMA_EVOLUTIONS)
            .tag("topic", "orders").counter()!!.count() shouldBe 2.0
    }

    @Test
    fun `metrics are tagged by topic`() {
        metrics.recordsReceived("orders", 10)
        metrics.recordsReceived("customers", 5)

        registry.find(ConnectorMetrics.RECORDS_RECEIVED)
            .tag("topic", "orders").counter()!!.count() shouldBe 10.0
        registry.find(ConnectorMetrics.RECORDS_RECEIVED)
            .tag("topic", "customers").counter()!!.count() shouldBe 5.0
    }
}
