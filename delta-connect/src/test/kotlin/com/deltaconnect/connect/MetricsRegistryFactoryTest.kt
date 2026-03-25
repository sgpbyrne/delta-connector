package com.deltaconnect.connect

import io.kotest.matchers.collections.shouldHaveAtLeastSize
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import io.micrometer.jmx.JmxMeterRegistry
import org.junit.jupiter.api.Test

class MetricsRegistryFactoryTest {
    @Test
    fun `creates JMX registry by default`() {
        val closeable = MetricsRegistryFactory.create()
        try {
            val registries = closeable.registry.registries.toList()
            registries shouldHaveAtLeastSize 1
            registries[0].shouldBeInstanceOf<JmxMeterRegistry>()
        } finally {
            closeable.close()
        }
    }

    @Test
    fun `creates JMX plus OTLP when endpoint provided`() {
        val closeable =
            MetricsRegistryFactory.create(
                otlpEndpoint = "http://localhost:4318/v1/metrics",
            )
        try {
            val registries = closeable.registry.registries.toList()
            registries shouldHaveAtLeastSize 2
            registries.any { it is JmxMeterRegistry } shouldBe true
        } finally {
            closeable.close()
        }
    }

    @Test
    fun `skips OTLP when endpoint is blank`() {
        val closeable = MetricsRegistryFactory.create(otlpEndpoint = "")
        try {
            val registries = closeable.registry.registries.toList()
            registries.size shouldBe 1
            registries[0].shouldBeInstanceOf<JmxMeterRegistry>()
        } finally {
            closeable.close()
        }
    }

    @Test
    fun `skips OTLP when endpoint is null`() {
        val closeable = MetricsRegistryFactory.create(otlpEndpoint = null)
        try {
            closeable.registry.registries.size shouldBe 1
        } finally {
            closeable.close()
        }
    }

    @Test
    fun `close shuts down all registries`() {
        val closeable = MetricsRegistryFactory.create()
        closeable.close()

        // After close, registry should be closed (isClosed)
        closeable.registry.isClosed shouldBe true
    }

    @Test
    fun `metrics recorded to JMX registry are accessible`() {
        val closeable = MetricsRegistryFactory.create()
        try {
            val metrics = ConnectorMetrics(closeable.registry)
            metrics.recordsReceived("orders", 42)

            val counter =
                closeable.registry
                    .find(ConnectorMetrics.RECORDS_RECEIVED)
                    .tag("topic", "orders")
                    .counter()
            counter!!.count() shouldBe 42.0
        } finally {
            closeable.close()
        }
    }
}
