package com.deltaconnect.connect

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.composite.CompositeMeterRegistry
import io.micrometer.jmx.JmxConfig
import io.micrometer.jmx.JmxMeterRegistry
import org.slf4j.LoggerFactory
import java.time.Duration

/**
 * Creates and manages Micrometer meter registries for the connector.
 *
 * Always creates a [JmxMeterRegistry] so metrics appear as JMX MBeans
 *
 * Optionally creates an OTLP registry when an endpoint URL is configured,
 * enabling push-based telemetry to an OpenTelemetry collector.
 *
 * Both registries are combined in a [CompositeMeterRegistry] so every
 * metric is recorded to all active backends simultaneously.
 */
object MetricsRegistryFactory {
    private val log = LoggerFactory.getLogger(MetricsRegistryFactory::class.java)

    /**
     * Create a [MeterRegistry] with JMX always enabled and OTLP opt-in.
     *
     * @param otlpEndpoint OTLP endpoint URL (e.g., `http://collector:4318/v1/metrics`).
     *   If blank or null, only JMX is active.
     * @return A composite registry with all active backends.
     */
    fun create(otlpEndpoint: String? = null): CloseableMeterRegistry {
        val composite = CompositeMeterRegistry()

        // JMX: always on — metrics appear as MBeans under delta.sink.*
        val jmxRegistry =
            JmxMeterRegistry(
                object : JmxConfig {
                    override fun get(key: String): String? = null

                    override fun domain(): String = "delta.sink"

                    override fun step(): Duration = Duration.ofSeconds(30)
                },
                io.micrometer.core.instrument.Clock.SYSTEM,
            )
        composite.add(jmxRegistry)
        log.info("JMX metrics registry enabled: domain=delta.sink")

        if (!otlpEndpoint.isNullOrBlank()) {
            try {
                val otlpRegistry =
                    io.micrometer.registry.otlp.OtlpMeterRegistry(
                        object : io.micrometer.registry.otlp.OtlpConfig {
                            override fun get(key: String): String? = null

                            override fun url(): String = otlpEndpoint

                            override fun step(): Duration = Duration.ofSeconds(30)
                        },
                        io.micrometer.core.instrument.Clock.SYSTEM,
                    )
                composite.add(otlpRegistry)
                log.info("OTLP metrics registry enabled: endpoint={}", otlpEndpoint)
            } catch (
                @Suppress("TooGenericExceptionCaught") e: Exception,
            ) {
                // Best-effort OTLP setup — connector should still function without metrics
                log.warn("Failed to initialize OTLP metrics registry: {}", e.message)
            }
        }

        return CloseableMeterRegistry(composite)
    }
}

/**
 * Wrapper around [CompositeMeterRegistry] that supports cleanup on close.
 *
 * Closes all child registries (stops OTLP background push thread, etc.).
 */
class CloseableMeterRegistry(
    val registry: CompositeMeterRegistry,
) : AutoCloseable {
    override fun close() {
        registry.registries.forEach { child ->
            try {
                child.close()
            } catch (_: Exception) {
                // Best-effort cleanup
            }
        }
        registry.close()
    }
}
