package com.deltaconnect.connect

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Timer
import java.util.concurrent.TimeUnit

/**
 * Centralized metrics for the Delta Lake Sink Connector.
 *
 * Uses Micrometer's global registry by default. When no registry is
 * configured, all operations are no ops. In production,
 * configure a Micrometer OTLP or Prometheus registry and it will
 * automatically pick up these metrics.
 *
 * Metric naming follows Micrometer conventions: `delta.sink.*` prefix,
 * dot-separated, lowercase.
 *
 * @property registry The Micrometer meter registry.
 */
class ConnectorMetrics(
    private val registry: MeterRegistry = Metrics.globalRegistry
) {

    /** Records received in put() calls (before conversion). */
    fun recordsReceived(topic: String, count: Long) {
        Counter.builder(RECORDS_RECEIVED)
            .tag(TAG_TOPIC, topic)
            .register(registry)
            .increment(count.toDouble())
    }

    /** Records successfully converted and buffered. */
    fun recordsConverted(topic: String, count: Long) {
        Counter.builder(RECORDS_CONVERTED)
            .tag(TAG_TOPIC, topic)
            .register(registry)
            .increment(count.toDouble())
    }

    /** Records sent to dead-letter queue due to conversion errors. */
    fun recordsDlq(topic: String) {
        Counter.builder(RECORDS_DLQ)
            .tag(TAG_TOPIC, topic)
            .register(registry)
            .increment()
    }

    /** Records written in a flush. */
    fun recordsFlushed(topic: String, count: Long) {
        Counter.builder(RECORDS_FLUSHED)
            .tag(TAG_TOPIC, topic)
            .register(registry)
            .increment(count.toDouble())
    }

    /** Time a flush operation and record its duration. */
    fun recordFlushLatency(topic: String, durationMs: Long) {
        Timer.builder(FLUSH_LATENCY)
            .tag(TAG_TOPIC, topic)
            .register(registry)
            .record(durationMs, TimeUnit.MILLISECONDS)
    }

    /** Delta commit version after a successful flush. */
    fun commitVersion(topic: String, version: Long) {
        registry.gauge(
            COMMIT_VERSION,
            listOf(Tag.of(TAG_TOPIC, topic)),
            version.toDouble()
        )
    }

    fun mergeRecordsInserted(topic: String, count: Long) {
        Counter.builder(MERGE_INSERTED)
            .tag(TAG_TOPIC, topic)
            .register(registry)
            .increment(count.toDouble())
    }

    fun mergeRecordsUpdated(topic: String, count: Long) {
        Counter.builder(MERGE_UPDATED)
            .tag(TAG_TOPIC, topic)
            .register(registry)
            .increment(count.toDouble())
    }

    fun mergeRecordsDeleted(topic: String, count: Long) {
        Counter.builder(MERGE_DELETED)
            .tag(TAG_TOPIC, topic)
            .register(registry)
            .increment(count.toDouble())
    }

    fun mergeFilesRewritten(topic: String, count: Int) {
        Counter.builder(MERGE_FILES_REWRITTEN)
            .tag(TAG_TOPIC, topic)
            .register(registry)
            .increment(count.toDouble())
    }

    fun mergeFilesSkipped(topic: String, count: Int) {
        Counter.builder(MERGE_FILES_SKIPPED)
            .tag(TAG_TOPIC, topic)
            .register(registry)
            .increment(count.toDouble())
    }

    fun schemaEvolutions(topic: String) {
        Counter.builder(SCHEMA_EVOLUTIONS)
            .tag(TAG_TOPIC, topic)
            .register(registry)
            .increment()
    }

    companion object {
        private const val PREFIX = "delta.sink"
        private const val TAG_TOPIC = "topic"

        const val RECORDS_RECEIVED = "$PREFIX.records.received"
        const val RECORDS_CONVERTED = "$PREFIX.records.converted"
        const val RECORDS_DLQ = "$PREFIX.records.dlq"
        const val RECORDS_FLUSHED = "$PREFIX.records.flushed"
        const val FLUSH_LATENCY = "$PREFIX.flush.latency"
        const val COMMIT_VERSION = "$PREFIX.commit.version"
        const val MERGE_INSERTED = "$PREFIX.merge.inserted"
        const val MERGE_UPDATED = "$PREFIX.merge.updated"
        const val MERGE_DELETED = "$PREFIX.merge.deleted"
        const val MERGE_FILES_REWRITTEN = "$PREFIX.merge.files.rewritten"
        const val MERGE_FILES_SKIPPED = "$PREFIX.merge.files.skipped"
        const val SCHEMA_EVOLUTIONS = "$PREFIX.schema.evolutions"
    }
}
