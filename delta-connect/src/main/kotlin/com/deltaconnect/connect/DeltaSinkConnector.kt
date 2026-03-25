package com.deltaconnect.connect

import org.apache.kafka.common.config.Config
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigValue
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.sink.SinkConnector
import org.slf4j.LoggerFactory

/**
 * Kafka Connect SinkConnector that writes records to Delta Lake tables.
 *
 * Supports multiple write modes: append (every record is an INSERT),
 * upsert (keyed records with merge), and cdc (Debezium envelope parsing
 * with full insert/update/delete).
 *
 * Storage backend is determined by the URI scheme of `delta.storage.path`
 * (e.g. `abfss://` for Azure ADLS Gen2, `s3a://` for AWS S3).
 */
class DeltaSinkConnector : SinkConnector() {
    private val log = LoggerFactory.getLogger(DeltaSinkConnector::class.java)

    private lateinit var configProps: Map<String, String>

    override fun version(): String = VERSION

    override fun start(props: Map<String, String>) {
        log.info("Starting DeltaSinkConnector")
        configProps = props
        DeltaSinkConfig(props)
        log.info(
            "DeltaSinkConnector started with storage.path={}, write.mode={}, merge.keys={}",
            props[DeltaSinkConfig.DELTA_STORAGE_PATH],
            props[DeltaSinkConfig.DELTA_WRITE_MODE] ?: "cdc",
            props[DeltaSinkConfig.DELTA_MERGE_KEYS],
        )
    }

    override fun taskClass(): Class<out Task> = DeltaSinkTask::class.java

    override fun taskConfigs(maxTasks: Int): List<Map<String, String>> = (1..maxTasks).map { configProps }

    override fun stop() {
        log.info("Stopping DeltaSinkConnector")
    }

    override fun config(): ConfigDef = DeltaSinkConfig.CONFIG_DEF

    override fun validate(connectorConfigs: Map<String, String>): Config {
        val baseConfig = super.validate(connectorConfigs)

        val crossErrors = DeltaSinkConfig.validateCrossProperties(connectorConfigs)
        if (crossErrors.isEmpty()) {
            return baseConfig
        }

        val configValues = baseConfig.configValues().associateBy { it.name() }
        for (error in crossErrors) {
            val targetKey = findBestConfigKey(error, configValues)
            targetKey?.addErrorMessage(error) ?: run {
                // Fallback: attach to first config value
                baseConfig.configValues().firstOrNull()?.addErrorMessage(error)
            }
        }

        return baseConfig
    }

    private fun findBestConfigKey(
        error: String,
        configValues: Map<String, ConfigValue>,
    ): ConfigValue? =
        configValues.entries
            .firstOrNull { (key, _) -> error.contains(key) }
            ?.value

    companion object {
        const val VERSION = "0.1.0-SNAPSHOT"
    }
}
