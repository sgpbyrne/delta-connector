package com.deltaconnect.connect

import com.deltaconnect.connect.storage.StorageProviderRegistry
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Type
import org.apache.kafka.common.config.ConfigDef.Width
import org.apache.kafka.common.config.ConfigException

/**
 * Configuration for the Delta Lake Sink Connector.
 *
 * Groups:
 * - Delta Lake: storage path, table naming, write behavior, checkpointing
 * - CDC: Debezium envelope format and source database type (only when write.mode=cdc)
 * - Unity Catalog: optional table registration
 * - Storage provider-specific: contributed dynamically by [StorageProviderRegistry]
 */
class DeltaSinkConfig(props: Map<String, String>) : AbstractConfig(CONFIG_DEF, props) {

    val storagePath: String get() = getString(DELTA_STORAGE_PATH)
    val mergeKeys: List<String> get() = getList(DELTA_MERGE_KEYS)
    val tableName: String get() = getString(DELTA_TABLE_NAME)
    val writeMode: WriteMode get() = WriteMode.fromString(getString(DELTA_WRITE_MODE))
    val mergeBatchSize: Int get() = getInt(DELTA_MERGE_BATCH_SIZE)
    val mergeIntervalMs: Long get() = getLong(DELTA_MERGE_INTERVAL_MS)
    val mergeDeleteEnabled: Boolean get() = getBoolean(DELTA_MERGE_DELETE_ENABLED)
    val schemaEvolutionEnabled: Boolean get() = getBoolean(DELTA_SCHEMA_EVOLUTION)
    val schemaRegistryUrl: String get() = getString(DELTA_SCHEMA_REGISTRY_URL)
    val checkpointInterval: Int get() = getInt(DELTA_CHECKPOINT_INTERVAL)

    val cdcEnvelopeFormat: CdcEnvelopeFormat
        get() = CdcEnvelopeFormat.fromString(getString(CDC_ENVELOPE_FORMAT))
    val cdcSourceDatabase: String get() = getString(CDC_SOURCE_DATABASE)

    val unityCatalogEnabled: Boolean get() = getBoolean(UNITY_CATALOG_ENABLED)
    val unityCatalogWorkspaceUrl: String get() = getString(UNITY_CATALOG_WORKSPACE_URL)
    val unityCatalogName: String get() = getString(UNITY_CATALOG_NAME)
    val unityCatalogSchema: String get() = getString(UNITY_CATALOG_SCHEMA)
    val unityCatalogWarehouseId: String get() = getString(UNITY_CATALOG_WAREHOUSE_ID)
    val unityCatalogSyncIntervalMs: Long get() = getLong(UNITY_CATALOG_SYNC_INTERVAL_MS)

    /**
     * Write mode determines how incoming records are processed and written
     * to the Delta table.
     */
    enum class WriteMode(val value: String) {
        APPEND("append"),
        UPSERT("upsert"),
        CDC("cdc");

        companion object {
            fun fromString(value: String): WriteMode =
                entries.find { it.value == value }
                    ?: throw ConfigException("Invalid write mode: $value")
        }
    }

    enum class CdcEnvelopeFormat(val value: String) {
        DEBEZIUM_FULL("debezium_full"),
        DEBEZIUM_FLATTENED("debezium_flattened");

        companion object {
            fun fromString(value: String): CdcEnvelopeFormat =
                entries.find { it.value == value }
                    ?: throw ConfigException("Invalid CDC envelope format: $value")
        }
    }

    companion object {
        private const val DELTA_GROUP = "Delta Lake"

        const val DELTA_STORAGE_PATH = "delta.storage.path"
        private const val DELTA_STORAGE_PATH_DOC =
            "Base path for Delta tables (e.g. abfss://container@account.dfs.core.windows.net/path, " +
                "s3a://bucket/path, gs://bucket/path)."

        const val DELTA_MERGE_KEYS = "delta.merge.keys"
        private const val DELTA_MERGE_KEYS_DOC =
            "Comma separated list of column names used as merge keys for upsert/delete operations. " +
                "Required when delta.write.mode is 'upsert' or 'cdc'."

        const val DELTA_TABLE_NAME = "delta.table.name"
        private const val DELTA_TABLE_NAME_DOC =
            "Delta table name template. Supports \${topic} placeholder " +
                "(e.g. \${topic} or cdc_\${topic})."
        private const val DELTA_TABLE_NAME_DEFAULT = "\${topic}"

        const val DELTA_WRITE_MODE = "delta.write.mode"
        private const val DELTA_WRITE_MODE_DOC =
            "Write mode: 'append' (every record is an INSERT, no merge), " +
                "'upsert' (keyed records, insert/update/delete via tombstones), " +
                "'cdc' (Debezium envelope parsing with insert/update/delete)."
        private const val DELTA_WRITE_MODE_DEFAULT = "cdc"

        const val DELTA_MERGE_BATCH_SIZE = "delta.merge.batch.size"
        private const val DELTA_MERGE_BATCH_SIZE_DOC =
            "Maximum number of records to buffer before triggering a flush."
        private const val DELTA_MERGE_BATCH_SIZE_DEFAULT = 50_000

        const val DELTA_MERGE_INTERVAL_MS = "delta.merge.interval.ms"
        private const val DELTA_MERGE_INTERVAL_MS_DOC =
            "Maximum time in milliseconds to buffer records before triggering a flush."
        private const val DELTA_MERGE_INTERVAL_MS_DEFAULT = 60_000L

        const val DELTA_MERGE_DELETE_ENABLED = "delta.merge.delete.enabled"
        private const val DELTA_MERGE_DELETE_ENABLED_DOC =
            "Whether to process delete operations (upsert and cdc modes). " +
                "When false, deletes are ignored."
        private const val DELTA_MERGE_DELETE_ENABLED_DEFAULT = true

        const val DELTA_SCHEMA_EVOLUTION = "delta.schema.evolution"
        private const val DELTA_SCHEMA_EVOLUTION_DOC =
            "Enable automatic schema evolution when new columns appear in incoming events."
        private const val DELTA_SCHEMA_EVOLUTION_DEFAULT = true

        const val DELTA_SCHEMA_REGISTRY_URL = "delta.schema.registry.url"
        private const val DELTA_SCHEMA_REGISTRY_URL_DOC =
            "URL of the Confluent Schema Registry. Required when using Avro events."
        private const val DELTA_SCHEMA_REGISTRY_URL_DEFAULT = ""

        const val DELTA_CHECKPOINT_INTERVAL = "delta.checkpoint.interval"
        private const val DELTA_CHECKPOINT_INTERVAL_DOC =
            "Number of Delta commits between automatic checkpoint writes."
        private const val DELTA_CHECKPOINT_INTERVAL_DEFAULT = 10

        private const val CDC_GROUP = "CDC"

        const val CDC_ENVELOPE_FORMAT = "cdc.envelope.format"
        private const val CDC_ENVELOPE_FORMAT_DOC =
            "Debezium envelope format: 'debezium_full' (before/after envelope) or " +
                "'debezium_flattened' (ExtractNewRecordState SMT applied). " +
                "Only used when delta.write.mode=cdc."
        private const val CDC_ENVELOPE_FORMAT_DEFAULT = "debezium_full"

        const val CDC_SOURCE_DATABASE = "cdc.source.database"
        private const val CDC_SOURCE_DATABASE_DOC =
            "Source database type for CDC event parsing and type mapping. " +
                "Only used when delta.write.mode=cdc."
        private const val CDC_SOURCE_DATABASE_DEFAULT = "sqlserver"

        private const val UNITY_CATALOG_GROUP = "Unity Catalog"

        const val UNITY_CATALOG_ENABLED = "unity.catalog.enabled"
        private const val UNITY_CATALOG_ENABLED_DOC =
            "Enable automatic table registration in Unity Catalog."
        private const val UNITY_CATALOG_ENABLED_DEFAULT = false

        const val UNITY_CATALOG_WORKSPACE_URL = "unity.catalog.workspace.url"
        private const val UNITY_CATALOG_WORKSPACE_URL_DOC =
            "Databricks workspace URL (e.g. https://adb-xxxx.azuredatabricks.net). " +
                "Auth uses cloud credentials (DefaultAzureCredential / workload identity)."

        const val UNITY_CATALOG_NAME = "unity.catalog.name"
        private const val UNITY_CATALOG_NAME_DOC =
            "Unity Catalog name to register tables in."

        const val UNITY_CATALOG_SCHEMA = "unity.catalog.schema"
        private const val UNITY_CATALOG_SCHEMA_DOC =
            "Unity Catalog schema name to register tables in."

        const val UNITY_CATALOG_WAREHOUSE_ID = "unity.catalog.warehouse.id"
        private const val UNITY_CATALOG_WAREHOUSE_ID_DOC =
            "SQL warehouse ID for executing Unity Catalog registration statements."

        const val UNITY_CATALOG_SYNC_INTERVAL_MS = "unity.catalog.sync.interval.ms"
        private const val UNITY_CATALOG_SYNC_INTERVAL_MS_DOC =
            "Interval in milliseconds between Unity Catalog REPAIR TABLE SYNC METADATA calls."
        private const val UNITY_CATALOG_SYNC_INTERVAL_MS_DEFAULT = 300_000L

        val CONFIG_DEF: ConfigDef = buildConfigDef()

        private fun buildConfigDef(): ConfigDef {
            val configDef = ConfigDef()
                .define(
                    DELTA_STORAGE_PATH, Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    Importance.HIGH, DELTA_STORAGE_PATH_DOC,
                    DELTA_GROUP, 1, Width.LONG, "Storage Path"
                )
                .define(
                    DELTA_MERGE_KEYS, Type.LIST, "",
                    Importance.HIGH, DELTA_MERGE_KEYS_DOC,
                    DELTA_GROUP, 2, Width.LONG, "Merge Keys"
                )
                .define(
                    DELTA_TABLE_NAME, Type.STRING, DELTA_TABLE_NAME_DEFAULT,
                    Importance.MEDIUM, DELTA_TABLE_NAME_DOC,
                    DELTA_GROUP, 3, Width.LONG, "Table Name Template"
                )
                .define(
                    DELTA_WRITE_MODE, Type.STRING, DELTA_WRITE_MODE_DEFAULT,
                    ConfigDef.ValidString.`in`("append", "upsert", "cdc"),
                    Importance.HIGH, DELTA_WRITE_MODE_DOC,
                    DELTA_GROUP, 4, Width.MEDIUM, "Write Mode"
                )
                .define(
                    DELTA_MERGE_BATCH_SIZE, Type.INT, DELTA_MERGE_BATCH_SIZE_DEFAULT,
                    ConfigDef.Range.atLeast(1),
                    Importance.MEDIUM, DELTA_MERGE_BATCH_SIZE_DOC,
                    DELTA_GROUP, 5, Width.SHORT, "Batch Size"
                )
                .define(
                    DELTA_MERGE_INTERVAL_MS, Type.LONG, DELTA_MERGE_INTERVAL_MS_DEFAULT,
                    ConfigDef.Range.atLeast(1000),
                    Importance.MEDIUM, DELTA_MERGE_INTERVAL_MS_DOC,
                    DELTA_GROUP, 6, Width.SHORT, "Flush Interval (ms)"
                )
                .define(
                    DELTA_MERGE_DELETE_ENABLED, Type.BOOLEAN, DELTA_MERGE_DELETE_ENABLED_DEFAULT,
                    Importance.MEDIUM, DELTA_MERGE_DELETE_ENABLED_DOC,
                    DELTA_GROUP, 7, Width.SHORT, "Delete Enabled"
                )
                .define(
                    DELTA_SCHEMA_EVOLUTION, Type.BOOLEAN, DELTA_SCHEMA_EVOLUTION_DEFAULT,
                    Importance.MEDIUM, DELTA_SCHEMA_EVOLUTION_DOC,
                    DELTA_GROUP, 8, Width.SHORT, "Schema Evolution"
                )
                .define(
                    DELTA_SCHEMA_REGISTRY_URL, Type.STRING, DELTA_SCHEMA_REGISTRY_URL_DEFAULT,
                    Importance.LOW, DELTA_SCHEMA_REGISTRY_URL_DOC,
                    DELTA_GROUP, 9, Width.LONG, "Schema Registry URL"
                )
                .define(
                    DELTA_CHECKPOINT_INTERVAL, Type.INT, DELTA_CHECKPOINT_INTERVAL_DEFAULT,
                    ConfigDef.Range.atLeast(1),
                    Importance.LOW, DELTA_CHECKPOINT_INTERVAL_DOC,
                    DELTA_GROUP, 10, Width.SHORT, "Checkpoint Interval"
                )
                .define(
                    CDC_ENVELOPE_FORMAT, Type.STRING, CDC_ENVELOPE_FORMAT_DEFAULT,
                    ConfigDef.ValidString.`in`("debezium_full", "debezium_flattened"),
                    Importance.HIGH, CDC_ENVELOPE_FORMAT_DOC,
                    CDC_GROUP, 1, Width.MEDIUM, "Envelope Format"
                )
                .define(
                    CDC_SOURCE_DATABASE, Type.STRING, CDC_SOURCE_DATABASE_DEFAULT,
                    ConfigDef.ValidString.`in`("sqlserver"),
                    Importance.HIGH, CDC_SOURCE_DATABASE_DOC,
                    CDC_GROUP, 2, Width.MEDIUM, "Source Database"
                )
                .define(
                    UNITY_CATALOG_ENABLED, Type.BOOLEAN, UNITY_CATALOG_ENABLED_DEFAULT,
                    Importance.MEDIUM, UNITY_CATALOG_ENABLED_DOC,
                    UNITY_CATALOG_GROUP, 1, Width.SHORT, "Enabled"
                )
                .define(
                    UNITY_CATALOG_WORKSPACE_URL, Type.STRING, "",
                    Importance.MEDIUM, UNITY_CATALOG_WORKSPACE_URL_DOC,
                    UNITY_CATALOG_GROUP, 2, Width.LONG, "Workspace URL"
                )
                .define(
                    UNITY_CATALOG_NAME, Type.STRING, "",
                    Importance.MEDIUM, UNITY_CATALOG_NAME_DOC,
                    UNITY_CATALOG_GROUP, 3, Width.LONG, "Catalog Name"
                )
                .define(
                    UNITY_CATALOG_SCHEMA, Type.STRING, "",
                    Importance.MEDIUM, UNITY_CATALOG_SCHEMA_DOC,
                    UNITY_CATALOG_GROUP, 4, Width.LONG, "Schema Name"
                )
                .define(
                    UNITY_CATALOG_WAREHOUSE_ID, Type.STRING, "",
                    Importance.MEDIUM, UNITY_CATALOG_WAREHOUSE_ID_DOC,
                    UNITY_CATALOG_GROUP, 5, Width.LONG, "Warehouse ID"
                )
                .define(
                    UNITY_CATALOG_SYNC_INTERVAL_MS, Type.LONG,
                    UNITY_CATALOG_SYNC_INTERVAL_MS_DEFAULT,
                    ConfigDef.Range.atLeast(10_000),
                    Importance.LOW, UNITY_CATALOG_SYNC_INTERVAL_MS_DOC,
                    UNITY_CATALOG_GROUP, 6, Width.SHORT, "Sync Interval (ms)"
                )

            // Add storage provider config keys (discovered via ServiceLoader)
            for (provider in StorageProviderRegistry.allProviders()) {
                provider.defineConfig(configDef)
            }

            return configDef
        }

        /**
         * Validates cross-property dependencies. Returns a list of error messages.
         */
        fun validateCrossProperties(props: Map<String, String>): List<String> {
            val errors = mutableListOf<String>()
            val config = try {
                DeltaSinkConfig(props)
            } catch (e: ConfigException) {
                return listOf(e.message ?: "Invalid configuration")
            }

            if (config.writeMode != WriteMode.APPEND && config.mergeKeys.isEmpty()) {
                errors.add(
                    "$DELTA_MERGE_KEYS is required when $DELTA_WRITE_MODE " +
                        "is '${config.writeMode.value}'"
                )
            }

            try {
                val provider = StorageProviderRegistry.resolve(config.storagePath)
                errors.addAll(provider.validate(props))
            } catch (e: ConfigException) {
                errors.add(e.message ?: "Storage provider error")
            }

            if (config.unityCatalogEnabled) {
                if (config.unityCatalogWorkspaceUrl.isBlank()) {
                    errors.add(
                        "$UNITY_CATALOG_WORKSPACE_URL is required " +
                            "when $UNITY_CATALOG_ENABLED=true"
                    )
                }
                if (config.unityCatalogName.isBlank()) {
                    errors.add(
                        "$UNITY_CATALOG_NAME is required " +
                            "when $UNITY_CATALOG_ENABLED=true"
                    )
                }
                if (config.unityCatalogSchema.isBlank()) {
                    errors.add(
                        "$UNITY_CATALOG_SCHEMA is required " +
                            "when $UNITY_CATALOG_ENABLED=true"
                    )
                }
                if (config.unityCatalogWarehouseId.isBlank()) {
                    errors.add(
                        "$UNITY_CATALOG_WAREHOUSE_ID is required " +
                            "when $UNITY_CATALOG_ENABLED=true"
                    )
                }
            }

            return errors
        }

        /**
         * Resolves the Delta table name from the template, substituting \${topic}.
         */
        fun resolveTableName(template: String, topic: String): String =
            template.replace("\${topic}", topic)
    }
}
