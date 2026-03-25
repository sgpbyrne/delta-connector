package com.deltaconnect.connect.storage

import com.deltaconnect.protocol.storage.DeltaLogStore
import org.apache.kafka.common.config.ConfigDef

/**
 * Pluggable storage backend for the Delta Lake Sink Connector.
 *
 * Each implementation handles a specific cloud storage system (Azure ADLS Gen2,
 * AWS S3, GCP GCS, local filesystem for testing). Providers are discovered via
 * [java.util.ServiceLoader] or registered programmatically with [StorageProviderRegistry].
 *
 * The URI scheme of `delta.storage.path` determines which provider is used:
 * - `abfss://` = Azure ADLS Gen2
 * - `s3://` or `s3a://` = AWS S3
 * - `gs://` = Google Cloud Storage
 * - `file://` or plain path = Local filesystem (testing)
 */
interface StorageProvider {
    /**
     * URI schemes this provider handles.
     *
     * Examples: `setOf("abfss")`, `setOf("s3", "s3a")`, `setOf("gs")`, `setOf("file")`
     */
    fun schemes(): Set<String>

    /**
     * Add provider-specific configuration keys to the given [ConfigDef].
     *
     * Called once at class-load time when building the connector's ConfigDef.
     * Providers should add their storage-specific config keys (e.g., auth type,
     * account name, credentials).
     *
     * @param configDef The ConfigDef to add keys to.
     * @return The same ConfigDef instance (for chaining).
     */
    fun defineConfig(configDef: ConfigDef): ConfigDef

    /**
     * Validate provider specific configuration properties.
     *
     * Called during cross property validation to check provider specific
     * constraints (e.g. Azure requires account key when auth type is storage_key).
     *
     * @param props The full connector configuration properties.
     * @return List of error messages, empty if valid.
     */
    fun validate(props: Map<String, String>): List<String>

    /**
     * Create a [DeltaLogStore] from the given configuration properties.
     *
     * @param props The full connector configuration properties.
     * @return A configured DeltaLogStore for this storage backend.
     */
    fun createLogStore(props: Map<String, String>): DeltaLogStore

    /**
     * Parse the base path (relative to the storage root) from the storage URI.
     *
     * For example: `abfss://container@account.dfs.core.windows.net/base/path`
     * returns `base/path`.
     *
     * @param storagePath The full storage path URI.
     * @return The relative base path.
     */
    fun parseBasePath(storagePath: String): String
}
