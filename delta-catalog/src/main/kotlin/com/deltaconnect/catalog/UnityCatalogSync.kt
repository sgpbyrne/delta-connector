package com.deltaconnect.catalog

import org.slf4j.LoggerFactory

/**
 * Orchestrates Unity Catalog registration and periodic metadata refresh
 * for Delta tables written by the sink connector.
 *
 * On first write to a table, registers it as an external Delta table in
 * Unity Catalog. Periodically refreshes metadata to sync schema changes
 * and new files.
 *
 * All operations are best effort: failures are logged but do not block
 * the data pipeline. The connector continues writing data regardless of
 * UC sync status.
 *
 * @property client Databricks SQL client for executing UC statements.
 * @property catalog Unity Catalog name.
 * @property schema Unity Catalog schema name.
 * @property syncIntervalMs Interval between periodic metadata refreshes.
 * @property clock Time supplier for testing.
 */
class UnityCatalogSync(
    private val client: DatabricksSqlClient,
    private val catalog: String,
    private val schema: String,
    private val syncIntervalMs: Long,
    private val clock: () -> Long = System::currentTimeMillis
) {

    private val log = LoggerFactory.getLogger(UnityCatalogSync::class.java)

    /** Tables that have been registered. */
    private val registeredTables = mutableMapOf<String, Long>()

    /** Last sync time per table. */
    private val lastSyncTime = mutableMapOf<String, Long>()

    /**
     * Called after a successful Delta table flush. Handles initial
     * registration and periodic metadata refresh.
     *
     * @param tableName Delta table name (as stored in the Delta log).
     * @param location Full storage location (e.g. `abfss://container@account.dfs.core.windows.net/path/table`).
     */
    fun onTableFlush(tableName: String, location: String) {
        if (tableName !in registeredTables) {
            registerTable(tableName, location)
        } else if (shouldSync(tableName)) {
            refreshTable(tableName)
        }
    }

    /**
     * Whether a table needs a metadata refresh based on the sync interval.
     */
    fun shouldSync(tableName: String): Boolean {
        val lastSync = lastSyncTime[tableName] ?: return true
        return clock() - lastSync >= syncIntervalMs
    }

    private fun registerTable(tableName: String, location: String) {
        try {
            client.registerTable(catalog, schema, tableName, location)
            val now = clock()
            registeredTables[tableName] = now
            lastSyncTime[tableName] = now
            log.info(
                "Table registered in Unity Catalog: {}.{}.{}",
                catalog, schema, tableName
            )
        } catch (e: Exception) {
            log.warn(
                "Failed to register table in Unity Catalog: {}.{}.{} — {}",
                catalog, schema, tableName, e.message
            )
        }
    }

    private fun refreshTable(tableName: String) {
        try {
            client.refreshTable(catalog, schema, tableName)
            lastSyncTime[tableName] = clock()
            log.info(
                "Table metadata refreshed in Unity Catalog: {}.{}.{}",
                catalog, schema, tableName
            )
        } catch (e: Exception) {
            log.warn(
                "Failed to refresh table metadata in Unity Catalog: {}.{}.{} — {}",
                catalog, schema, tableName, e.message
            )
        }
    }

    /** Number of tables currently registered. For testing. */
    internal fun registeredTableCount(): Int = registeredTables.size
}
