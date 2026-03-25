package com.deltaconnect.catalog

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.PropertyNamingStrategies
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.slf4j.LoggerFactory
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration

/**
 * HTTP client for the Databricks SQL Statement Execution API.
 *
 * Submits SQL statements to a Databricks SQL warehouse and waits for
 * the result synchronously. Used to register and refresh Delta tables
 * in Unity Catalog.
 *
 * @property workspaceUrl Databricks workspace URL (e.g. `https://adb-xxxx.azuredatabricks.net`).
 * @property warehouseId SQL warehouse ID for statement execution.
 * @property tokenSupplier Supplies a bearer token for authentication (PAT or cloud credential).
 * @property requestTimeout HTTP request timeout.
 */
class DatabricksSqlClient(
    private val workspaceUrl: String,
    private val warehouseId: String,
    private val tokenSupplier: () -> String,
    private val requestTimeout: Duration = Duration.ofSeconds(30),
) {
    private val log = LoggerFactory.getLogger(DatabricksSqlClient::class.java)

    private val httpClient: HttpClient =
        HttpClient
            .newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build()

    private val mapper: ObjectMapper =
        jacksonObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)

    /**
     * Execute a SQL statement against the Databricks SQL warehouse.
     *
     * @param statement SQL statement to execute.
     * @return The execution result.
     * @throws SqlExecutionException if the statement fails or times out.
     */
    fun executeStatement(statement: String): StatementResult {
        val url = "${workspaceUrl.trimEnd('/')}/api/2.0/sql/statements"

        val requestBody =
            mapper.writeValueAsString(
                mapOf(
                    "warehouse_id" to warehouseId,
                    "statement" to statement,
                    "wait_timeout" to "30s",
                    "disposition" to "INLINE",
                    "format" to "JSON_ARRAY",
                ),
            )

        log.debug("Executing SQL: warehouse={}, statement={}", warehouseId, statement)

        val request =
            HttpRequest
                .newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer ${tokenSupplier()}")
                .timeout(requestTimeout)
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build()

        val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())

        if (response.statusCode() !in 200..299) {
            throw SqlExecutionException(
                "SQL Statement API returned ${response.statusCode()}: ${response.body()}",
                statement,
            )
        }

        val result = mapper.readValue<StatementResponse>(response.body())

        return when (result.status.state) {
            "SUCCEEDED" -> {
                log.debug("SQL succeeded: statement_id={}", result.statementId)
                StatementResult(
                    statementId = result.statementId,
                    state = StatementState.SUCCEEDED,
                )
            }
            "FAILED" -> {
                val errorMessage = result.status.error?.message ?: "Unknown error"
                log.warn("SQL failed: statement_id={}, error={}", result.statementId, errorMessage)
                throw SqlExecutionException(
                    "SQL statement failed: $errorMessage",
                    statement,
                )
            }
            "CANCELED" -> {
                throw SqlExecutionException("SQL statement was canceled", statement)
            }
            else -> {
                // PENDING, RUNNING - shouldn't happen with wait_timeout
                log.warn(
                    "SQL statement did not complete within timeout: state={}",
                    result.status.state,
                )
                throw SqlExecutionException(
                    "SQL statement timed out in state: ${result.status.state}",
                    statement,
                )
            }
        }
    }

    /**
     * Register a Delta table as an external table in Unity Catalog.
     *
     * Uses `CREATE TABLE IF NOT EXISTS` so it's idempotent.
     *
     * @param catalog Unity Catalog name.
     * @param schema Unity Catalog schema name.
     * @param tableName Table name.
     * @param location Storage location (e.g. `abfss://container@account.dfs.core.windows.net/path/table`).
     * @return The execution result.
     */
    fun registerTable(
        catalog: String,
        schema: String,
        tableName: String,
        location: String,
    ): StatementResult {
        val fqn = "`$catalog`.`$schema`.`$tableName`"
        val sql = "CREATE TABLE IF NOT EXISTS $fqn USING DELTA LOCATION '$location'"
        log.info("Registering table: fqn={}, location={}", fqn, location)
        return executeStatement(sql)
    }

    /**
     * Refresh Unity Catalog metadata for a Delta table.
     *
     * Forces UC to reread the Delta transaction log for the latest
     * schema and file metadata.
     *
     * @param catalog Unity Catalog name.
     * @param schema Unity Catalog schema name.
     * @param tableName Table name.
     * @return The execution result.
     */
    fun refreshTable(
        catalog: String,
        schema: String,
        tableName: String,
    ): StatementResult {
        val fqn = "`$catalog`.`$schema`.`$tableName`"
        val sql = "MSCK REPAIR TABLE $fqn SYNC METADATA"
        log.info("Refreshing table metadata: fqn={}", fqn)
        return executeStatement(sql)
    }
}

/** Result of a SQL statement execution. */
data class StatementResult(
    val statementId: String,
    val state: StatementState,
)

enum class StatementState {
    SUCCEEDED,
    FAILED,
    CANCELED,
    PENDING,
    RUNNING,
}

/** Exception thrown when a SQL statement execution fails. */
class SqlExecutionException(
    message: String,
    val statement: String,
    cause: Throwable? = null,
) : RuntimeException(message, cause)

internal data class StatementResponse(
    val statementId: String = "",
    val status: StatementStatus = StatementStatus(),
) {
    internal data class StatementStatus(
        val state: String = "",
        val error: StatementError? = null,
    )

    internal data class StatementError(
        val errorCode: String = "",
        val message: String = "",
    )
}
