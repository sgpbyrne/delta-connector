package com.deltaconnect.catalog

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.sun.net.httpserver.HttpServer
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.net.InetSocketAddress

class DatabricksSqlClientTest {

    private lateinit var server: HttpServer
    private lateinit var client: DatabricksSqlClient
    private val mapper = jacksonObjectMapper()
    private var lastRequestBody: String? = null
    private var lastAuthHeader: String? = null

    @BeforeEach
    fun setUp() {
        server = HttpServer.create(InetSocketAddress(0), 0)
        val port = server.address.port

        client = DatabricksSqlClient(
            workspaceUrl = "http://localhost:$port",
            warehouseId = "test-warehouse-id",
            tokenSupplier = { "test-token-123" }
        )
    }

    @AfterEach
    fun tearDown() {
        server.stop(0)
    }

    @Nested
    inner class ExecuteStatement {

        @Test
        fun `sends correct request and parses success response`() {
            server.createContext("/api/2.0/sql/statements") { exchange ->
                lastRequestBody = exchange.requestBody.bufferedReader().readText()
                lastAuthHeader = exchange.requestHeaders.getFirst("Authorization")

                val response = """
                    {
                        "statement_id": "stmt-001",
                        "status": { "state": "SUCCEEDED" }
                    }
                """.trimIndent()
                exchange.sendResponseHeaders(200, response.toByteArray().size.toLong())
                exchange.responseBody.use { it.write(response.toByteArray()) }
            }
            server.start()

            val result = client.executeStatement("SELECT 1")

            result.state shouldBe StatementState.SUCCEEDED
            result.statementId shouldBe "stmt-001"
            lastAuthHeader shouldBe "Bearer test-token-123"

            val body = mapper.readTree(lastRequestBody)
            body.get("warehouse_id").asText() shouldBe "test-warehouse-id"
            body.get("statement").asText() shouldBe "SELECT 1"
        }

        @Test
        fun `throws on FAILED state`() {
            server.createContext("/api/2.0/sql/statements") { exchange ->
                val response = """
                    {
                        "statement_id": "stmt-002",
                        "status": {
                            "state": "FAILED",
                            "error": {
                                "error_code": "SCHEMA_NOT_FOUND",
                                "message": "Schema 'test_schema' not found"
                            }
                        }
                    }
                """.trimIndent()
                exchange.sendResponseHeaders(200, response.toByteArray().size.toLong())
                exchange.responseBody.use { it.write(response.toByteArray()) }
            }
            server.start()

            val ex = assertThrows<SqlExecutionException> {
                client.executeStatement("CREATE TABLE t LOCATION 'x'")
            }
            ex.message shouldContain "Schema 'test_schema' not found"
        }

        @Test
        fun `throws on HTTP error status`() {
            server.createContext("/api/2.0/sql/statements") { exchange ->
                val response = "Unauthorized"
                exchange.sendResponseHeaders(401, response.toByteArray().size.toLong())
                exchange.responseBody.use { it.write(response.toByteArray()) }
            }
            server.start()

            val ex = assertThrows<SqlExecutionException> {
                client.executeStatement("SELECT 1")
            }
            ex.message shouldContain "401"
        }
    }

    @Nested
    inner class RegisterTable {

        @Test
        fun `generates correct CREATE TABLE statement`() {
            server.createContext("/api/2.0/sql/statements") { exchange ->
                lastRequestBody = exchange.requestBody.bufferedReader().readText()
                val response = """
                    { "statement_id": "stmt-003", "status": { "state": "SUCCEEDED" } }
                """.trimIndent()
                exchange.sendResponseHeaders(200, response.toByteArray().size.toLong())
                exchange.responseBody.use { it.write(response.toByteArray()) }
            }
            server.start()

            client.registerTable(
                catalog = "my_catalog",
                schema = "my_schema",
                tableName = "orders",
                location = "abfss://container@account.dfs.core.windows.net/delta/orders"
            )

            val body = mapper.readTree(lastRequestBody)
            val sql = body.get("statement").asText()
            sql shouldContain "CREATE TABLE IF NOT EXISTS"
            sql shouldContain "`my_catalog`.`my_schema`.`orders`"
            sql shouldContain "USING DELTA"
            sql shouldContain "LOCATION 'abfss://container@account.dfs.core.windows.net/delta/orders'"
        }
    }

    @Nested
    inner class RefreshTable {

        @Test
        fun `generates correct REPAIR TABLE statement`() {
            server.createContext("/api/2.0/sql/statements") { exchange ->
                lastRequestBody = exchange.requestBody.bufferedReader().readText()
                val response = """
                    { "statement_id": "stmt-004", "status": { "state": "SUCCEEDED" } }
                """.trimIndent()
                exchange.sendResponseHeaders(200, response.toByteArray().size.toLong())
                exchange.responseBody.use { it.write(response.toByteArray()) }
            }
            server.start()

            client.refreshTable(
                catalog = "my_catalog",
                schema = "my_schema",
                tableName = "orders"
            )

            val body = mapper.readTree(lastRequestBody)
            val sql = body.get("statement").asText()
            sql shouldContain "MSCK REPAIR TABLE"
            sql shouldContain "`my_catalog`.`my_schema`.`orders`"
            sql shouldContain "SYNC METADATA"
        }
    }
}
