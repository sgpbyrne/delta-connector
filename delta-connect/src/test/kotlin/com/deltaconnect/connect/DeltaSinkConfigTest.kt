package com.deltaconnect.connect

import com.deltaconnect.connect.storage.LocalStorageProvider
import com.deltaconnect.connect.storage.StorageProviderRegistry
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import org.apache.kafka.common.config.ConfigException
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path

class DeltaSinkConfigTest {
    @TempDir
    lateinit var tempDir: Path

    @BeforeEach
    fun setUp() {
        StorageProviderRegistry.clear()
        StorageProviderRegistry.register(LocalStorageProvider(tempDir))
    }

    @AfterEach
    fun tearDown() {
        StorageProviderRegistry.clear()
    }

    private fun baseProps(): MutableMap<String, String> =
        mutableMapOf(
            DeltaSinkConfig.DELTA_STORAGE_PATH to tempDir.toString(),
            DeltaSinkConfig.DELTA_MERGE_KEYS to "id",
            DeltaSinkConfig.DELTA_WRITE_MODE to "cdc",
        )

    @Nested
    inner class RequiredFields {
        @Test
        fun `valid minimal config parses successfully`() {
            val config = DeltaSinkConfig(baseProps())
            config.storagePath shouldBe tempDir.toString()
            config.mergeKeys shouldContainExactly listOf("id")
            config.writeMode shouldBe DeltaSinkConfig.WriteMode.CDC
        }

        @Test
        fun `missing storage path throws ConfigException`() {
            val props = baseProps().apply { remove(DeltaSinkConfig.DELTA_STORAGE_PATH) }
            val ex = assertThrows<ConfigException> { DeltaSinkConfig(props) }
            ex.message shouldContain DeltaSinkConfig.DELTA_STORAGE_PATH
        }
    }

    @Nested
    inner class Defaults {
        @Test
        fun `default values are applied correctly`() {
            val config = DeltaSinkConfig(baseProps())
            config.tableName shouldBe "\${topic}"
            config.writeMode shouldBe DeltaSinkConfig.WriteMode.CDC
            config.mergeBatchSize shouldBe 50_000
            config.mergeIntervalMs shouldBe 60_000L
            config.mergeDeleteEnabled shouldBe true
            config.schemaEvolutionEnabled shouldBe true
            config.checkpointInterval shouldBe 10
            config.cdcEnvelopeFormat shouldBe DeltaSinkConfig.CdcEnvelopeFormat.DEBEZIUM_FULL
            config.cdcSourceDatabase shouldBe "sqlserver"
            config.unityCatalogEnabled shouldBe false
        }

        @Test
        fun `custom values override defaults`() {
            val props =
                baseProps().apply {
                    put(DeltaSinkConfig.DELTA_TABLE_NAME, "cdc_\${topic}")
                    put(DeltaSinkConfig.DELTA_WRITE_MODE, "append")
                    put(DeltaSinkConfig.DELTA_MERGE_BATCH_SIZE, "10000")
                    put(DeltaSinkConfig.DELTA_MERGE_INTERVAL_MS, "30000")
                    put(DeltaSinkConfig.DELTA_MERGE_DELETE_ENABLED, "false")
                    put(DeltaSinkConfig.DELTA_SCHEMA_EVOLUTION, "false")
                    put(DeltaSinkConfig.DELTA_CHECKPOINT_INTERVAL, "20")
                    put(DeltaSinkConfig.CDC_ENVELOPE_FORMAT, "debezium_flattened")
                }
            val config = DeltaSinkConfig(props)
            config.tableName shouldBe "cdc_\${topic}"
            config.writeMode shouldBe DeltaSinkConfig.WriteMode.APPEND
            config.mergeBatchSize shouldBe 10_000
            config.mergeIntervalMs shouldBe 30_000L
            config.mergeDeleteEnabled shouldBe false
            config.schemaEvolutionEnabled shouldBe false
            config.checkpointInterval shouldBe 20
            config.cdcEnvelopeFormat shouldBe DeltaSinkConfig.CdcEnvelopeFormat.DEBEZIUM_FLATTENED
        }
    }

    @Nested
    inner class EnumValidation {
        @Test
        fun `invalid write mode rejected`() {
            val props =
                baseProps().apply {
                    put(DeltaSinkConfig.DELTA_WRITE_MODE, "invalid")
                }
            assertThrows<ConfigException> { DeltaSinkConfig(props) }
        }

        @Test
        fun `invalid cdc envelope format rejected`() {
            val props =
                baseProps().apply {
                    put(DeltaSinkConfig.CDC_ENVELOPE_FORMAT, "avro")
                }
            assertThrows<ConfigException> { DeltaSinkConfig(props) }
        }

        @Test
        fun `invalid source database rejected`() {
            val props =
                baseProps().apply {
                    put(DeltaSinkConfig.CDC_SOURCE_DATABASE, "postgres")
                }
            assertThrows<ConfigException> { DeltaSinkConfig(props) }
        }

        @Test
        fun `all write modes are valid`() {
            for (mode in listOf("append", "upsert", "cdc")) {
                val props =
                    baseProps().apply {
                        put(DeltaSinkConfig.DELTA_WRITE_MODE, mode)
                    }
                DeltaSinkConfig(props).writeMode.value shouldBe mode
            }
        }
    }

    @Nested
    inner class RangeValidation {
        @Test
        fun `merge batch size must be at least 1`() {
            val props =
                baseProps().apply {
                    put(DeltaSinkConfig.DELTA_MERGE_BATCH_SIZE, "0")
                }
            assertThrows<ConfigException> { DeltaSinkConfig(props) }
        }

        @Test
        fun `merge interval must be at least 1000ms`() {
            val props =
                baseProps().apply {
                    put(DeltaSinkConfig.DELTA_MERGE_INTERVAL_MS, "500")
                }
            assertThrows<ConfigException> { DeltaSinkConfig(props) }
        }

        @Test
        fun `checkpoint interval must be at least 1`() {
            val props =
                baseProps().apply {
                    put(DeltaSinkConfig.DELTA_CHECKPOINT_INTERVAL, "0")
                }
            assertThrows<ConfigException> { DeltaSinkConfig(props) }
        }

        @Test
        fun `unity catalog sync interval must be at least 10000ms`() {
            val props =
                baseProps().apply {
                    put(DeltaSinkConfig.UNITY_CATALOG_SYNC_INTERVAL_MS, "5000")
                }
            assertThrows<ConfigException> { DeltaSinkConfig(props) }
        }
    }

    @Nested
    inner class CrossPropertyValidation {
        @Test
        fun `valid config has no cross-property errors`() {
            DeltaSinkConfig.validateCrossProperties(baseProps()).shouldBeEmpty()
        }

        @Test
        fun `merge keys required for cdc mode`() {
            val props =
                baseProps().apply {
                    put(DeltaSinkConfig.DELTA_MERGE_KEYS, "")
                    put(DeltaSinkConfig.DELTA_WRITE_MODE, "cdc")
                }
            val errors = DeltaSinkConfig.validateCrossProperties(props)
            errors.any { it.contains(DeltaSinkConfig.DELTA_MERGE_KEYS) } shouldBe true
        }

        @Test
        fun `merge keys not required for append mode`() {
            val props =
                baseProps().apply {
                    put(DeltaSinkConfig.DELTA_WRITE_MODE, "append")
                    put(DeltaSinkConfig.DELTA_MERGE_KEYS, "")
                }
            val errors = DeltaSinkConfig.validateCrossProperties(props)
            errors.none { it.contains(DeltaSinkConfig.DELTA_MERGE_KEYS) } shouldBe true
        }

        @Test
        fun `merge keys required for upsert mode`() {
            val props =
                baseProps().apply {
                    put(DeltaSinkConfig.DELTA_WRITE_MODE, "upsert")
                    put(DeltaSinkConfig.DELTA_MERGE_KEYS, "")
                }
            val errors = DeltaSinkConfig.validateCrossProperties(props)
            errors.any { it.contains(DeltaSinkConfig.DELTA_MERGE_KEYS) } shouldBe true
        }

        @Test
        fun `unity catalog enabled requires routing fields`() {
            val props =
                baseProps().apply {
                    put(DeltaSinkConfig.UNITY_CATALOG_ENABLED, "true")
                }
            val errors = DeltaSinkConfig.validateCrossProperties(props)
            errors.any { it.contains(DeltaSinkConfig.UNITY_CATALOG_WORKSPACE_URL) } shouldBe true
            errors.any { it.contains(DeltaSinkConfig.UNITY_CATALOG_TOKEN) } shouldBe true
            errors.any { it.contains(DeltaSinkConfig.UNITY_CATALOG_NAME) } shouldBe true
            errors.any { it.contains(DeltaSinkConfig.UNITY_CATALOG_SCHEMA) } shouldBe true
            errors.any { it.contains(DeltaSinkConfig.UNITY_CATALOG_WAREHOUSE_ID) } shouldBe true
            errors shouldHaveSize 5
        }

        @Test
        fun `unity catalog disabled skips UC validation`() {
            val props =
                baseProps().apply {
                    put(DeltaSinkConfig.UNITY_CATALOG_ENABLED, "false")
                }
            val errors = DeltaSinkConfig.validateCrossProperties(props)
            errors.none { it.contains("unity.catalog") } shouldBe true
        }
    }

    @Nested
    inner class MergeKeys {
        @Test
        fun `single merge key parsed`() {
            val config = DeltaSinkConfig(baseProps())
            config.mergeKeys shouldContainExactly listOf("id")
        }

        @Test
        fun `composite merge keys parsed`() {
            val props =
                baseProps().apply {
                    put(DeltaSinkConfig.DELTA_MERGE_KEYS, "tenant_id,customer_id")
                }
            val config = DeltaSinkConfig(props)
            config.mergeKeys shouldContainExactly listOf("tenant_id", "customer_id")
        }
    }

    @Nested
    inner class TableNameTemplate {
        @Test
        fun `resolves topic placeholder`() {
            DeltaSinkConfig.resolveTableName("\${topic}", "server1.dbo.orders") shouldBe
                "server1.dbo.orders"
        }

        @Test
        fun `resolves prefixed template`() {
            DeltaSinkConfig.resolveTableName("cdc_\${topic}", "orders") shouldBe "cdc_orders"
        }

        @Test
        fun `static name unchanged`() {
            DeltaSinkConfig.resolveTableName("my_table", "orders") shouldBe "my_table"
        }
    }
}
