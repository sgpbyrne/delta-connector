package com.deltaconnect.connect

import com.deltaconnect.connect.storage.LocalStorageProvider
import com.deltaconnect.connect.storage.StorageProviderRegistry
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import org.apache.kafka.common.config.ConfigException
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path

class DeltaSinkConnectorTest {

    @TempDir
    lateinit var tempDir: Path

    private lateinit var connector: DeltaSinkConnector

    @BeforeEach
    fun setUp() {
        StorageProviderRegistry.clear()
        StorageProviderRegistry.register(LocalStorageProvider(tempDir))
        connector = DeltaSinkConnector()
    }

    @AfterEach
    fun tearDown() {
        StorageProviderRegistry.clear()
    }

    private fun baseProps(): MutableMap<String, String> = mutableMapOf(
        DeltaSinkConfig.DELTA_STORAGE_PATH to tempDir.toString(),
        DeltaSinkConfig.DELTA_MERGE_KEYS to "id",
        DeltaSinkConfig.DELTA_WRITE_MODE to "cdc"
    )

    @Test
    fun `version returns connector version`() {
        connector.version() shouldBe DeltaSinkConnector.VERSION
    }

    @Test
    fun `start succeeds with valid config`() {
        connector.start(baseProps())
    }

    @Test
    fun `start fails with missing required config`() {
        val props = baseProps().apply { remove(DeltaSinkConfig.DELTA_STORAGE_PATH) }
        assertThrows<ConfigException> { connector.start(props) }
    }

    @Test
    fun `taskClass returns DeltaSinkTask`() {
        connector.taskClass() shouldBe DeltaSinkTask::class.java
    }

    @Test
    fun `taskConfigs returns correct number of configs`() {
        connector.start(baseProps())
        connector.taskConfigs(3) shouldHaveSize 3
    }

    @Test
    fun `taskConfigs returns same props for each task`() {
        val props = baseProps()
        connector.start(props)
        val configs = connector.taskConfigs(2)
        configs[0] shouldBe configs[1]
    }

    @Test
    fun `config returns CONFIG_DEF`() {
        connector.config() shouldBe DeltaSinkConfig.CONFIG_DEF
    }

    @Test
    fun `validate returns no errors for valid config`() {
        val config = connector.validate(baseProps())
        val allErrors = config.configValues().flatMap { it.errorMessages() }
        allErrors shouldHaveSize 0
    }

    @Test
    fun `validate reports cross-property errors`() {
        val props = baseProps().apply {
            put(DeltaSinkConfig.DELTA_WRITE_MODE, "cdc")
            put(DeltaSinkConfig.DELTA_MERGE_KEYS, "")
        }
        val config = connector.validate(props)
        val allErrors = config.configValues().flatMap { it.errorMessages() }
        allErrors.any { it.contains(DeltaSinkConfig.DELTA_MERGE_KEYS) } shouldBe true
    }

    @Test
    fun `validate reports unity catalog errors when enabled`() {
        val props = baseProps().apply {
            put(DeltaSinkConfig.UNITY_CATALOG_ENABLED, "true")
        }
        val config = connector.validate(props)
        val allErrors = config.configValues().flatMap { it.errorMessages() }
        allErrors.any { it.contains(DeltaSinkConfig.UNITY_CATALOG_WORKSPACE_URL) } shouldBe true
    }

    @Test
    fun `stop completes without error`() {
        connector.start(baseProps())
        connector.stop()
    }
}
