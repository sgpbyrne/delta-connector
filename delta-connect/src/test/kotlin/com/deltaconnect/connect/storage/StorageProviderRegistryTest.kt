package com.deltaconnect.connect.storage

import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import org.apache.kafka.common.config.ConfigException
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path

class StorageProviderRegistryTest {
    @TempDir
    lateinit var tempDir: Path

    @BeforeEach
    fun setUp() {
        StorageProviderRegistry.clear()
    }

    @AfterEach
    fun tearDown() {
        StorageProviderRegistry.clear()
    }

    @Test
    fun `resolve finds registered provider by scheme`() {
        StorageProviderRegistry.register(LocalStorageProvider(tempDir))
        val provider = StorageProviderRegistry.resolve("file:///tmp/delta")
        provider.schemes() shouldBe setOf("file")
    }

    @Test
    fun `resolve finds provider for plain path via file scheme`() {
        StorageProviderRegistry.register(LocalStorageProvider(tempDir))
        val provider = StorageProviderRegistry.resolve("/tmp/delta")
        provider.schemes() shouldBe setOf("file")
    }

    @Test
    fun `resolve throws for unknown scheme`() {
        val ex =
            assertThrows<ConfigException> {
                StorageProviderRegistry.resolve("s3a://bucket/path")
            }
        ex.message shouldContain "No StorageProvider found for scheme 's3a'"
    }

    @Test
    fun `extractScheme extracts scheme from URI`() {
        StorageProviderRegistry.extractScheme("abfss://container@account.dfs.core.windows.net/path") shouldBe "abfss"
        StorageProviderRegistry.extractScheme("s3a://bucket/path") shouldBe "s3a"
        StorageProviderRegistry.extractScheme("gs://bucket/path") shouldBe "gs"
        StorageProviderRegistry.extractScheme("file:///tmp/delta") shouldBe "file"
    }

    @Test
    fun `extractScheme returns file for plain paths`() {
        StorageProviderRegistry.extractScheme("/tmp/delta") shouldBe "file"
        StorageProviderRegistry.extractScheme("relative/path") shouldBe "file"
    }

    @Test
    fun `register overwrites existing provider for same scheme`() {
        val provider1 = LocalStorageProvider(tempDir)
        val provider2 = LocalStorageProvider(tempDir)
        StorageProviderRegistry.register(provider1)
        StorageProviderRegistry.register(provider2)
        val resolved = StorageProviderRegistry.resolve("file:///tmp/delta")
        resolved shouldBe provider2
    }

    @Test
    fun `allProviders returns empty when nothing registered`() {
        StorageProviderRegistry.allProviders().size shouldBe 0
    }

    @Test
    fun `allProviders returns registered providers`() {
        StorageProviderRegistry.register(LocalStorageProvider(tempDir))
        StorageProviderRegistry.allProviders().size shouldBe 1
    }

    @Test
    fun `clear removes all providers`() {
        StorageProviderRegistry.register(LocalStorageProvider(tempDir))
        StorageProviderRegistry.clear()
        assertThrows<ConfigException> {
            StorageProviderRegistry.resolve("file:///tmp/delta")
        }
    }
}
