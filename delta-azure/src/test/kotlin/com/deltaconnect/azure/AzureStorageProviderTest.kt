package com.deltaconnect.azure

import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import org.apache.kafka.common.config.ConfigDef
import org.junit.jupiter.api.Test

class AzureStorageProviderTest {

    private val provider = AzureStorageProvider()

    @Test
    fun `schemes returns abfss`() {
        provider.schemes() shouldBe setOf("abfss")
    }

    @Test
    fun `defineConfig adds Azure config keys`() {
        val configDef = ConfigDef()
        provider.defineConfig(configDef)

        configDef.configKeys().containsKey(AzureStorageProvider.AZURE_AUTH_TYPE) shouldBe true
        configDef.configKeys().containsKey(AzureStorageProvider.AZURE_STORAGE_ACCOUNT_NAME) shouldBe true
        configDef.configKeys().containsKey(AzureStorageProvider.AZURE_STORAGE_ACCOUNT_KEY) shouldBe true
        configDef.configKeys().containsKey(AzureStorageProvider.AZURE_SAS_TOKEN) shouldBe true
    }

    @Test
    fun `validate passes with valid identity config`() {
        val props = mapOf(
            AzureStorageProvider.AZURE_AUTH_TYPE to "identity",
            AzureStorageProvider.AZURE_STORAGE_ACCOUNT_NAME to "myaccount"
        )
        provider.validate(props).shouldBeEmpty()
    }

    @Test
    fun `validate passes with valid storage_key config`() {
        val props = mapOf(
            AzureStorageProvider.AZURE_AUTH_TYPE to "storage_key",
            AzureStorageProvider.AZURE_STORAGE_ACCOUNT_NAME to "myaccount",
            AzureStorageProvider.AZURE_STORAGE_ACCOUNT_KEY to "dGVzdGtleQ=="
        )
        provider.validate(props).shouldBeEmpty()
    }

    @Test
    fun `validate passes with valid sas_token config`() {
        val props = mapOf(
            AzureStorageProvider.AZURE_AUTH_TYPE to "sas_token",
            AzureStorageProvider.AZURE_STORAGE_ACCOUNT_NAME to "myaccount",
            AzureStorageProvider.AZURE_SAS_TOKEN to "sv=2021-06-08&ss=bfqt&srt=sco"
        )
        provider.validate(props).shouldBeEmpty()
    }

    @Test
    fun `validate requires account name`() {
        val props = mapOf(
            AzureStorageProvider.AZURE_AUTH_TYPE to "identity",
            AzureStorageProvider.AZURE_STORAGE_ACCOUNT_NAME to ""
        )
        val errors = provider.validate(props)
        errors shouldHaveSize 1
        errors[0] shouldContain AzureStorageProvider.AZURE_STORAGE_ACCOUNT_NAME
    }

    @Test
    fun `validate requires account key for storage_key auth`() {
        val props = mapOf(
            AzureStorageProvider.AZURE_AUTH_TYPE to "storage_key",
            AzureStorageProvider.AZURE_STORAGE_ACCOUNT_NAME to "myaccount",
            AzureStorageProvider.AZURE_STORAGE_ACCOUNT_KEY to ""
        )
        val errors = provider.validate(props)
        errors shouldHaveSize 1
        errors[0] shouldContain AzureStorageProvider.AZURE_STORAGE_ACCOUNT_KEY
    }

    @Test
    fun `validate requires sas token for sas_token auth`() {
        val props = mapOf(
            AzureStorageProvider.AZURE_AUTH_TYPE to "sas_token",
            AzureStorageProvider.AZURE_STORAGE_ACCOUNT_NAME to "myaccount"
        )
        val errors = provider.validate(props)
        errors shouldHaveSize 1
        errors[0] shouldContain AzureStorageProvider.AZURE_SAS_TOKEN
    }

    @Test
    fun `validate reports multiple errors`() {
        val props = mapOf(
            AzureStorageProvider.AZURE_AUTH_TYPE to "storage_key",
            AzureStorageProvider.AZURE_STORAGE_ACCOUNT_NAME to ""
            // Missing account key AND account name
        )
        val errors = provider.validate(props)
        errors shouldHaveSize 2
    }

    @Test
    fun `parseBasePath extracts path from abfss URI`() {
        provider.parseBasePath(
            "abfss://container@account.dfs.core.windows.net/base/path"
        ) shouldBe "base/path"
    }

    @Test
    fun `parseBasePath handles empty path`() {
        provider.parseBasePath(
            "abfss://container@account.dfs.core.windows.net/"
        ) shouldBe ""
    }

    @Test
    fun `parseBasePath handles root path`() {
        provider.parseBasePath(
            "abfss://container@account.dfs.core.windows.net"
        ) shouldBe ""
    }

    @Test
    fun `parseContainer extracts container from abfss URI`() {
        AzureStorageProvider.parseContainer(
            "abfss://mycontainer@myaccount.dfs.core.windows.net/data"
        ) shouldBe "mycontainer"
    }
}
