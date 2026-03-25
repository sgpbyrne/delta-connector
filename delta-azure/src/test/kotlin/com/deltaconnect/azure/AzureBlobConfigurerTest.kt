package com.deltaconnect.azure

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.types.shouldBeInstanceOf
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

class AzureBlobConfigurerTest {

    @Nested
    inner class Validation {

        @Test
        fun `STORAGE_KEY requires accountKey`() {
            val config = AzureBlobConfig(
                authType = AzureAuthType.STORAGE_KEY,
                accountName = "myaccount",
                containerName = "mycontainer",
                accountKey = null
            )

            val ex = shouldThrow<IllegalArgumentException> {
                AzureBlobConfigurer.createContainerClient(config)
            }
            ex.message shouldContain "accountKey"
        }

        @Test
        fun `SAS_TOKEN requires sasToken`() {
            val config = AzureBlobConfig(
                authType = AzureAuthType.SAS_TOKEN,
                accountName = "myaccount",
                containerName = "mycontainer",
                sasToken = null
            )

            val ex = shouldThrow<IllegalArgumentException> {
                AzureBlobConfigurer.createContainerClient(config)
            }
            ex.message shouldContain "sasToken"
        }
    }

    @Nested
    inner class DefaultEndpoint {

        @Test
        fun `uses default blob endpoint when endpoint is null`() {
            val config = AzureBlobConfig(
                authType = AzureAuthType.STORAGE_KEY,
                accountName = "myaccount",
                containerName = "mycontainer",
                accountKey = "dGVzdGtleQ=="
            )

            config.endpoint shouldBe null
            config.accountName shouldBe "myaccount"
            config.containerName shouldBe "mycontainer"
        }

        @Test
        fun `custom endpoint is preserved`() {
            val config = AzureBlobConfig(
                authType = AzureAuthType.STORAGE_KEY,
                accountName = "myaccount",
                containerName = "mycontainer",
                accountKey = "dGVzdGtleQ==",
                endpoint = "https://custom.endpoint.com"
            )

            config.endpoint shouldBe "https://custom.endpoint.com"
        }
    }

    @Nested
    inner class CreateLogStore {

        @Test
        fun `creates log store with storage key auth`() {
            val config = AzureBlobConfig(
                authType = AzureAuthType.STORAGE_KEY,
                accountName = "devstoreaccount1",
                containerName = "test",
                accountKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
            )

            val logStore = AzureBlobConfigurer.createLogStore(config)
            logStore.shouldBeInstanceOf<AzureBlobLogStore>()
        }

        @Test
        fun `creates log store with identity auth`() {
            val config = AzureBlobConfig(
                authType = AzureAuthType.IDENTITY,
                accountName = "devstoreaccount1",
                containerName = "test"
            )

            val logStore = AzureBlobConfigurer.createLogStore(config)
            logStore.shouldBeInstanceOf<AzureBlobLogStore>()
        }
    }
}
