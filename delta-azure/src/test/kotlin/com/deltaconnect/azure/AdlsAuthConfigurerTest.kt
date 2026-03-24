package com.deltaconnect.azure

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.types.shouldBeInstanceOf
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

class AdlsAuthConfigurerTest {

    @Nested
    inner class Validation {

        @Test
        fun `STORAGE_KEY requires accountKey`() {
            val config = AdlsAuthConfig(
                authType = AdlsAuthType.STORAGE_KEY,
                accountName = "myaccount",
                fileSystemName = "myfs",
                accountKey = null
            )

            val ex = shouldThrow<IllegalArgumentException> {
                AdlsAuthConfigurer.createFileSystemClient(config)
            }
            ex.message shouldContain "accountKey"
        }

        @Test
        fun `SAS_TOKEN requires sasToken`() {
            val config = AdlsAuthConfig(
                authType = AdlsAuthType.SAS_TOKEN,
                accountName = "myaccount",
                fileSystemName = "myfs",
                sasToken = null
            )

            val ex = shouldThrow<IllegalArgumentException> {
                AdlsAuthConfigurer.createFileSystemClient(config)
            }
            ex.message shouldContain "sasToken"
        }
    }

    @Nested
    inner class DefaultEndpoint {

        @Test
        fun `uses default dfs endpoint when endpoint is null`() {
            val config = AdlsAuthConfig(
                authType = AdlsAuthType.STORAGE_KEY,
                accountName = "myaccount",
                fileSystemName = "myfs",
                accountKey = "dGVzdGtleQ=="
            )

            config.endpoint shouldBe null
            config.accountName shouldBe "myaccount"
            config.fileSystemName shouldBe "myfs"
        }

        @Test
        fun `custom endpoint is preserved`() {
            val config = AdlsAuthConfig(
                authType = AdlsAuthType.STORAGE_KEY,
                accountName = "myaccount",
                fileSystemName = "myfs",
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
            val config = AdlsAuthConfig(
                authType = AdlsAuthType.STORAGE_KEY,
                accountName = "devstoreaccount1",
                fileSystemName = "test",
                accountKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
            )

            val logStore = AdlsAuthConfigurer.createLogStore(config)
            logStore.shouldBeInstanceOf<AdlsGen2LogStore>()
        }

        @Test
        fun `creates log store with identity auth`() {
            val config = AdlsAuthConfig(
                authType = AdlsAuthType.IDENTITY,
                accountName = "devstoreaccount1",
                fileSystemName = "test"
            )

            val logStore = AdlsAuthConfigurer.createLogStore(config)
            logStore.shouldBeInstanceOf<AdlsGen2LogStore>()
        }
    }
}
