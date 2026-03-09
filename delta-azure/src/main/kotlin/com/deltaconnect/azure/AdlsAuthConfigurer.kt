package com.deltaconnect.azure

import com.azure.identity.DefaultAzureCredentialBuilder
import com.azure.storage.common.StorageSharedKeyCredential
import com.azure.storage.file.datalake.DataLakeFileSystemClient
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder

/**
 * Authentication type for ADLS Gen2 access.
 */
enum class AdlsAuthType {
    /** Storage account name + key. */
    STORAGE_KEY,
    /** Shared Access Signature token. */
    SAS_TOKEN,
    /** Azure Identity DefaultAzureCredential. */
    DEFAULT_CREDENTIAL
}

/**
 * Configuration for connecting to an ADLS Gen2 storage account.
 *
 * @param authType Authentication method.
 * @param accountName Azure storage account name.
 * @param fileSystemName ADLS Gen2 filesystem (container) name.
 * @param accountKey Storage account key (required for [AdlsAuthType.STORAGE_KEY]).
 * @param sasToken SAS token (required for [AdlsAuthType.SAS_TOKEN]).
 * @param endpoint Override endpoint URL. If null, defaults to `https://{accountName}.dfs.core.windows.net`.
 */
data class AdlsAuthConfig(
    val authType: AdlsAuthType,
    val accountName: String,
    val fileSystemName: String,
    val accountKey: String? = null,
    val sasToken: String? = null,
    val endpoint: String? = null
)

/**
 * Builds an [AdlsGen2LogStore] from an [AdlsAuthConfig].
 */
object AdlsAuthConfigurer {

    /**
     * Create a [DataLakeFileSystemClient] from the given configuration.
     *
     * @param config Authentication and connection configuration.
     * @return Configured filesystem client ready for use with [AdlsGen2LogStore].
     * @throws IllegalArgumentException if required credentials are missing for the auth type.
     */
    fun createFileSystemClient(config: AdlsAuthConfig): DataLakeFileSystemClient {
        val endpoint = config.endpoint
            ?: "https://${config.accountName}.dfs.core.windows.net"

        val builder = DataLakeServiceClientBuilder().endpoint(endpoint)

        when (config.authType) {
            AdlsAuthType.STORAGE_KEY -> {
                requireNotNull(config.accountKey) {
                    "accountKey is required for STORAGE_KEY auth type"
                }
                val credential = StorageSharedKeyCredential(config.accountName, config.accountKey)
                builder.credential(credential)
            }
            AdlsAuthType.SAS_TOKEN -> {
                requireNotNull(config.sasToken) {
                    "sasToken is required for SAS_TOKEN auth type"
                }
                builder.sasToken(config.sasToken)
            }
            AdlsAuthType.DEFAULT_CREDENTIAL -> {
                builder.credential(DefaultAzureCredentialBuilder().build())
            }
        }

        return builder.buildClient().getFileSystemClient(config.fileSystemName)
    }

    /**
     * Create an [AdlsGen2LogStore] from the given configuration.
     *
     * @param config Authentication and connection configuration.
     * @return Configured log store.
     */
    fun createLogStore(config: AdlsAuthConfig): AdlsGen2LogStore {
        val fileSystemClient = createFileSystemClient(config)
        return AdlsGen2LogStore(fileSystemClient)
    }
}
