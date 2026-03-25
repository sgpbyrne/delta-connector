package com.deltaconnect.azure

import com.azure.identity.DefaultAzureCredentialBuilder
import com.azure.storage.blob.BlobContainerClient
import com.azure.storage.blob.BlobServiceClientBuilder
import com.azure.storage.common.StorageSharedKeyCredential

/**
 * Authentication type for Azure Blob Storage access.
 */
enum class AzureAuthType {
    /** Storage account name + key. */
    STORAGE_KEY,

    /** Shared Access Signature token. */
    SAS_TOKEN,

    /** Azure Identity DefaultAzureCredential (managed identity, workload identity, service principal via env vars, etc.). */
    IDENTITY,
}

/**
 * Configuration for connecting to an Azure Blob Storage account.
 *
 * @param authType Authentication method.
 * @param accountName Azure storage account name.
 * @param containerName Blob container name.
 * @param accountKey Storage account key (required for [AzureAuthType.STORAGE_KEY]).
 * @param sasToken SAS token (required for [AzureAuthType.SAS_TOKEN]).
 * @param endpoint Override endpoint URL. If null, defaults to `https://{accountName}.blob.core.windows.net`.
 */
data class AzureBlobConfig(
    val authType: AzureAuthType,
    val accountName: String,
    val containerName: String,
    val accountKey: String? = null,
    val sasToken: String? = null,
    val endpoint: String? = null,
)

/**
 * Builds an [AzureBlobLogStore] from an [AzureBlobConfig].
 */
object AzureBlobConfigurer {
    /**
     * Create a [BlobContainerClient] from the given configuration.
     *
     * @param config Authentication and connection configuration.
     * @return Configured blob container client ready for use with [AzureBlobLogStore].
     * @throws IllegalArgumentException if required credentials are missing for the auth type.
     */
    fun createContainerClient(config: AzureBlobConfig): BlobContainerClient {
        val endpoint =
            config.endpoint
                ?: "https://${config.accountName}.blob.core.windows.net"

        val builder = BlobServiceClientBuilder().endpoint(endpoint)

        when (config.authType) {
            AzureAuthType.STORAGE_KEY -> {
                requireNotNull(config.accountKey) {
                    "accountKey is required for STORAGE_KEY auth type"
                }
                val credential = StorageSharedKeyCredential(config.accountName, config.accountKey)
                builder.credential(credential)
            }
            AzureAuthType.SAS_TOKEN -> {
                requireNotNull(config.sasToken) {
                    "sasToken is required for SAS_TOKEN auth type"
                }
                builder.sasToken(config.sasToken)
            }
            AzureAuthType.IDENTITY -> {
                builder.credential(DefaultAzureCredentialBuilder().build())
            }
        }

        return builder.buildClient().getBlobContainerClient(config.containerName)
    }

    /**
     * Create an [AzureBlobLogStore] from the given configuration.
     *
     * @param config Authentication and connection configuration.
     * @return Configured log store.
     */
    fun createLogStore(config: AzureBlobConfig): AzureBlobLogStore {
        val containerClient = createContainerClient(config)
        return AzureBlobLogStore(containerClient)
    }
}
