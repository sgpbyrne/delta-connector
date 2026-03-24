package com.deltaconnect.azure

import com.deltaconnect.connect.storage.StorageProvider
import com.deltaconnect.protocol.storage.DeltaLogStore
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Type
import org.apache.kafka.common.config.ConfigDef.Width
import java.net.URI

/**
 * [StorageProvider] for Azure ADLS Gen2 storage.
 *
 * Handles `abfss://` URI scheme. Contributes Azure-specific config keys
 * (`azure.auth.type`, `azure.storage.account.name`, etc.) and creates
 * an [AdlsGen2LogStore] via [AdlsAuthConfigurer].
 */
class AzureStorageProvider : StorageProvider {

    override fun schemes(): Set<String> = setOf("abfss")

    override fun defineConfig(configDef: ConfigDef): ConfigDef {
        return configDef
            .define(
                AZURE_AUTH_TYPE, Type.STRING, AZURE_AUTH_TYPE_DEFAULT,
                ConfigDef.ValidString.`in`("storage_key", "sas_token", "identity"),
                Importance.HIGH, AZURE_AUTH_TYPE_DOC,
                AZURE_GROUP, 1, Width.MEDIUM, "Auth Type"
            )
            .define(
                AZURE_STORAGE_ACCOUNT_NAME, Type.STRING, "",
                Importance.HIGH, AZURE_STORAGE_ACCOUNT_NAME_DOC,
                AZURE_GROUP, 2, Width.LONG, "Storage Account Name"
            )
            .define(
                AZURE_STORAGE_ACCOUNT_KEY, Type.PASSWORD, "",
                Importance.HIGH, AZURE_STORAGE_ACCOUNT_KEY_DOC,
                AZURE_GROUP, 3, Width.LONG, "Storage Account Key"
            )
            .define(
                AZURE_SAS_TOKEN, Type.PASSWORD, "",
                Importance.HIGH, AZURE_SAS_TOKEN_DOC,
                AZURE_GROUP, 4, Width.LONG, "SAS Token"
            )
    }

    override fun validate(props: Map<String, String>): List<String> {
        val errors = mutableListOf<String>()

        val authType = props[AZURE_AUTH_TYPE] ?: AZURE_AUTH_TYPE_DEFAULT
        val accountName = props[AZURE_STORAGE_ACCOUNT_NAME] ?: ""
        val accountKey = props[AZURE_STORAGE_ACCOUNT_KEY] ?: ""
        val sasToken = props[AZURE_SAS_TOKEN] ?: ""

        if (accountName.isBlank()) {
            errors.add("$AZURE_STORAGE_ACCOUNT_NAME is required for Azure storage")
        }

        when (authType) {
            "storage_key" -> {
                if (accountKey.isBlank()) {
                    errors.add(
                        "$AZURE_STORAGE_ACCOUNT_KEY is required " +
                            "when $AZURE_AUTH_TYPE=storage_key"
                    )
                }
            }
            "sas_token" -> {
                if (sasToken.isBlank()) {
                    errors.add(
                        "$AZURE_SAS_TOKEN is required " +
                            "when $AZURE_AUTH_TYPE=sas_token"
                    )
                }
            }
            "identity" -> {
                // Uses DefaultAzureCredential. No checks required.
            }
        }

        return errors
    }

    override fun createLogStore(props: Map<String, String>): DeltaLogStore {
        val storagePath = props["delta.storage.path"]
            ?: throw IllegalArgumentException("delta.storage.path is required")

        val container = parseContainer(storagePath)
        val authType = props[AZURE_AUTH_TYPE] ?: AZURE_AUTH_TYPE_DEFAULT
        val accountName = props[AZURE_STORAGE_ACCOUNT_NAME] ?: ""
        val accountKey = props[AZURE_STORAGE_ACCOUNT_KEY] ?: ""
        val sasToken = props[AZURE_SAS_TOKEN] ?: ""

        val authConfig = AdlsAuthConfig(
            authType = when (authType) {
                "storage_key" -> AdlsAuthType.STORAGE_KEY
                "sas_token" -> AdlsAuthType.SAS_TOKEN
                "identity" -> AdlsAuthType.IDENTITY
                else -> AdlsAuthType.IDENTITY
            },
            accountName = accountName,
            fileSystemName = container,
            accountKey = accountKey.ifBlank { null },
            sasToken = sasToken.ifBlank { null }
        )

        return AdlsAuthConfigurer.createLogStore(authConfig)
    }

    override fun parseBasePath(storagePath: String): String {
        require(storagePath.startsWith("abfss://")) {
            "AzureStorageProvider requires abfss:// scheme, got: $storagePath"
        }
        val uri = URI(storagePath)
        return uri.path.trimStart('/').trimEnd('/')
    }

    companion object {
        private const val AZURE_GROUP = "Azure"

        const val AZURE_AUTH_TYPE = "azure.auth.type"
        private const val AZURE_AUTH_TYPE_DOC =
            "Authentication type: 'storage_key' (account key in config), " +
                "'sas_token' (SAS token in config), or 'identity' " +
                "(Uses DefaultAzureCredential chain: env vars, workload identity, managed identity)."
        private const val AZURE_AUTH_TYPE_DEFAULT = "identity"

        const val AZURE_STORAGE_ACCOUNT_NAME = "azure.storage.account.name"
        private const val AZURE_STORAGE_ACCOUNT_NAME_DOC =
            "Azure storage account name."

        const val AZURE_STORAGE_ACCOUNT_KEY = "azure.storage.account.key"
        private const val AZURE_STORAGE_ACCOUNT_KEY_DOC =
            "Azure storage account access key. Required when azure.auth.type=storage_key."

        const val AZURE_SAS_TOKEN = "azure.sas.token"
        private const val AZURE_SAS_TOKEN_DOC =
            "Azure SAS token. Required when azure.auth.type=sas_token."

        /**
         * Parse the container (filesystem) name from an abfss URI.
         */
        internal fun parseContainer(storagePath: String): String {
            require(storagePath.startsWith("abfss://")) {
                "Storage path must use abfss:// scheme for Azure, got: $storagePath"
            }
            val uri = URI(storagePath)
            return uri.authority.substringBefore('@')
        }
    }
}
