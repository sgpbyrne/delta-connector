package com.deltaconnect.connect.storage

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigException
import org.slf4j.LoggerFactory
import java.net.URI
import java.util.ServiceLoader

/**
 * Registry that discovers and resolves [StorageProvider] instances.
 *
 * Providers are discovered via [ServiceLoader] on first access and can also
 * be registered programmatically.
 *
 * Resolution works by matching the URI scheme of the storage path to a
 * provider's declared [StorageProvider.schemes].
 */
object StorageProviderRegistry {

    private val log = LoggerFactory.getLogger(StorageProviderRegistry::class.java)

    private val providers = mutableMapOf<String, StorageProvider>()
    private var serviceLoaderInitialized = false

    /**
     * Register a [StorageProvider] for its declared schemes.
     *
     * If a scheme is already registered, the new provider overwrites the
     * previous one.
     */
    fun register(provider: StorageProvider) {
        for (scheme in provider.schemes()) {
            providers[scheme.lowercase()] = provider
            log.debug("Registered StorageProvider: scheme={}, class={}", scheme, provider::class.qualifiedName)
        }
    }

    /**
     * Remove all registered providers. Primarily used in tests.
     */
    fun clear() {
        providers.clear()
        serviceLoaderInitialized = false
    }

    /**
     * Resolve the [StorageProvider] for the given storage path.
     *
     * Extracts the URI scheme and looks up the matching provider. Triggers
     * ServiceLoader discovery on first call if no providers are registered yet.
     *
     * @param storagePath The storage path (e.g. `abfss://container@account/path`).
     * @return The matching StorageProvider.
     * @throws ConfigException if no provider matches the scheme.
     */
    fun resolve(storagePath: String): StorageProvider {
        ensureServiceLoaderInit()

        val scheme = extractScheme(storagePath)
        return providers[scheme]
            ?: throw ConfigException(
                "No StorageProvider found for scheme '$scheme' " +
                    "(storage path: $storagePath). " +
                    "Available schemes: ${providers.keys.sorted()}. " +
                    "Ensure the appropriate storage backend module " +
                    "(e.g. delta-azure) is on the classpath."
            )
    }

    /**
     * Return all currently registered providers (after ServiceLoader init).
     *
     * Used to build the composite ConfigDef with all provider-contributed keys.
     */
    fun allProviders(): Collection<StorageProvider> {
        ensureServiceLoaderInit()
        return providers.values.toSet()
    }

    private fun ensureServiceLoaderInit() {
        if (serviceLoaderInitialized) return
        serviceLoaderInitialized = true

        val loader = ServiceLoader.load(StorageProvider::class.java)
        for (provider in loader) {
            register(provider)
            log.info(
                "Discovered StorageProvider via ServiceLoader: schemes={}, class={}",
                provider.schemes(), provider::class.qualifiedName
            )
        }
    }

    /**
     * Extract the URI scheme from a storage path.
     *
     * Handles standard URIs (e.g. `abfss://...`) and plain paths (returns "file").
     */
    internal fun extractScheme(storagePath: String): String {
        return try {
            val uri = URI(storagePath)
            uri.scheme?.lowercase() ?: "file"
        } catch (_: Exception) {
            "file"
        }
    }
}
