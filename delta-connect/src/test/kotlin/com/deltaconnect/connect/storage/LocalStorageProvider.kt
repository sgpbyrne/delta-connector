package com.deltaconnect.connect.storage

import com.deltaconnect.protocol.storage.DeltaLogStore
import com.deltaconnect.protocol.storage.LocalFileSystemLogStore
import org.apache.kafka.common.config.ConfigDef
import java.nio.file.Path

/**
 * [StorageProvider] for local filesystem storage (testing only).
 *
 * Handles `file://` URIs and plain filesystem paths. Creates a
 * [LocalFileSystemLogStore] backed by the configured directory.
 *
 * @param basePath The root directory for the local file system store.
 */
class LocalStorageProvider(
    private val basePath: Path,
) : StorageProvider {
    override fun schemes(): Set<String> = setOf("file")

    override fun defineConfig(configDef: ConfigDef): ConfigDef = configDef

    override fun validate(props: Map<String, String>): List<String> = emptyList()

    override fun createLogStore(props: Map<String, String>): DeltaLogStore = LocalFileSystemLogStore(basePath)

    override fun parseBasePath(storagePath: String): String =
        if (storagePath.startsWith("file://")) {
            storagePath.removePrefix("file://").trimEnd('/')
        } else {
            storagePath.trimEnd('/')
        }
}
