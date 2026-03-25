package com.deltaconnect.protocol.log

import com.deltaconnect.protocol.DeltaSnapshot
import com.deltaconnect.protocol.DeltaTable
import com.deltaconnect.protocol.actions.ActionSerializer
import com.deltaconnect.protocol.actions.AddFile
import com.deltaconnect.protocol.actions.CommitInfo
import com.deltaconnect.protocol.actions.Format
import com.deltaconnect.protocol.actions.MetaData
import com.deltaconnect.protocol.actions.Protocol
import com.deltaconnect.protocol.actions.RemoveFile
import com.deltaconnect.protocol.actions.SetTransaction
import com.deltaconnect.protocol.log.LogTestFixtures.addFile
import com.deltaconnect.protocol.log.LogTestFixtures.commitInfo
import com.deltaconnect.protocol.log.LogTestFixtures.metaData
import com.deltaconnect.protocol.log.LogTestFixtures.protocol
import com.deltaconnect.protocol.log.LogTestFixtures.removeFile
import com.deltaconnect.protocol.log.LogTestFixtures.setTransaction
import com.deltaconnect.protocol.log.LogTestFixtures.writeCommit
import com.deltaconnect.protocol.log.LogTestFixtures.writeInitialCommit
import com.deltaconnect.protocol.schema.DeltaType
import com.deltaconnect.protocol.schema.StructField
import com.deltaconnect.protocol.storage.LocalFileSystemLogStore
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path

class CheckpointWriterTest {
    @TempDir
    lateinit var tempDir: Path
    private lateinit var logStore: LocalFileSystemLogStore
    private lateinit var writer: CheckpointWriter
    private lateinit var reader: TransactionLogReader

    private val tablePath = "test-table"

    @BeforeEach
    fun setUp() {
        logStore = LocalFileSystemLogStore(tempDir)
        writer = CheckpointWriter(logStore)
        reader = TransactionLogReader(logStore)
    }

    @Nested
    inner class WriteReadRoundtrip {
        @Test
        fun `round-trips Protocol`() {
            writeInitialCommit(logStore, tablePath)
            val snapshot = reader.getSnapshot(tablePath)

            writer.writeCheckpoint(tablePath, snapshot)
            val actions = writer.readCheckpoint(tablePath, 0)

            val protocol = actions.filterIsInstance<Protocol>().single()
            protocol.minReaderVersion shouldBe 1
            protocol.minWriterVersion shouldBe 2
        }

        @Test
        fun `round-trips MetaData`() {
            writeInitialCommit(logStore, tablePath)
            val snapshot = reader.getSnapshot(tablePath)

            writer.writeCheckpoint(tablePath, snapshot)
            val actions = writer.readCheckpoint(tablePath, 0)

            val meta = actions.filterIsInstance<MetaData>().single()
            meta.id shouldBe "test-table-id"
            meta.schemaString shouldBe LogTestFixtures.SIMPLE_SCHEMA
            meta.format.provider shouldBe "parquet"
        }

        @Test
        fun `round-trips AddFiles`() {
            val files =
                listOf(
                    addFile("data/part-00000.parquet", size = 1024),
                    addFile("data/part-00001.parquet", size = 2048),
                )
            writeInitialCommit(logStore, tablePath, dataFiles = files)
            val snapshot = reader.getSnapshot(tablePath)

            writer.writeCheckpoint(tablePath, snapshot)
            val actions = writer.readCheckpoint(tablePath, 0)

            val addFiles = actions.filterIsInstance<AddFile>()
            addFiles shouldHaveSize 2
            addFiles.map { it.path } shouldContainExactlyInAnyOrder
                listOf("data/part-00000.parquet", "data/part-00001.parquet")
            addFiles.find { it.path == "data/part-00000.parquet" }!!.size shouldBe 1024
            addFiles.find { it.path == "data/part-00001.parquet" }!!.size shouldBe 2048
        }

        @Test
        fun `round-trips SetTransactions`() {
            writeInitialCommit(logStore, tablePath)
            writeCommit(
                logStore,
                tablePath,
                1,
                listOf(
                    setTransaction("app-1", 100, 1000L),
                    setTransaction("app-2", 200, 2000L),
                    commitInfo(operation = "WRITE"),
                ),
            )
            val snapshot = reader.getSnapshot(tablePath)

            writer.writeCheckpoint(tablePath, snapshot)
            val actions = writer.readCheckpoint(tablePath, 1)

            val txns = actions.filterIsInstance<SetTransaction>()
            txns shouldHaveSize 2
            txns.find { it.appId == "app-1" }!!.version shouldBe 100
            txns.find { it.appId == "app-2" }!!.version shouldBe 200
        }

        @Test
        fun `checkpoint does not contain CommitInfo`() {
            writeInitialCommit(logStore, tablePath)
            val snapshot = reader.getSnapshot(tablePath)

            writer.writeCheckpoint(tablePath, snapshot)
            val actions = writer.readCheckpoint(tablePath, 0)

            actions.none { it is CommitInfo } shouldBe true
        }

        @Test
        fun `checkpoint does not contain RemoveFile`() {
            writeInitialCommit(logStore, tablePath, listOf(addFile("data/old.parquet")))
            writeCommit(
                logStore,
                tablePath,
                1,
                listOf(
                    removeFile("data/old.parquet"),
                    addFile("data/new.parquet"),
                    commitInfo(operation = "WRITE"),
                ),
            )
            val snapshot = reader.getSnapshot(tablePath)

            writer.writeCheckpoint(tablePath, snapshot)
            val actions = writer.readCheckpoint(tablePath, 1)

            actions.none { it is RemoveFile } shouldBe true
            val addFiles = actions.filterIsInstance<AddFile>()
            addFiles shouldHaveSize 1
            addFiles.single().path shouldBe "data/new.parquet"
        }
    }

    @Nested
    inner class AddFileFields {
        @Test
        fun `round-trips AddFile with stats`() {
            val statsJson = """{"numRecords":10,"minValues":{"id":1},"maxValues":{"id":10},"nullCount":{"id":0}}"""
            writeInitialCommit(
                logStore,
                tablePath,
                listOf(
                    addFile("data/part.parquet", stats = statsJson),
                ),
            )
            val snapshot = reader.getSnapshot(tablePath)

            writer.writeCheckpoint(tablePath, snapshot)
            val actions = writer.readCheckpoint(tablePath, 0)

            val add = actions.filterIsInstance<AddFile>().single()
            add.stats shouldBe statsJson
        }

        @Test
        fun `round-trips AddFile with null stats`() {
            writeInitialCommit(
                logStore,
                tablePath,
                listOf(
                    addFile("data/part.parquet", stats = null),
                ),
            )
            val snapshot = reader.getSnapshot(tablePath)

            writer.writeCheckpoint(tablePath, snapshot)
            val actions = writer.readCheckpoint(tablePath, 0)

            val add = actions.filterIsInstance<AddFile>().single()
            add.stats shouldBe null
        }

        @Test
        fun `round-trips AddFile with partitionValues`() {
            val file =
                AddFile(
                    path = "year=2024/month=01/part.parquet",
                    partitionValues = mapOf("year" to "2024", "month" to "01"),
                    size = 512,
                    modificationTime = 1000L,
                    dataChange = true,
                )
            writeInitialCommit(logStore, tablePath, listOf(file))
            val snapshot = reader.getSnapshot(tablePath)

            writer.writeCheckpoint(tablePath, snapshot)
            val actions = writer.readCheckpoint(tablePath, 0)

            val add = actions.filterIsInstance<AddFile>().single()
            add.partitionValues shouldBe mapOf("year" to "2024", "month" to "01")
        }

        @Test
        fun `round-trips AddFile with empty partitionValues`() {
            writeInitialCommit(logStore, tablePath, listOf(addFile("data/part.parquet")))
            val snapshot = reader.getSnapshot(tablePath)

            writer.writeCheckpoint(tablePath, snapshot)
            val actions = writer.readCheckpoint(tablePath, 0)

            val add = actions.filterIsInstance<AddFile>().single()
            add.partitionValues shouldBe emptyMap()
        }

        @Test
        fun `round-trips AddFile with tags`() {
            val file =
                AddFile(
                    path = "data/part.parquet",
                    size = 1024,
                    modificationTime = 1000L,
                    dataChange = true,
                    tags = mapOf("engine" to "delta-connector", "source" to "test"),
                )
            writeInitialCommit(logStore, tablePath, listOf(file))
            val snapshot = reader.getSnapshot(tablePath)

            writer.writeCheckpoint(tablePath, snapshot)
            val actions = writer.readCheckpoint(tablePath, 0)

            val add = actions.filterIsInstance<AddFile>().single()
            add.tags shouldBe mapOf("engine" to "delta-connector", "source" to "test")
        }
    }

    @Nested
    inner class MetaDataFields {
        @Test
        fun `round-trips MetaData with name and description`() {
            val meta =
                MetaData(
                    id = "table-with-name",
                    name = "customers",
                    description = "Customer records from SQL Server",
                    format = Format(provider = "parquet", options = mapOf("compression" to "snappy")),
                    schemaString = LogTestFixtures.SIMPLE_SCHEMA,
                    partitionColumns = listOf("year", "month"),
                    configuration = mapOf("delta.appendOnly" to "false"),
                    createdTime = 1234567890L,
                )
            writeCommit(logStore, tablePath, 0, listOf(protocol(), meta, commitInfo()))
            val snapshot = reader.getSnapshot(tablePath)

            writer.writeCheckpoint(tablePath, snapshot)
            val actions = writer.readCheckpoint(tablePath, 0)

            val readMeta = actions.filterIsInstance<MetaData>().single()
            readMeta.id shouldBe "table-with-name"
            readMeta.name shouldBe "customers"
            readMeta.description shouldBe "Customer records from SQL Server"
            readMeta.format.provider shouldBe "parquet"
            readMeta.format.options shouldBe mapOf("compression" to "snappy")
            readMeta.partitionColumns shouldBe listOf("year", "month")
            readMeta.configuration shouldBe mapOf("delta.appendOnly" to "false")
            readMeta.createdTime shouldBe 1234567890L
        }

        @Test
        fun `round-trips MetaData with null optional fields`() {
            val meta =
                MetaData(
                    id = "minimal-table",
                    schemaString = LogTestFixtures.SIMPLE_SCHEMA,
                )
            writeCommit(logStore, tablePath, 0, listOf(protocol(), meta, commitInfo()))
            val snapshot = reader.getSnapshot(tablePath)

            writer.writeCheckpoint(tablePath, snapshot)
            val actions = writer.readCheckpoint(tablePath, 0)

            val readMeta = actions.filterIsInstance<MetaData>().single()
            readMeta.name shouldBe null
            readMeta.description shouldBe null
            readMeta.createdTime shouldBe null
        }
    }

    @Nested
    inner class ProtocolFields {
        @Test
        fun `round-trips Protocol with features`() {
            val proto =
                Protocol(
                    minReaderVersion = 3,
                    minWriterVersion = 7,
                    readerFeatures = setOf("columnMapping", "deletionVectors"),
                    writerFeatures = setOf("columnMapping", "deletionVectors", "appendOnly"),
                )
            writeCommit(logStore, tablePath, 0, listOf(proto, metaData(), commitInfo()))
            val snapshot = reader.getSnapshot(tablePath)

            writer.writeCheckpoint(tablePath, snapshot)
            val actions = writer.readCheckpoint(tablePath, 0)

            val readProto = actions.filterIsInstance<Protocol>().single()
            readProto.minReaderVersion shouldBe 3
            readProto.minWriterVersion shouldBe 7
            readProto.readerFeatures shouldBe setOf("columnMapping", "deletionVectors")
            readProto.writerFeatures shouldBe setOf("columnMapping", "deletionVectors", "appendOnly")
        }

        @Test
        fun `round-trips Protocol with null features`() {
            writeInitialCommit(logStore, tablePath)
            val snapshot = reader.getSnapshot(tablePath)

            writer.writeCheckpoint(tablePath, snapshot)
            val actions = writer.readCheckpoint(tablePath, 0)

            val readProto = actions.filterIsInstance<Protocol>().single()
            readProto.readerFeatures shouldBe null
            readProto.writerFeatures shouldBe null
        }
    }

    @Nested
    inner class SetTransactionFields {
        @Test
        fun `round-trips SetTransaction with null lastUpdated`() {
            writeInitialCommit(logStore, tablePath)
            writeCommit(
                logStore,
                tablePath,
                1,
                listOf(
                    SetTransaction(appId = "app-no-timestamp", version = 42, lastUpdated = null),
                    commitInfo(),
                ),
            )
            val snapshot = reader.getSnapshot(tablePath)

            writer.writeCheckpoint(tablePath, snapshot)
            val actions = writer.readCheckpoint(tablePath, 1)

            val txn = actions.filterIsInstance<SetTransaction>().single()
            txn.appId shouldBe "app-no-timestamp"
            txn.version shouldBe 42
            txn.lastUpdated shouldBe null
        }
    }

    @Nested
    inner class LastCheckpointFile {
        @Test
        fun `writeCheckpoint updates _last_checkpoint file`() {
            writeInitialCommit(logStore, tablePath)
            val snapshot = reader.getSnapshot(tablePath)

            writer.writeCheckpoint(tablePath, snapshot)

            val info = reader.readCheckpointInfo(tablePath)
            info shouldNotBe null
            info!!.version shouldBe 0
            info.size shouldBe 2L
        }

        @Test
        fun `_last_checkpoint size reflects action count`() {
            writeInitialCommit(
                logStore,
                tablePath,
                listOf(
                    addFile("data/part-0.parquet"),
                    addFile("data/part-1.parquet"),
                    addFile("data/part-2.parquet"),
                ),
            )
            val snapshot = reader.getSnapshot(tablePath)

            writer.writeCheckpoint(tablePath, snapshot)

            val info = reader.readCheckpointInfo(tablePath)!!
            info.size shouldBe 5L
        }

        @Test
        fun `LastCheckpointInfo toJson roundtrips`() {
            val info = LastCheckpointInfo(version = 42, size = 100, parts = null)
            val json = info.toJson()
            val parsed = LastCheckpointInfo.fromJson(json)
            parsed.version shouldBe 42
            parsed.size shouldBe 100
            parsed.parts shouldBe null
        }
    }

    @Nested
    inner class SnapshotViaCheckpoint {
        @Test
        fun `snapshot loaded from checkpoint matches full replay`() {
            writeInitialCommit(logStore, tablePath, listOf(addFile("data/v0.parquet")))
            writeCommit(
                logStore,
                tablePath,
                1,
                listOf(
                    addFile("data/v1.parquet"),
                    commitInfo(operation = "WRITE"),
                ),
            )
            writeCommit(
                logStore,
                tablePath,
                2,
                listOf(
                    removeFile("data/v0.parquet"),
                    addFile("data/v2.parquet"),
                    commitInfo(operation = "MERGE"),
                ),
            )
            writeCommit(
                logStore,
                tablePath,
                3,
                listOf(
                    addFile("data/v3.parquet"),
                    setTransaction("app-1", 50),
                    commitInfo(operation = "WRITE"),
                ),
            )
            writeCommit(
                logStore,
                tablePath,
                4,
                listOf(
                    addFile("data/v4.parquet"),
                    commitInfo(operation = "WRITE"),
                ),
            )

            val fullReplaySnapshot = reader.getSnapshot(tablePath)
            writer.writeCheckpoint(tablePath, fullReplaySnapshot)

            val checkpointSnapshot = TransactionLogReader(logStore).getSnapshot(tablePath)

            checkpointSnapshot.version shouldBe fullReplaySnapshot.version
            checkpointSnapshot.activeFiles shouldBe fullReplaySnapshot.activeFiles
            checkpointSnapshot.metaData shouldBe fullReplaySnapshot.metaData
            checkpointSnapshot.protocol shouldBe fullReplaySnapshot.protocol
            checkpointSnapshot.transactions shouldBe fullReplaySnapshot.transactions
        }

        @Test
        fun `snapshot with checkpoint plus subsequent commits`() {
            writeInitialCommit(logStore, tablePath, listOf(addFile("data/v0.parquet")))
            for (i in 1..4) {
                writeCommit(
                    logStore,
                    tablePath,
                    i.toLong(),
                    listOf(
                        addFile("data/v$i.parquet"),
                        commitInfo(operation = "WRITE"),
                    ),
                )
            }
            val snapshotAtV4 = reader.getSnapshot(tablePath)
            writer.writeCheckpoint(tablePath, snapshotAtV4)

            writeCommit(
                logStore,
                tablePath,
                5,
                listOf(
                    addFile("data/v5.parquet"),
                    commitInfo(operation = "WRITE"),
                ),
            )
            writeCommit(
                logStore,
                tablePath,
                6,
                listOf(
                    removeFile("data/v0.parquet"),
                    commitInfo(operation = "MERGE"),
                ),
            )

            val snapshot = TransactionLogReader(logStore).getSnapshot(tablePath)

            snapshot.version shouldBe 6
            snapshot.activeFiles.map { it.path } shouldContainExactlyInAnyOrder
                listOf(
                    "data/v1.parquet",
                    "data/v2.parquet",
                    "data/v3.parquet",
                    "data/v4.parquet",
                    "data/v5.parquet",
                )
        }

        @Test
        fun `snapshot with corrupt checkpoint falls back to full replay`() {
            writeInitialCommit(logStore, tablePath, listOf(addFile("data/v0.parquet")))
            writeCommit(
                logStore,
                tablePath,
                1,
                listOf(
                    addFile("data/v1.parquet"),
                    commitInfo(operation = "WRITE"),
                ),
            )

            logStore.writeLastCheckpoint(tablePath, """{"version":1,"size":3}""")

            val snapshot = TransactionLogReader(logStore).getSnapshot(tablePath)
            snapshot.version shouldBe 1
            snapshot.activeFiles shouldHaveSize 2
        }
    }

    @Nested
    inner class Validation {
        @Test
        fun `writeCheckpoint rejects empty table`() {
            val empty = DeltaSnapshot.empty()
            shouldThrow<IllegalArgumentException> {
                writer.writeCheckpoint(tablePath, empty)
            }
        }

        @Test
        fun `writeCheckpoint rejects snapshot without Protocol`() {
            val snapshot =
                DeltaSnapshot(
                    version = 0,
                    activeFiles = emptySet(),
                    metaData = metaData(),
                    protocol = null,
                    transactions = emptyMap(),
                    commitInfo = null,
                )
            shouldThrow<IllegalArgumentException> {
                writer.writeCheckpoint(tablePath, snapshot)
            }
        }

        @Test
        fun `writeCheckpoint rejects snapshot without MetaData`() {
            val snapshot =
                DeltaSnapshot(
                    version = 0,
                    activeFiles = emptySet(),
                    metaData = null,
                    protocol = protocol(),
                    transactions = emptyMap(),
                    commitInfo = null,
                )
            shouldThrow<IllegalArgumentException> {
                writer.writeCheckpoint(tablePath, snapshot)
            }
        }
    }

    @Nested
    inner class CheckpointTrigger {
        @Test
        fun `checkpoint is triggered at version 10`() {
            val schema =
                DeltaType.StructType(
                    listOf(
                        StructField("id", DeltaType.IntegerType, nullable = false),
                        StructField("value", DeltaType.StringType, nullable = true),
                    ),
                )
            val table = DeltaTable.createOrReplace(logStore, tablePath, schema)

            for (i in 1..9) {
                val txn = table.startTransaction()
                txn.addAction(addFile("data/v$i.parquet", size = 100))
                txn.commit("WRITE")
            }

            reader.readCheckpointInfo(tablePath) shouldBe null

            val txn = table.startTransaction()
            txn.addAction(addFile("data/v10.parquet", size = 100))
            txn.commit("WRITE")

            val info = reader.readCheckpointInfo(tablePath)
            info shouldNotBe null
            info!!.version shouldBe 10
        }

        @Test
        fun `checkpoint is not triggered at non-interval versions`() {
            val schema =
                DeltaType.StructType(
                    listOf(
                        StructField("id", DeltaType.IntegerType, nullable = false),
                    ),
                )
            val table = DeltaTable.createOrReplace(logStore, tablePath, schema)

            for (i in 1..5) {
                val txn = table.startTransaction()
                txn.addAction(addFile("data/v$i.parquet", size = 100))
                txn.commit("WRITE")
            }

            reader.readCheckpointInfo(tablePath) shouldBe null
        }

        @Test
        fun `multiple checkpoints accumulate correctly`() {
            val schema =
                DeltaType.StructType(
                    listOf(
                        StructField("id", DeltaType.IntegerType, nullable = false),
                    ),
                )
            val table = DeltaTable.createOrReplace(logStore, tablePath, schema)

            for (i in 1..20) {
                val txn = table.startTransaction()
                txn.addAction(addFile("data/v$i.parquet", size = 100))
                txn.commit("WRITE")
            }

            val info = reader.readCheckpointInfo(tablePath)
            info shouldNotBe null
            info!!.version shouldBe 20

            val snapshot = TransactionLogReader(logStore).getSnapshot(tablePath)
            snapshot.version shouldBe 20
            snapshot.activeFiles shouldHaveSize 20
        }
    }

    @Nested
    inner class EdgeCases {
        @Test
        fun `checkpoint with many files round-trips correctly`() {
            val files = (1..50).map { addFile("data/part-$it.parquet", size = it.toLong() * 100) }
            writeInitialCommit(logStore, tablePath, files)
            val snapshot = reader.getSnapshot(tablePath)

            writer.writeCheckpoint(tablePath, snapshot)
            val actions = writer.readCheckpoint(tablePath, 0)

            val addFiles = actions.filterIsInstance<AddFile>()
            addFiles shouldHaveSize 50
            addFiles.map { it.path }.toSet() shouldBe files.map { it.path }.toSet()
        }

        @Test
        fun `checkpoint with only protocol and metadata round-trips`() {
            writeCommit(
                logStore,
                tablePath,
                0,
                listOf(
                    protocol(),
                    metaData(),
                    commitInfo(),
                ),
            )
            val snapshot = reader.getSnapshot(tablePath)

            writer.writeCheckpoint(tablePath, snapshot)
            val actions = writer.readCheckpoint(tablePath, 0)

            actions.filterIsInstance<Protocol>() shouldHaveSize 1
            actions.filterIsInstance<MetaData>() shouldHaveSize 1
            actions.filterIsInstance<AddFile>() shouldHaveSize 0
        }

        @Test
        fun `checkpointFileName formats correctly`() {
            ActionSerializer.checkpointFileName(0) shouldBe
                "00000000000000000000.checkpoint.parquet"
            ActionSerializer.checkpointFileName(10) shouldBe
                "00000000000000000010.checkpoint.parquet"
            ActionSerializer.checkpointFileName(Long.MAX_VALUE) shouldBe
                "${Long.MAX_VALUE}".padStart(20, '0') + ".checkpoint.parquet"
        }
    }
}
