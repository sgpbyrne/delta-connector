# Delta Lake Sink Connector

Kafka Connect sink connector that writes Change Data Capture (CDC) events and plain records to Delta Lake tables. Supports Debezium CDC envelopes, upsert, and append write modes. Cloud-agnostic through a pluggable `StorageProvider` Interface (SPI), with an Azure Blob Storage backend included.

## Overview

This connector reads records from Kafka topics and writes them as Delta Lake tables. Each topic maps to a Delta table. The connector manages the full Delta transaction log so tables are immediately queryable by any engine that reads Delta.

In CDC mode, the connector parses Debezium change events and applies inserts, updates, and deletes through a merge engine. In append mode, every record is written as a new row. Upsert mode does key-based insert-or-update without requiring Debezium envelopes.

### Features

- **Three write modes** - CDC (Debezium envelope parsing), upsert (keyed merge), append (insert-only)
- **Schema evolution** - New nullable columns are added automatically, type widening (INT→LONG, FLOAT→DOUBLE) is handled, incompatible changes are rejected
- **Exactly-once delivery** - Offset tracking in the Delta transaction log with conflict detection on commit
- **Dead letter queue** - Bad records route to Kafka Connect's error reporter, good records in the same batch still commit
- **Unity Catalog integration** - Optional automatic table registration and metadata sync with Databricks Unity Catalog
- **Observability** - JMX metrics exposed alongside Kafka Connect's built-in metrics. Optional OTLP push to an OpenTelemetry collector
- **Cloud-agnostic storage** - `StorageProvider` SPI with Azure Blob Storage included. Add S3 or GCS by implementing the interface

## Modules

| Module               | Description                                                                                                                                |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| `delta-protocol`     | Pure Kotlin Delta Lake protocol engine. Handles the transaction log, schema, merge, and Parquet I/O. Zero Kafka or cloud SDK dependencies. |
| `delta-connect`      | Kafka Connect `SinkConnector` and `SinkTask` implementation. Bridges Kafka Connect records to the protocol engine.                         |
| `delta-azure`        | Azure Blob Storage backend. Implements `DeltaLogStore` using the Azure Blob SDK with conditional create for atomic commits.                |
| `delta-catalog`      | Databricks Unity Catalog client. Registers Delta tables as external tables and runs periodic metadata sync.                                |
| `delta-connect-dist` | Distribution packaging. Produces a shadow JAR and Docker image with all dependencies bundled.                                              |

## Prerequisites

- **JDK 17+**
- **Docker** (for integration tests — Testcontainers runs Kafka and Azurite)
- Gradle wrapper is included; no separate Gradle install needed

## Build

```bash
# Full build (compile + unit tests)
./gradlew build

# Unit tests only
./gradlew test

# Single module
./gradlew :delta-protocol:test

# Integration tests (requires Docker)
./gradlew integrationTest

# Shadow JAR (output in delta-connect-dist/build/libs/)
./gradlew :delta-connect-dist:shadowJar

# Docker image
./gradlew :delta-connect-dist:dockerBuild
```

## Configuration

### Delta Lake

| Property                      | Type    | Default    | Description                                           |
| ----------------------------- | ------- | ---------- | ----------------------------------------------------- |
| `delta.storage.path`          | string  | —          | Base path for Delta tables.                           |
| `delta.write.mode`            | string  | `cdc`      | `append`, `upsert`, or `cdc`.                         |
| `delta.merge.keys`            | list    | —          | Merge key columns. Required for `upsert` and `cdc`.   |
| `delta.table.name`            | string  | `${topic}` | Table name template. Supports `${topic}` placeholder. |
| `delta.merge.batch.size`      | int     | `50000`    | Max records buffered before flush.                    |
| `delta.merge.interval.ms`     | long    | `60000`    | Max time (ms) before flush.                           |
| `delta.merge.delete.enabled`  | boolean | `true`     | Process deletes in upsert/cdc modes.                  |
| `delta.schema.evolution`      | boolean | `true`     | Auto-evolve table schema on new columns.              |
| `delta.checkpoint.interval`   | int     | `10`       | Commits between checkpoint writes.                    |
| `delta.metrics.otlp.endpoint` | string  | —          | OTLP endpoint for metrics push. Empty = JMX only.     |

### CDC

| Property              | Type   | Default         | Description                                          |
| --------------------- | ------ | --------------- | ---------------------------------------------------- |
| `cdc.envelope.format` | string | `debezium_full` | `debezium_full` or `debezium_flattened` (after SMT). |
| `cdc.source.database` | string | `sqlserver`     | Source database for type mapping.                    |

### Unity Catalog

| Property                         | Type     | Default  | Description                                             |
| -------------------------------- | -------- | -------- | ------------------------------------------------------- |
| `unity.catalog.enabled`          | boolean  | `false`  | Register tables in Unity Catalog.                       |
| `unity.catalog.workspace.url`    | string   | —        | Databricks workspace URL.                               |
| `unity.catalog.token`            | password | —        | Databricks PAT. Prefer workload identity in production. |
| `unity.catalog.name`             | string   | —        | Catalog name.                                           |
| `unity.catalog.schema`           | string   | —        | Schema name.                                            |
| `unity.catalog.warehouse.id`     | string   | —        | SQL warehouse ID.                                       |
| `unity.catalog.sync.interval.ms` | long     | `300000` | Interval between metadata sync calls.                   |

## Metrics

Metrics are exposed via JMX under the `delta.sink.*` domain. If `delta.metrics.otlp.endpoint` is set, metrics are also pushed to the configured OpenTelemetry collector.

| Metric                         | Type    | Description                            |
| ------------------------------ | ------- | -------------------------------------- |
| `delta.sink.records.received`  | counter | Records received from Kafka            |
| `delta.sink.records.flushed`   | counter | Records written to Delta               |
| `delta.sink.records.dlq`       | counter | Records sent to dead letter queue      |
| `delta.sink.flush.latency`     | timer   | Time to flush a batch (commit + write) |
| `delta.sink.schema.evolutions` | counter | Schema evolution events                |

## Contributing

### Setup

Fork the repo, create a branch, make your changes, run the tests, open a PR.

```bash
git clone https://github.com/<you>/delta-connector.git
cd delta-connector
./gradlew build          # compile + unit tests
./gradlew integrationTest # requires Docker
```

### Commit messages

Follow conventional commits scoped by module:

```
feat(delta-protocol): add MERGE engine
fix(delta-connect): handle null key in upsert mode
test(delta-azure): add Azurite conflict detection test
```

### Code standards

- Kotlin 1.9, JVM 17
- No wildcard imports, 120 char line limit
- Explicit return types on public functions
- `delta-protocol` must not depend on Kafka or cloud SDKs
- `delta-azure` must not depend on Kafka or Connect
- All file I/O goes through `DeltaLogStore` — no direct filesystem calls in protocol code
- Every change needs tests; `./gradlew test` must pass

## License

Apache License 2.0 — see [LICENSE](LICENSE).
