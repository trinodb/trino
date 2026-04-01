# Trino Ducklake Connector

Read-only Trino connector for the Ducklake table format.

## Current Docs
- [STATUS.md](STATUS.md): Current implementation state and known gaps.
- [REMEDIATION_PLAN.md](REMEDIATION_PLAN.md): Prioritized remediation plan.
- [REUSE.md](REUSE.md): Reuse strategy and where we still diverge from Iceberg.
- [SMOKE_TEST.md](SMOKE_TEST.md): Build and test commands.
- [TESTING_UPGRADE.md](TESTING_UPGRADE.md): Testing upgrade plan (query-runner, cross-engine, coverage gaps).

## What Works
- SQL catalog reads from Ducklake metadata tables (SQLite tested).
- Snapshot-scoped reads of current snapshot.
- Parquet reads through Trino Parquet reader path.
- Split generation with file-stats pruning.
- Partition pruning (identity + temporal transforms).
- Merge-on-read delete-file filtering.
- `applyFilter` support and dynamic-filter intersection at page source.

## What Is Still Limited
- Time travel (`FOR SYSTEM_TIME`) is not implemented.
- `json` and `variant` map to `VARCHAR`; geometry maps to `VARBINARY`.
- No write path (INSERT/UPDATE/DELETE/DDL).
- No query-runner integration tests yet.

## Configuration
Example `etc/catalog/ducklake.properties`:

```properties
connector.name=ducklake
ducklake.catalog.database-url=jdbc:sqlite:/path/to/catalog.db
ducklake.data-path=/path/to/data
ducklake.catalog.max-connections=10
```

## Build and Test

```bash
cd plugin/trino-ducklake
mvn test
```

See [SMOKE_TEST.md](SMOKE_TEST.md) for focused test commands and expected output.
