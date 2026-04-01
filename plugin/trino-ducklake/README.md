# Trino Ducklake Connector

Read-only Trino connector for the [Ducklake](https://ducklake.select/) table format.

Reads Ducklake metadata from a SQLite catalog database and data from Parquet files via Trino's native Parquet reader. Supports data inlined in the metadata catalog (DuckLake's default for small tables).

## Documentation

- [STATUS.md](STATUS.md) — Current implementation state, gaps, and concerns.
- [REUSE.md](REUSE.md) — What we reuse from Trino/Iceberg and what's custom.
- [REPORT_DUCKLAKE_PARTITION_PROB.md](REPORT_DUCKLAKE_PARTITION_PROB.md) — Open issue: temporal partition values in DuckDB don't match spec.

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

Targeted runs:

```bash
# Full integration tests (125 methods)
mvn test -Dtest=TestDucklakeIntegration

# Catalog metadata
mvn test -Dtest=TestDucklakeCatalog

# Split pruning + partition pruning
mvn test -Dtest=TestDucklakeSplitManager,TestDucklakePartitionPruning

# Page source + delete handling
mvn test -Dtest=TestDucklakePageSourceProvider,TestDucklakeDeleteFileHandling
```

173 tests across 7 test classes, 0 failures.
