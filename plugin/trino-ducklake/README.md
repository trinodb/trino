# Trino Ducklake Connector

A Trino connector for the [Ducklake](https://ducklake.select) lakehouse format (v0.4/0.5 spec).

**Quick Links:**
- **[STATUS.md](STATUS.md)** - Current implementation status, what works, what's next
- **[READ_ONLY_TODO.md](READ_ONLY_TODO.md)** - Prioritized read-only parity backlog vs Iceberg/Hive
- **[SMOKE_TEST.md](SMOKE_TEST.md)** - How to run the automated tests
- **[REVISED_PLAN.md](REVISED_PLAN.md)** - Technical architecture and design decisions
- **[REUSE.md](REUSE.md)** - Code reuse strategy from Iceberg connector

## Overview

Ducklake is a lakehouse format that uses:
- **SQL-based metadata catalog** (28 tables in any SQL-92 compliant database)
- **Parquet data files** for table storage
- **Snapshot-based MVCC** for time travel and transactional consistency
- **File-level statistics** for predicate pushdown

This connector provides read access to Ducklake tables from Trino.

## Architecture

The connector is designed with maximum code reuse from the Iceberg connector:

### Shared Components (~60% reuse)
- **Parquet I/O**: 100% reusable - both use Parquet files
- **Parquet reader path**: Reuses Trino Parquet reader (column projection, vectorized reads, encodings/codecs)
- **Type Conversion Patterns**: 70% reusable

### New Components
- **SQL Catalog Layer**: Queries 28 Ducklake metadata tables
- **Snapshot Management**: SQL-based vs file-based
- **Transaction Coordination**: JDBC transaction management

## Configuration

Create a catalog properties file (e.g., `etc/catalog/ducklake.properties`):

```properties
connector.name=ducklake

# SQLite catalog database (for testing)
ducklake.catalog.database-url=jdbc:sqlite:/path/to/catalog.db

# PostgreSQL catalog database (for production)
# ducklake.catalog.database-url=jdbc:postgresql://localhost:5432/ducklake_catalog
# ducklake.catalog.database-user=ducklake
# ducklake.catalog.database-password=secret

# Base path for relative data file paths
ducklake.data-path=s3://my-bucket/ducklake-data/

# Optional: JDBC connection pool size
ducklake.catalog.max-connections=10
```

## Catalog Database Setup

### SQLite (Development/Testing)

```bash
# Create a new SQLite database with Ducklake schema
sqlite3 /path/to/catalog.db < ducklake_schema.sql
```

### PostgreSQL (Production)

```sql
CREATE DATABASE ducklake_catalog;
\c ducklake_catalog
\i ducklake_schema.sql
```

The schema SQL includes the 28 Ducklake tables:
```sql
CREATE TABLE ducklake_snapshot (...);
CREATE TABLE ducklake_schema (...);
CREATE TABLE ducklake_table (...);
CREATE TABLE ducklake_column (...);
CREATE TABLE ducklake_data_file (...);
-- ... and 23 more tables
```

## Supported Features

### ✅ Implemented
- [x] List schemas
- [x] List tables
- [x] Get table metadata
- [x] Read table data (Parquet files)
- [x] Snapshot isolation (reads current snapshot)
- [x] Type mapping (primitives + `list<primitive>` / arrays)
- [x] Basic split pruning using `ducklake_file_column_stats` for range/single-value constraints

### 🚧 Planned
- [ ] Extended predicate pushdown coverage (discrete sets, dynamic filters, richer expression handling)
- [ ] Time travel queries (query historical snapshots)
- [ ] Delete file handling (merge-on-read)
- [ ] Partitioned table support
- [ ] Variant type handling (shredded types)
- [ ] Geometry type support
- [ ] Write operations (INSERT, DELETE, UPDATE)
- [ ] DDL operations (CREATE TABLE, DROP TABLE, etc.)
- [ ] Table procedures (OPTIMIZE, VACUUM, etc.)

### ❌ Not Supported
- Unsigned integer types (mapped to next larger signed type)
- Full variant type support (mapped to VARCHAR for now)
- Geometry types (mapped to VARBINARY for now)

## Type Mapping

| Ducklake Type | Trino Type |
|---------------|------------|
| `boolean` | `BOOLEAN` |
| `int8` | `TINYINT` |
| `int16` | `SMALLINT` |
| `int32` | `INTEGER` |
| `int64` | `BIGINT` |
| `uint8` | `SMALLINT` ⚠️ |
| `uint16` | `INTEGER` ⚠️ |
| `uint32` | `BIGINT` ⚠️ |
| `uint64` | `DECIMAL(20,0)` ⚠️ |
| `float32` | `REAL` |
| `float64` | `DOUBLE` |
| `decimal(P,S)` | `DECIMAL(P,S)` |
| `date` | `DATE` |
| `time` | `TIME(6)` |
| `timetz` | `TIME(6) WITH TIME ZONE` |
| `timestamp` | `TIMESTAMP(6)` |
| `timestamptz` | `TIMESTAMP(6) WITH TIME ZONE` |
| `varchar` | `VARCHAR` |
| `blob` | `VARBINARY` |
| `uuid` | `UUID` |
| `json` | `VARCHAR` ⚠️ |
| `variant` | `VARCHAR` ⚠️ |
| `geometry` | `VARBINARY` ⚠️ |

⚠️ = Partial or placeholder mapping

## Query Examples

```sql
-- List all schemas
SHOW SCHEMAS FROM ducklake;

-- List tables in a schema
SHOW TABLES FROM ducklake.main;

-- Query a table
SELECT * FROM ducklake.main.my_table;

-- Time travel query (future)
-- TODO: time travel is not implemented yet
-- SELECT * FROM ducklake.main.my_table FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-01 00:00:00';
```

## Implementation Details

### SQL Catalog Queries

The connector uses SQL queries against the Ducklake metadata tables:

**Get current snapshot:**
```sql
SELECT snapshot_id FROM ducklake_snapshot
WHERE snapshot_id = (SELECT max(snapshot_id) FROM ducklake_snapshot)
```

**Get table metadata:**
```sql
SELECT table_id, table_name FROM ducklake_table
WHERE schema_id = ? AND ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)
```

**Get data files:**
```sql
SELECT data.path, del.path AS delete_file_path
FROM ducklake_data_file AS data
LEFT JOIN ducklake_delete_file AS del USING (data_file_id)
WHERE data.table_id = ? AND ? >= data.begin_snapshot ...
```

### File Discovery

1. Query `ducklake_data_file` for table at snapshot
2. Join `ducklake_delete_file` metadata (delete application is still TODO)
3. Resolve relative paths using `ducklake_metadata.data_path`
4. Create splits (one per Parquet file)

### Predicate Pushdown

Current implementation uses `ducklake_file_column_stats` for basic split pruning by range/single-value constraints.

Future work will expand this to additional predicate forms.

Core pruning query pattern:
```sql
SELECT data_file_id FROM ducklake_file_column_stats
WHERE table_id = ? AND column_id = ?
  AND (? >= min_value OR min_value IS NULL)
  AND (? <= max_value OR max_value IS NULL)
```

## Development

### Building

```bash
cd plugin/trino-ducklake
mvn compile
```

### Testing

The connector includes automated unit tests that generate a test Ducklake catalog using DuckDB's embedded JDBC driver.

```bash
# Run all unit tests (auto-generates test catalog)
mvn test

# Run specific smoke test
mvn test -Dtest=TestDucklakeCatalog
```

The test catalog is automatically generated in `target/test-catalog/` with:
- SQLite catalog database: `target/test-catalog/catalog.db`
- Parquet data files: `target/test-catalog/data/*.parquet`
- Two test tables: `simple_table` (primitives only) and `array_table` (primitives + VARCHAR[])

See [SMOKE_TEST.md](SMOKE_TEST.md) for details on what the tests verify.

### Code Structure

```
plugin/trino-ducklake/
├── src/main/java/io/trino/plugin/ducklake/
│   ├── DucklakePlugin.java              # Plugin entry point
│   ├── DucklakeConnectorFactory.java    # Connector factory
│   ├── DucklakeConnector.java           # Main connector
│   ├── DucklakeModule.java              # Guice bindings
│   ├── DucklakeConfig.java              # Configuration
│   ├── DucklakeMetadata.java            # Metadata operations
│   ├── DucklakeSplitManager.java        # Split generation
│   ├── DucklakeTypeConverter.java       # Type mapping
│   ├── DucklakeTransactionManager.java  # Transaction handling
│   └── catalog/
│       ├── DucklakeCatalog.java         # Catalog interface
│       ├── SqliteDucklakeCatalog.java   # SQLite implementation
│       ├── DucklakeSnapshot.java        # Snapshot model
│       ├── DucklakeSchema.java          # Schema model
│       ├── DucklakeTable.java           # Table model
│       ├── DucklakeColumn.java          # Column model
│       └── DucklakeDataFile.java        # Data file model
```

## Troubleshooting

### "No snapshots found in ducklake_snapshot table"
- Ensure the catalog database is initialized with the Ducklake schema
- Check that at least one snapshot exists in `ducklake_snapshot`

### "No data path configured for relative file paths"
- Set `ducklake.data-path` in the catalog properties
- Or ensure `ducklake_metadata` table has a `data_path` entry

### JDBC connection errors
- Verify the JDBC URL is correct
- For SQLite, ensure the file path is absolute and accessible
- For PostgreSQL, check credentials and network connectivity

## Performance Considerations

### Catalog Database
- Use connection pooling (default: 10 connections)
- Consider PostgreSQL for production (better concurrency than SQLite)
- Index the snapshot filtering columns: `begin_snapshot`, `end_snapshot`

### Predicate Pushdown
- Enable file pruning via `ducklake_file_column_stats` (planned)
- Ensure statistics are up-to-date for optimal performance

### Caching
- Catalog query results can be cached (future enhancement)
- Schema version tracking allows cache invalidation

## Contributing

This connector is in active development. Contributions welcome!

Priority areas:
1. Predicate pushdown implementation
2. Variant type handling
3. Write operations
4. DDL operations
5. Performance optimizations

## References

- [Ducklake Specification](https://ducklake.select/docs/stable/specification/introduction)
- [Ducklake GitHub](https://github.com/duckdb/ducklake)
- [Trino Connector Development](https://trino.io/docs/current/develop/connectors.html)

## License

Licensed under the Apache License, Version 2.0. See the Trino project LICENSE file for details.
