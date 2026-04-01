# Ducklake Connector Status

Last updated: 2026-04-01

## Read Side — Complete

The read path is fully implemented and tested.

### Data Access
- Catalog reads from Ducklake SQL metadata tables (SQLite via JDBC/HikariCP).
- Snapshot-scoped reads of current snapshot.
- Parquet data files read through Trino's native Parquet reader.
- Inlined data read directly from the metadata catalog (DuckLake's default for tables with <=10 rows).
- Merge-on-read delete file filtering.
- Schema evolution: missing columns return NULLs.

### Query Optimization
- **File-level pruning**: eliminates whole Parquet files via `ducklake_file_column_stats` min/max.
- **Partition pruning**: identity and temporal transforms via `ducklake_partition_info` / `ducklake_file_partition_value`.
- **Row-group pruning**: Parquet footer statistics checked via `getFilteredRowGroups()`.
- **Page-level filtering**: Parquet page indexes passed to `ParquetReader` when available.
- **Dynamic filters**: intersected with file stats domain at page source creation.
- **Table statistics**: exposed via `ducklake_table_stats` + typed aggregated column min/max for cost-based optimization.
- `applyFilter` planner hook splits predicates into enforced (partition) and unenforced (engine-verified).

### Type Support
- Full: boolean, tinyint, smallint, integer, bigint, real, double, decimal, varchar, varbinary, date, time, timestamp, timestamptz, uuid.
- Full: arrays, structs/rows, maps, nested combinations.
- Degraded: `json` -> VARCHAR, `variant` -> VARCHAR, geometry family -> VARBINARY.
  These types are readable but lack type-specific operators and functions.

### Test Coverage
- 173 tests, 0 failures across 7 test classes.
- `TestDucklakeIntegration`: 125 end-to-end SQL tests via `DucklakeQueryRunner`.
- 15 test tables covering primitives, arrays, structs, maps, partitioning (identity/temporal/daily), schema evolution, NULLs, empty tables, delete files, multi-file scans, complex NULL patterns, and inlined data.
- Unit tests for catalog, split manager, partition pruning, page source provider, delete file handling, plugin wiring.

## Known Gaps and Concerns

### Temporal partition transform values (open issue)
DuckDB's ducklake extension writes literal calendar values (e.g., year=2023, month=6) to `ducklake_file_partition_value` instead of the epoch-based values described in the spec (e.g., year=53, month=641). Our implementation follows DuckDB's actual behavior. If the spec is updated or DuckDB changes behavior in a future release, this code will need reconciliation. See [REPORT_DUCKLAKE_PARTITION_PROB.md](REPORT_DUCKLAKE_PARTITION_PROB.md) and [duckdb/ducklake-web#312](https://github.com/duckdb/ducklake-web/issues/312).

### Time travel
`FOR SYSTEM_TIME AS OF` / `FOR SYSTEM_VERSION AS OF` are not implemented. The connector always reads the latest snapshot.

### Data inlining — read support only
Trino can read inlined data, but only for the simple case: tables with no Parquet files where all data is inlined. If a table has both inlined data AND Parquet files (possible during a partial flush), only the Parquet files are read. This covers the common case (small tables that were never flushed) but not the mixed case. The mixed case is rare in practice — DuckLake's flush operation is atomic.

### Degraded type semantics
`json`, `variant`, and geometry types are stored as VARCHAR/VARBINARY. No type-specific functions or operators. Variant shredding is not implemented.

### Catalog backend
Only SQLite is tested. DuckDB-as-catalog-backend and PostgreSQL-as-catalog-backend are not tested, though they should work via JDBC with minor catalog URL changes.

## Write Side — Not Implemented

No write operations are supported: INSERT, UPDATE, DELETE, MERGE, CREATE TABLE, DROP TABLE, ALTER TABLE, CREATE SCHEMA, DROP SCHEMA. All write attempts return "not supported" errors.

When write support is added, the recommended test approach is to extend Trino's `BaseConnectorTest` (226 standard connector methods) rather than porting DuckDB's SQLLogicTest files (which use DuckDB-specific SQL syntax).
