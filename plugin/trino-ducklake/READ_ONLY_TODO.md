# Ducklake Read-Only TODO (Parity Track)

Goal: bring Ducklake read-only behavior up to Iceberg/Hive quality for correctness, pruning, and planner integration.

## Priority Order

### P0: Correctness (must-have before parity claims)

1. ~~Delete-file application (merge-on-read)~~ ✅ DONE
- ~~Read delete metadata/files and filter deleted rows during scans.~~
- ~~Add tests where deleted rows are present in data files and must be suppressed.~~

### P1: Planner and Runtime Pushdown Parity

1. ~~Metadata `applyFilter` support~~ ✅ DONE
- ~~Persist pushed-down constraints in table handles.~~
- ~~Match Iceberg/Hive pattern so planner can avoid unnecessary scans earlier.~~

2. ~~Dynamic filter integration~~ ✅ DONE
- ~~Use dynamic filters in page-source skip paths (intersect with file statistics domain).~~
- ~~No wait/timeout needed — FixedSplitSource model; dynamic filter evaluated at read time.~~

3. ~~Broader stats-based pruning~~ ✅ DONE
- ~~Extend catalog pruning to handle discrete `IN` domains (extract min/max bounds from value sets).~~
- ~~Row-group pruning in Parquet path aligned with effective predicates via dynamic filter intersection.~~

4. ~~Table statistics exposure~~ ✅ DONE
- ~~Implement `getTableStatistics` querying ducklake_table_stats and ducklake_file_column_stats.~~
- ~~Exposes row count, null fraction, data size, and numeric/date ranges for CBO.~~

### P2: Type Completeness

1. ~~Complex nested read types~~ ✅ DONE
- ~~`MAP`, `ROW/STRUCT`, nested arrays.~~
- ~~Add corresponding parquet field conversion coverage.~~

2. Degraded type improvements
- Improve `variant`, `json`, and geometry handling beyond placeholder mappings.

### P3: Scale and Performance

1. Split planning quality
- Revisit one-file-one-split behavior for large files.

2. Catalog efficiency
- Avoid unnecessary metadata round-trips on hot paths.
- Add broader catalog backend testing (PostgreSQL in addition to SQLite).

### P4: Test and Validation Depth

1. Integration tests beyond unit scope
- Query-runner tests validating real SQL behavior through Trino planner/execution.

2. Regression matrix
- Add targeted cases for deletes + pruning + dynamic filters + nested types.

### P5: Deferred

1. Time-travel semantics
- Honor table-version inputs instead of always forcing current snapshot.
- Add tests for snapshot selection behavior.

## Current Selection

### Next item to implement: P2.2 Degraded type improvements

Reason: P0 correctness, P1 planner parity, and P2.1 complex nested types are complete. Degraded type handling (variant, json, geometry) is the next gap.
