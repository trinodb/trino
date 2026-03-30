# Ducklake Read-Only TODO (Parity Track)

Goal: bring Ducklake read-only behavior up to Iceberg/Hive quality for correctness, pruning, and planner integration.

## Priority Order

### P0: Correctness (must-have before parity claims)

1. Delete-file application (merge-on-read)
- Read delete metadata/files and filter deleted rows during scans.
- Add tests where deleted rows are present in data files and must be suppressed.

2. Time-travel semantics
- Honor table-version inputs instead of always forcing current snapshot.
- Add tests for snapshot selection behavior.

### P1: Planner and Runtime Pushdown Parity

1. Metadata `applyFilter` support
- Persist pushed-down constraints in table handles.
- Match Iceberg/Hive pattern so planner can avoid unnecessary scans earlier.

2. Dynamic filter integration
- Use dynamic filters in split pruning and page-source skip paths.
- Add wait/timeout behavior only if needed for correctness/performance.

3. Broader stats-based pruning
- Extend catalog pruning beyond range-span cases (e.g., discrete `IN` domains where practical).
- Keep row-group pruning in Parquet path aligned with effective predicates.

4. Table statistics exposure
- Implement `getTableStatistics` for CBO and better plan quality.

### P2: Type Completeness

1. Complex nested read types
- `MAP`, `ROW/STRUCT`, nested arrays.
- Add corresponding parquet field conversion coverage.

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

## Current Selection

### Next item to implement: P0.1 Delete-file application

Reason: this is the largest correctness gap in current read-only behavior. Without it, results can be wrong when position deletes exist.
