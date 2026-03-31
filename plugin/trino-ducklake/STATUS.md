# Ducklake Connector Status

Last updated: 2026-03-30

## Validation Snapshot
- Build: compiling.
- Tests: passing.
- Latest local verification: `mvn test` in `plugin/trino-ducklake`.
- Test count: 42 tests, 0 failures, 0 errors.

## Implemented
- Catalog reads from Ducklake SQL metadata tables.
- Metadata APIs: list schemas/tables, table metadata, column handles.
- Read path uses Trino file system + Trino Parquet reader.
- Split planning from `ducklake_data_file`.
- Stats pruning from `ducklake_file_column_stats`.
- Partition pruning from `ducklake_partition_info`, `ducklake_partition_column`, `ducklake_file_partition_value`.
- Merge-on-read delete file filtering.
- Planner hooks: `applyFilter` and propagated table-handle predicates.
- Runtime predicate narrowing: split domain intersected with dynamic filter in page source.
- Table stats exposure via `ducklake_table_stats` + `ducklake_file_column_stats`.

## Type Support
- Solid: primitives, arrays, row/struct, map, nested combinations.
- Degraded placeholders:
  - `json` -> `VARCHAR`
  - `variant` -> `VARCHAR`
  - geometry family -> `VARBINARY`

## Known Gaps
- Time travel/table-version semantics are not implemented.
- Schema evolution safety gap: missing top-level Parquet column currently throws instead of returning nulls.
- Temporal partition transform semantics in code do not match the checked-in Ducklake spec definitions.
- Aggregated column min/max stats are computed lexicographically (string min/max), not typed.
- No query-runner coverage yet (unit coverage only).
- No write support.

## Scope Status
- Connector is read-only and usable for core scan flows.
- Not yet at full Iceberg/Hive parity for correctness edge cases and validation depth.

## Next Execution Plan
See [REMEDIATION_PLAN.md](REMEDIATION_PLAN.md).
