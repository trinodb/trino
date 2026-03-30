# Ducklake Connector - Revised Plan (Current)

## Strategy

Keep the Ducklake connector thin and reuse Trino/Iceberg/Hive reader infrastructure where possible:
- Reuse Trino file system and Parquet reader path.
- Reuse Iceberg/Hive connector patterns for split/page-source wiring.
- Build only Ducklake-specific SQL catalog logic and path/snapshot semantics.

## Current State

### Implemented
- SQL catalog reads for snapshots/schemas/tables/columns/data files.
- Primitive and `list<primitive>` type reconstruction + Trino type mapping.
- Split generation and hierarchical relative path resolution (catalog -> schema -> table -> file).
- Connector page-source-provider factory wiring.
- Catalog-focused unit tests for schema/table/files/list type/predicate pruning behavior.

### Intentionally Deferred
- Root Trino reactor integration for the plugin module.
- Packaging/distribution flow for plugin ZIP.
- End-to-end Trino server smoke validation.

## Active Roadmap

Detailed parity checklist lives in `READ_ONLY_TODO.md`.
Current execution target: **P0 correctness - delete-file application**.

### Phase A: Catalog-first hardening (current)
- Expand catalog tests before broader plugin validation.
- Keep reading current/all data (no time-travel query semantics yet).
- Track time travel explicitly as TODO.

### Phase B: Predicate integration
- Wire `ducklake_file_column_stats` pruning from metadata to split planning.
- Preserve Parquet reader behavior for additional in-file pruning.

### Phase C: Delete-file handling
- Adapt Iceberg delete-application patterns to Ducklake delete metadata/files.
- Ensure merge-on-read semantics are correct before broad integration testing.

### Phase D: Trino integration and packaging (later)
- Enable module wiring in root build when ready.
- Run manual/automated Trino smoke tests.
- Decide packaging/runtime dependency strategy (SQLite path likely test-only).
