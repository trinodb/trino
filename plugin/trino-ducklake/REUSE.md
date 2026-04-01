# Ducklake Reuse Status

Goal: maximize reuse of public Trino/Iceberg/Hive reader infrastructure and keep Ducklake-specific code focused on SQL catalog semantics.

## Directly Reused
- Trino file system stack (`TrinoFileSystem`, `TrinoFileSystemFactory`, `Location`).
- Trino Parquet stack (`ParquetReader`, `ParquetPageSource`, row-group pruning via `getFilteredRowGroups`, page-level filtering, reader options).
- Hive Parquet datasource factory (`ParquetPageSourceFactory.createDataSource`).
- Hive `TransformConnectorPageSource` for schema-evolution null column injection.
- Standard SPI components (`FixedSplitSource`, `InMemoryRecordSet`, `RecordPageSource`, connector interfaces, classloader-safe wrappers).
- Standard metrics/memory/context infrastructure.

## Adapted Patterns
- Connector factory/module/page-source factory patterns adapted from Iceberg/Hive connector style.
- Predicate/split/page-source flow mirrors Iceberg structure at a high level.

## Ducklake-Specific (Expected)
- SQL catalog access layer.
- Ducklake metadata models and path resolution.
- Ducklake type string parsing and mapping.

## P2.1 Reuse Evaluation (Completed)

Four areas were reviewed for reuse opportunities:

### 1. Parquet Field Construction — Intentionally Custom
`DucklakeParquetTypeUtils.constructField()` overlaps with `ParquetTypeUtils.constructField()` in trino-parquet.
**Decision: Keep custom.**
- The base version carries variant type handling, backward-compat repeated-field edge cases, and Hive-specific logic that Ducklake doesn't need.
- The Ducklake version is simpler (~60 lines), well-tested across all nested types, and easy to reason about.
- Risk of adopting: base changes for Hive/variant could subtly break Ducklake reads.
- Benefit of adopting: marginal (less code, but more complexity and coupling).

### 2. Delete File Handling — Intentionally Custom
Ducklake uses a simple `DeleteRowFilterTransform` + `readDeletedRows()` (~90 lines).
Iceberg has `DeleteManager`, `PositionDeleteReader`, `DeletionVector`, `EqualityDeleteFilter`.
**Decision: Keep custom.**
- Ducklake delete model: single INT64 row-ID column per delete file.
- Iceberg delete model: position deletes keyed by file path, equality deletes, PUFFIN deletion vectors.
- The models are fundamentally different. Adapting to Iceberg's interface would require fake file-path keys and unused equality delete infrastructure.
- Ducklake's implementation is simple, correct, and well-tested.

### 3. Type Conversion — Intentionally Custom
`DucklakeTypeConverter` parses Ducklake SQL type strings (e.g., `"struct<key:varchar,value:int32>"`) into Trino types.
Iceberg's `TypeConverter` converts from `org.apache.iceberg.types.Type` objects.
**Decision: Keep custom.**
- Different source formats (string parsing vs object tree traversal).
- Ducklake handles DuckDB-specific types (uint*, geometry, json, variant) that Iceberg doesn't have.
- No meaningful code sharing possible without an abstraction that costs more than it saves.

### 4. Split Manager & Metadata — Architecturally Different
Ducklake queries SQL catalog tables; Iceberg reads manifest files.
**Decision: Keep custom (by design).**
- Snapshot selection, delete file discovery, and partition metadata all differ at the model level.
- These are architectural differences, not duplicated implementations.

## Summary
Reuse is strong where it matters (Parquet reader, file system, SPI, page source infrastructure). Custom code is limited to catalog semantics, type mapping, and simple delete handling — all areas where Iceberg's abstractions don't align with Ducklake's simpler metadata model. No actionable reuse gaps remain.
