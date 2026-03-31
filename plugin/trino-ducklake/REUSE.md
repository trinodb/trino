# Ducklake Reuse Status

Goal: maximize reuse of public Trino/Iceberg/Hive reader infrastructure and keep Ducklake-specific code focused on SQL catalog semantics.

## Directly Reused
- Trino file system stack (`TrinoFileSystem`, `TrinoFileSystemFactory`, `Location`).
- Trino Parquet stack (`ParquetReader`, `ParquetPageSource`, row-group pruning primitives, reader options).
- Hive Parquet datasource factory (`ParquetPageSourceFactory.createDataSource`).
- Standard SPI components (`FixedSplitSource`, connector interfaces, classloader-safe wrappers).
- Standard metrics/memory/context infrastructure.

## Adapted Patterns
- Connector factory/module/page-source factory patterns adapted from Iceberg/Hive connector style.
- Predicate/split/page-source flow mirrors Iceberg structure at a high level.

## Ducklake-Specific (Expected)
- SQL catalog access layer.
- Ducklake metadata models and path resolution.
- Ducklake type string parsing and mapping.

## Where Expectations Are Not Fully Met Yet
- No direct use of Iceberg delete utility classes (for example, `DeleteManager`, `PositionDeleteReader`).
- No direct use of Iceberg parquet column mapping helper (`IcebergParquetColumnIOConverter`).
- Some logic remains custom where reusable public Iceberg components likely exist.

## Practical Constraint
- Some high-value Iceberg classes are public but may pull in Iceberg-specific model assumptions.
- Reuse should be increased where interfaces align cleanly; otherwise keep a thin custom adapter with tests.

## Recommended Direction
- Prefer extracting shared helper(s) into common public utility modules when Ducklake and Iceberg need the same behavior but have different metadata models.
- Prioritize correctness and maintainability over forced reuse that introduces brittle adapter layers.
