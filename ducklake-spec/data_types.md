---
layout: docu
title: Data Types
---

DuckLake specifies multiple different data types for field values, and also supports nested types.
The types of columns are defined in the `column_type` field of the `ducklake_column` table.

## Primitive Types

| Type            | Description                                                                                  |
| --------------- | -------------------------------------------------------------------------------------------- |
| `boolean`       | True or false                                                                                |
| `int8`          | 8-bit signed integer                                                                         |
| `int16`         | 16-bit signed integer                                                                        |
| `int32`         | 32-bit signed integer                                                                        |
| `int64`         | 64-bit signed integer                                                                        |
| `uint8`         | 8-bit unsigned integer                                                                       |
| `uint16`        | 16-bit unsigned integer                                                                      |
| `uint32`        | 32-bit unsigned integer                                                                      |
| `uint64`        | 64-bit unsigned integer                                                                      |
| `float32`       | 32-bit [IEEE 754](https://en.wikipedia.org/wiki/IEEE_754) floating-point value               |
| `float64`       | 64-bit [IEEE 754](https://en.wikipedia.org/wiki/IEEE_754) floating-point value               |
| `decimal(P, S)` | Fixed-point decimal with precision `P` and scale `S`                                         |
| `time`          | Time of day, microsecond precision                                                           |
| `timetz`        | Time of day, microsecond precision, with time zone                                           |
| `date`          | Calendar date                                                                                |
| `timestamp`     | Timestamp, microsecond precision                                                             |
| `timestamptz`   | Timestamp, microsecond precision, with time zone                                             |
| `timestamp_s`   | Timestamp, second precision                                                                  |
| `timestamp_ms`  | Timestamp, millisecond precision                                                             |
| `timestamp_ns`  | Timestamp, nanosecond precision                                                              |
| `interval`      | Time interval in three different granularities: months, days, and milliseconds               |
| `varchar`       | Text                                                                                         |
| `blob`          | Binary data                                                                                  |
| `json`          | JSON                                                                                         |
| `uuid`          | [Universally unique identifier](https://en.wikipedia.org/wiki/Universally_unique_identifier) |

## Nested Types

DuckLake supports nested types and primitive types. Nested types are defined recursively, i.e.,
in order to define a column of type `INT[]` two columns are defined. The top-level column is of type `list`, which has a child column of type `int32`.

The following nested types are supported:

| Type     | Description                                   |
| -------- | --------------------------------------------- |
| `list`   | Collection of values with a single child type |
| `struct` | A tuple of typed values                       |
| `map`    | A collection of key-value pairs               |

## Semi-Structured Types

| Type      | Description                                                                                        |
| --------- | -------------------------------------------------------------------------------------------------- |
| `variant` | A dynamically typed value that can hold any primitive or nested type, stored in a binary encoding. |

The `variant` type is similar to JSON but is more strongly typed internally, supports a wider range of types (e.g., `date`, `timestamp`, `decimal`), and is stored in a binary-encoded format rather than as a string. Variants are stored in Parquet files according to the [Parquet variant encoding specification](https://github.com/apache/parquet-format/blob/master/VariantEncoding.md).

Variants can be **shredded** into their constituent primitive types when all rows share a consistent schema for a given sub-field. Shredded fields are stored and queried with the same efficiency as native primitive columns. Per-file statistics for shredded sub-fields are recorded in the [`ducklake_file_variant_stats`]({% link docs/preview/specification/tables/ducklake_file_variant_stats.md %}) table.

> Note The `variant` type is natively supported in DuckDB. For catalog databases that do not have a native variant type (e.g., PostgreSQL, SQLite), variants cannot yet be stored as inline values in those catalogs.

## Geometry Types

DuckLake supports geometry types using the `geometry` type of the Parquet format. The `geometry` type can store different types of spatial representations called geometry primitives, of which DuckLake supports the following:

| Geometry primitive   | Description                                                                                     |
| -------------------- | ----------------------------------------------------------------------------------------------- |
| `point`              | A single location in coordinate space.                                                          |
| `linestring`         | A sequence of points connected by straight line segments.                                       |
| `polygon`            | A planar surface defined by one exterior boundary and zero or more interior boundaries (holes). |
| `multipoint`         | A collection of `point` geometries.                                                             |
| `multilinestring`    | A collection of `linestring` geometries.                                                        |
| `multipolygon`       | A collection of `polygon` geometries.                                                           |
| `linestring z`       | A `linestring` geometry with an additional Z (elevation) coordinate for each point.             |
| `geometrycollection` | A heterogeneous collection of geometry primitives (e.g., points, lines, polygons, etc.).        |
