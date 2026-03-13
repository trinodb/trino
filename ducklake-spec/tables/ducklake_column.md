---
layout: docu
title: ducklake_column
---

This table describes the columns that are part of a table, including their types, default values, etc.

| Column name       | Column type |             |
| ----------------- | ----------- | ----------- |
| `column_id`       | `BIGINT`    |             |
| `begin_snapshot`  | `BIGINT`    |             |
| `end_snapshot`    | `BIGINT`    |             |
| `table_id`        | `BIGINT`    |             |
| `column_order`    | `BIGINT`    |             |
| `column_name`     | `VARCHAR`   |             |
| `column_type`     | `VARCHAR`   |             |
| `initial_default` | `VARCHAR`   |             |
| `default_value`   | `VARCHAR`   |             |
| `nulls_allowed`   | `BOOLEAN`   |             |
| `parent_column`   | `BIGINT`    |             |

- `column_id` is the numeric identifier of the column. If the Parquet file includes a field identifier, it corresponds to the file's [`field_id`](https://github.com/apache/parquet-format/blob/f1fd3b9171aec7a7f0106e0203caef88d17dda82/src/main/thrift/parquet.thrift#L550). This identifier should remain consistent throughout all versions of the column, until it's dropped. The `column_id` must be unique per table.
- `begin_snapshot` refers to a `snapshot_id` from the [`ducklake_snapshot` table]({% link docs/preview/specification/tables/ducklake_snapshot.md %}). This version of the column exists *starting with* this snapshot id.
- `end_snapshot` refers to a `snapshot_id` from the [`ducklake_snapshot` table]({% link docs/preview/specification/tables/ducklake_snapshot.md %}). This version of the column exists *up to but not including* this snapshot id. If `end_snapshot` is `NULL`, this version of the column is currently valid.
- `table_id` refers to a `table_id` from the [`ducklake_table` table]({% link docs/preview/specification/tables/ducklake_table.md %}).
- `column_order` is a number that defines the position of the column in the list of columns. It needs to be unique within a snapshot but does not have to be contiguous (gaps are ok).
- `column_name` is the name of this version of the column, e.g., `my_column`.
- `column_type` is the type of this version of the column as defined in the list of [data types]({% link docs/preview/specification/data_types.md %}).
- `initial_default` is the *initial* default value as the column is being created, e.g., in `ALTER TABLE`, encoded as a string. Can be `NULL`.
- `default_value` is the *operational* default value as data is being inserted and updated, e.g., in `INSERT`, encoded as a string. Can be `NULL`.
- `nulls_allowed` defines whether `NULL` values are allowed in this version of the column. Note that default values have to be set if this is set to `false`.
- `parent_column` is the `column_id` of the parent column. This is `NULL` for top-level and non-nested columns. For example, for `STRUCT` types, this would refer to the "parent" struct column.

> Every `ALTER` of the column creates a new version of the column, which will use the same `column_id`.
