---
layout: docu
title: ducklake_table_column_stats
---

This table contains column-level statistics for an entire table.

| Column name     | Column type |             |
| --------------- | ----------- | ----------- |
| `table_id`      | `BIGINT`    |             |
| `column_id`     | `BIGINT`    |             |
| `contains_null` | `BOOLEAN`   |             |
| `contains_nan`  | `BOOLEAN`   |             |
| `min_value`     | `VARCHAR`   |             |
| `max_value`     | `VARCHAR`   |             |
| `extra_stats`   | `VARCHAR`   |             |

- `table_id` refers to a `table_id` from the [`ducklake_table` table]({% link docs/preview/specification/tables/ducklake_table.md %}).
- `column_id` refers to a `column_id` from the [`ducklake_column` table]({% link docs/preview/specification/tables/ducklake_column.md %}).
- `contains_null` is a flag whether the column contains any `NULL` values.
- `contains_nan` is a flag whether the column contains any `NaN` values. This is only relevant for floating-point types.
- `min_value` contains the minimum value for the column, encoded as a string. This does not have to be exact but has to be a lower bound. The value has to be cast to the actual type for accurate comparison, e.g., on integer types.
- `max_value` contains the maximum value for the column, encoded as a string. This does not have to be exact but has to be an upper bound. The value has to be cast to the actual type for accurate comparison, e.g., on integer types.
- `extra_stats` contains additional type-specific statistics, such as bounding box information for geometry types or global shredded-field statistics for `variant` columns encoded as JSON. Variant global stats are populated only for sub-fields that are consistently shredded across every data file; if any file contains inconsistent data for a field, global stats for that field are omitted.
