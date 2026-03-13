---
layout: docu
title: ducklake_file_variant_stats
---

This table contains per-file statistics for the shredded sub-fields of `variant` columns.

| Column name         | Column type |             |
| ------------------- | ----------- | ----------- |
| `data_file_id`      | `BIGINT`    |             |
| `table_id`          | `BIGINT`    |             |
| `column_id`         | `BIGINT`    |             |
| `variant_path`      | `VARCHAR`   |             |
| `shredded_type`     | `VARCHAR`   |             |
| `column_size_bytes` | `BIGINT`    |             |
| `value_count`       | `BIGINT`    |             |
| `null_count`        | `BIGINT`    |             |
| `min_value`         | `VARCHAR`   |             |
| `max_value`         | `VARCHAR`   |             |
| `contains_nan`      | `BOOLEAN`   |             |
| `extra_stats`       | `VARCHAR`   |             |

- `data_file_id` refers to a `data_file_id` from the [`ducklake_data_file` table]({% link docs/preview/specification/tables/ducklake_data_file.md %}).
- `table_id` refers to a `table_id` from the [`ducklake_table` table]({% link docs/preview/specification/tables/ducklake_table.md %}).
- `column_id` refers to a `column_id` from the [`ducklake_column` table]({% link docs/preview/specification/tables/ducklake_column.md %}) and identifies the `variant` column that contains the shredded field.
- `variant_path` is the path to the shredded sub-field within the variant. Named fields are always quoted (e.g., `"l_orderkey"`), and any quote characters within a field name are escaped by doubling them. Two special path values exist:
    * `root` — the variant value itself is a primitive (i.e., not nested).
    * `element` — the statistics apply to elements of an array-typed variant.
    * Paths can be composed, e.g., `element."a"` refers to the `a` field of each element of an array.
- `shredded_type` is the DuckLake type name of the shredded field (e.g., `int64`, `date`, `decimal(15,2)`, `varchar`).
- `column_size_bytes` is the byte size of the shredded field in this file.
- `value_count` is the number of non-null values in the shredded field.
- `null_count` is the number of rows where the shredded field is `NULL` or absent.
- `min_value` contains the minimum value for the shredded field, encoded as a string.
- `max_value` contains the maximum value for the shredded field, encoded as a string.
- `contains_nan` is a flag whether the shredded field contains any `NaN` values. Only relevant for floating-point types.
- `extra_stats` is reserved for additional type-specific statistics.

A row is written to this table for every **fully shredded** sub-field of a `variant` column in a data file. A sub-field is considered fully shredded when, for every row in the file, the field is either present as a primitive of a single consistent type, absent, or `NULL`. Fields that mix types across rows are not recorded.

Global (table-wide) statistics for shredded variant fields that are consistently shredded across every file are stored in the `extra_stats` column of the [`ducklake_table_column_stats`]({% link docs/preview/specification/tables/ducklake_table_column_stats.md %}) table, encoded as JSON.
