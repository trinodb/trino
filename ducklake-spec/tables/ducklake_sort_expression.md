---
layout: docu
title: ducklake_sort_expression
---

The `ducklake_sort_expression` table stores the individual sort key expressions for each sort configuration. Each row corresponds to one expression in a `SET SORTED BY` clause.

| Column name       | Column type |             |
| ----------------- | ----------- | ----------- |
| `sort_id`         | `BIGINT`    |             |
| `table_id`        | `BIGINT`    |             |
| `sort_key_index`  | `BIGINT`    |             |
| `expression`      | `VARCHAR`   |             |
| `dialect`         | `VARCHAR`   |             |
| `sort_direction`  | `VARCHAR`   |             |
| `null_order`      | `VARCHAR`   |             |

- `sort_id` refers to a `sort_id` from the [`ducklake_sort_info` table]({% link docs/preview/specification/tables/ducklake_sort_info.md %}).
- `table_id` refers to a `table_id` from the [`ducklake_table` table]({% link docs/preview/specification/tables/ducklake_table.md %}).
- `sort_key_index` defines the position of this expression within the sort key using 0-based indexing. For example, in `SET SORTED BY (a ASC, b DESC, c ASC)` the `sort_key_index` of `b` is `1`.
- `expression` is the sort expression as a string. Currently only column names are supported.
- `dialect` identifies the SQL dialect used to interpret `expression`. Currently always `duckdb`.
- `sort_direction` is either `ASC` or `DESC`.
- `null_order` is either `NULLS_FIRST` or `NULLS_LAST`.
