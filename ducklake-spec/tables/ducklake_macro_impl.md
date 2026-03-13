---
layout: docu
title: ducklake_macro_impl
---

This table stores macro implementations. A single macro can have multiple implementations.

| Column name | Column type |
| ----------- | ----------- |
| `macro_id`  | `BIGINT`    |
| `impl_id`   | `BIGINT`    |
| `dialect`   | `VARCHAR`   |
| `sql`       | `VARCHAR`   |
| `type`      | `VARCHAR`   |

- `macro_id` refers to a `macro_id` from the [`ducklake_macro` table]({% link docs/preview/specification/tables/ducklake_macro.md %}).
- `impl_id` is the numeric identifier of the implementation within the macro.
- `dialect` is the SQL dialect of the macro implementation, e.g., `duckdb`.
- `sql` is the SQL expression or query that defines the macro body.
- `type` is the type of macro: `scalar` for scalar macros or `table` for table macros.
