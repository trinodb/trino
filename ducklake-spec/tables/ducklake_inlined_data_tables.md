---
layout: docu
title: ducklake_inlined_data_tables
---

This table links DuckLake snapshots with [inlined data tables]({% link docs/preview/duckdb/advanced_features/data_inlining.md %}).

| Column name       | Column type |             |
| ----------------- | ----------- | ----------- |
| `table_id`        | `BIGINT`    |             |
| `table_name`      | `VARCHAR`   |             |
| `schema_version`  | `BIGINT`    |             |

- `table_id` refers to a `table_id` from the [`ducklake_table` table]({% link docs/preview/specification/tables/ducklake_table.md %}).
- `table_name` is a string that names the data table for inlined data.
- `schema_version` refers to a schema version in the [`ducklake_snapshot` table]({% link docs/preview/specification/tables/ducklake_snapshot.md %}).
