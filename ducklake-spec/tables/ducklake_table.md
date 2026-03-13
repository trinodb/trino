---
layout: docu
title: ducklake_table
---

This table describes tables. Inception!

| Column name        | Column type |             |
| ------------------ | ----------- | ----------- |
| `table_id`         | `BIGINT`    |             |
| `table_uuid`       | `UUID`      |             |
| `begin_snapshot`   | `BIGINT`    |             |
| `end_snapshot`     | `BIGINT`    |             |
| `schema_id`        | `BIGINT`    |             |
| `table_name`       | `VARCHAR`   |             |
| `path`             | `VARCHAR`   |             |
| `path_is_relative` | `BOOLEAN`   |             |

- `table_id` is the numeric identifier of the table. `table_id` is incremented from `next_catalog_id` in the `ducklake_snapshot` table.
- `table_uuid` is a UUID that gives a persistent identifier for this table. The UUID is stored here for compatibility with existing lakehouse formats.
- `begin_snapshot` refers to a `snapshot_id` from the `ducklake_snapshot` table. The table exists *starting with* this snapshot id.
- `end_snapshot` refers to a `snapshot_id` from the `ducklake_snapshot` table. The table exists *up to but not including* this snapshot id. If `end_snapshot` is `NULL`, the table is currently valid.
- `schema_id` refers to a `schema_id` from the `ducklake_schema` table.
- `table_name` is the name of the table, e.g., `my_table`.
- `path` is the `data_path` of the table.
- `path_is_relative` whether the `path` is relative to the [`path`]({% link docs/preview/specification/tables/ducklake_schema.md %}) of the schema (true) or an absolute path (false).
