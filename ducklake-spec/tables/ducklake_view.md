---
layout: docu
title: ducklake_view
---

This table describes SQL-style `VIEW` definitions.

| Column name      | Column type |             |
| ---------------- | ----------- | ----------- |
| `view_id`        | `BIGINT`    |             |
| `view_uuid`      | `UUID`      |             |
| `begin_snapshot` | `BIGINT`    |             |
| `end_snapshot`   | `BIGINT`    |             |
| `schema_id`      | `BIGINT`    |             |
| `view_name`      | `VARCHAR`   |             |
| `dialect`        | `VARCHAR`   |             |
| `sql`            | `VARCHAR`   |             |
| `column_aliases` | `VARCHAR`   |             |

- `view_id` is the numeric identifier of the view.  `view_id` is incremented from `next_catalog_id` in the [`ducklake_snapshot` table]({% link docs/preview/specification/tables/ducklake_snapshot.md %}).
- `view_uuid` is a UUID that gives a persistent identifier for this view. The UUID is stored here for compatibility with existing lakehouse formats.
- `begin_snapshot` refers to a `snapshot_id` from the [`ducklake_snapshot` table]({% link docs/preview/specification/tables/ducklake_snapshot.md %}). The view exists *starting with* this snapshot id.
- `end_snapshot` refers to a `snapshot_id` from the [`ducklake_snapshot` table]({% link docs/preview/specification/tables/ducklake_snapshot.md %}). The view exists *up to but not including* this snapshot id. If `end_snapshot` is `NULL`, the view is currently valid.
- `schema_id` refers to a `schema_id` from the [`ducklake_schema` table]({% link docs/preview/specification/tables/ducklake_schema.md %}).
- `view_name` is the name of the view, e.g., `my_view`.
- `dialect` is the SQL dialect of the view definition, e.g., `duckdb`.
- `sql` is the SQL string that defines the view, e.g., `SELECT * FROM my_table`.
- `column_aliases` contains a possible rename of the view columns. Can be `NULL` if no rename is set.
