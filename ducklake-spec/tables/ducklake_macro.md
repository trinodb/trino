---
layout: docu
title: ducklake_macro
---

This table stores macro definitions. Each macro is associated with a schema and tracks its lifecycle through snapshots.

| Column name      | Column type |
| ---------------- | ----------- |
| `schema_id`      | `BIGINT`    |
| `macro_id`       | `BIGINT`    |
| `macro_name`     | `VARCHAR`   |
| `begin_snapshot` | `BIGINT`    |
| `end_snapshot`   | `BIGINT`    |

- `schema_id` refers to a `schema_id` from the [`ducklake_schema` table]({% link docs/preview/specification/tables/ducklake_schema.md %}).
- `macro_id` is the numeric identifier of the macro. `macro_id` is incremented from `next_catalog_id` in the [`ducklake_snapshot` table]({% link docs/preview/specification/tables/ducklake_snapshot.md %}).
- `macro_name` is the name of the macro, e.g., `my_macro`.
- `begin_snapshot` refers to a `snapshot_id` from the [`ducklake_snapshot` table]({% link docs/preview/specification/tables/ducklake_snapshot.md %}). The macro exists *starting with* this snapshot id.
- `end_snapshot` refers to a `snapshot_id` from the [`ducklake_snapshot` table]({% link docs/preview/specification/tables/ducklake_snapshot.md %}). The macro exists *up to but not including* this snapshot id. If `end_snapshot` is `NULL`, the macro is currently valid.
