---
layout: docu
title: ducklake_schema
---

This table defines valid schemas.

| Column name        | Column type |             |
| ------------------ | ----------- | ----------- |
| `schema_id`        | `BIGINT`    | Primary key |
| `schema_uuid`      | `UUID`      |             |
| `begin_snapshot`   | `BIGINT`    |             |
| `end_snapshot`     | `BIGINT`    |             |
| `schema_name`      | `VARCHAR`   |             |
| `path`             | `VARCHAR`   |             |
| `path_is_relative` | `BOOLEAN`   |             |

- `schema_id` is the numeric identifier of the schema. `schema_id` is incremented from `next_catalog_id` in the [`ducklake_snapshot` table]({% link docs/preview/specification/tables/ducklake_snapshot.md %}).
- `schema_uuid` is a UUID that gives a persistent identifier for this schema. The UUID is stored here for compatibility with existing lakehouse formats.
- `begin_snapshot` refers to a `snapshot_id` from the [`ducklake_snapshot` table]({% link docs/preview/specification/tables/ducklake_snapshot.md %}). The schema exists *starting with* this snapshot id.
- `end_snapshot` refers to a `snapshot_id` from the [`ducklake_snapshot` table]({% link docs/preview/specification/tables/ducklake_snapshot.md %}). The schema exists *up to but not including* this snapshot id. If `end_snapshot` is `NULL`, the schema is currently valid.
- `schema_name` is the name of the schema, e.g., `my_schema`.
- `path` is the `data_path` of the schema.
- `path_is_relative` whether the `path` is relative to the [`data_path`]({% link docs/preview/specification/tables/ducklake_metadata.md %}) of the catalog (true) or an absolute path (false).
