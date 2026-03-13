---
layout: docu
title: ducklake_tag
---

Schemas, tables, and views, etc. can have tags, those are declared in this table.

| Column name      | Column type |             |
| ---------------- | ----------- | ----------- |
| `object_id`      | `BIGINT`    |             |
| `begin_snapshot` | `BIGINT`    |             |
| `end_snapshot`   | `BIGINT`    |             |
| `key`            | `VARCHAR`   |             |
| `value`          | `VARCHAR`   |             |

- `object_id` refers to a `schema_id`, `table_id`, etc. from various tables above.
- `begin_snapshot` refers to a `snapshot_id` from the [`ducklake_snapshot` table]({% link docs/preview/specification/tables/ducklake_snapshot.md %}). The tag is valid *starting with* this snapshot id.
- `end_snapshot` refers to a `snapshot_id` from the [`ducklake_snapshot` table]({% link docs/preview/specification/tables/ducklake_snapshot.md %}). The tag is valid *up to but not including* this snapshot id. If `end_snapshot` is `NULL`, the tag is currently valid.
- `key` is an arbitrary key string. The key can't be `NULL`.
- `value` is the arbitrary value string.
