---
layout: docu
title: ducklake_column_tag
---

Columns can also have tags, those are defined in this table.

| Column name      | Column type |             |
| ---------------- | ----------- | ----------- |
| `table_id`       | `BIGINT`    |             |
| `column_id`      | `BIGINT`    |             |
| `begin_snapshot` | `BIGINT`    |             |
| `end_snapshot`   | `BIGINT`    |             |
| `key`            | `VARCHAR`   |             |
| `value`          | `VARCHAR`   |             |

- `table_id` refers to a `table_id` from the [`ducklake_table` table]({% link docs/preview/specification/tables/ducklake_table.md %}).
- `column_id` refers to a `column_id` from the [`ducklake_column` table]({% link docs/preview/specification/tables/ducklake_column.md %}).
- `begin_snapshot` refers to a `snapshot_id` from the [`ducklake_snapshot` table]({% link docs/preview/specification/tables/ducklake_snapshot.md %}). The tag is valid *starting with* this snapshot id.
- `end_snapshot` refers to a `snapshot_id` from the [`ducklake_snapshot` table]({% link docs/preview/specification/tables/ducklake_snapshot.md %}). The tag is valid *up to but not including* this snapshot id. If `end_snapshot` is `NULL`, the tag is currently valid.
- `key` is an arbitrary key string. The key can't be `NULL`.
- `value` is the arbitrary value string.
