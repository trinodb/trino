---
layout: docu
title: ducklake_table_stats
---

This table contains table-level statistics.

| Column name       | Column type |             |
| ----------------- | ----------- | ----------- |
| `table_id`        | `BIGINT`    |             |
| `record_count`    | `BIGINT`    |             |
| `next_row_id`     | `BIGINT`    |             |
| `file_size_bytes` | `BIGINT`    |             |

- `table_id` refers to a `table_id` from the [`ducklake_table` table]({% link docs/preview/specification/tables/ducklake_table.md %}).
- `record_count` is the total amount of rows in the table. This can be approximate.
- `next_row_id` is the row id for newly inserted rows. Used for row lineage tracking.
- `file_size_bytes` is the total file size of all data files in the table. This can be approximate.
