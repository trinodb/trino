---
layout: docu
title: ducklake_sort_info
---

The `ducklake_sort_info` table records the version history of sort settings for tables. Each row represents one sort configuration applied to a table, with snapshot-based validity tracking.

| Column name      | Column type |             |
| ---------------- | ----------- | ----------- |
| `sort_id`        | `BIGINT`    |             |
| `table_id`       | `BIGINT`    |             |
| `begin_snapshot` | `BIGINT`    |             |
| `end_snapshot`   | `BIGINT`    |             |

- `sort_id` is a numeric identifier for a sort configuration. `sort_id` is allocated from `next_catalog_id` in the [`ducklake_snapshot` table]({% link docs/preview/specification/tables/ducklake_snapshot.md %}).
- `table_id` refers to a `table_id` from the [`ducklake_table` table]({% link docs/preview/specification/tables/ducklake_table.md %}).
- `begin_snapshot` refers to a `snapshot_id` from the [`ducklake_snapshot` table]({% link docs/preview/specification/tables/ducklake_snapshot.md %}). The sort configuration is valid *starting with* this snapshot id.
- `end_snapshot` refers to a `snapshot_id` from the [`ducklake_snapshot` table]({% link docs/preview/specification/tables/ducklake_snapshot.md %}). The sort configuration is valid *up to but not including* this snapshot id. If `end_snapshot` is `NULL`, the sort configuration is currently active.

The expressions associated with a sort configuration are stored in [`ducklake_sort_expression`]({% link docs/preview/specification/tables/ducklake_sort_expression.md %}).
