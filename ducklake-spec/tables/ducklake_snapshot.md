---
layout: docu
title: ducklake_snapshot
---

This table contains the valid snapshots in a DuckLake.

| Column name       | Column type                |             |
| ----------------- | -------------------------- | ----------- |
| `snapshot_id`     | `BIGINT`                   | Primary key |
| `snapshot_time`   | `TIMESTAMP`                |             |
| `schema_version`  | `BIGINT`                   |             |
| `next_catalog_id` | `BIGINT`                   |             |
| `next_file_id`    | `BIGINT`                   |             |

- `snapshot_id` is the continuously increasing numeric identifier of the snapshot. It is a primary key and is referred to by various other tables.
- `snapshot_time` is the timestamp at which the snapshot was created.
- `schema_version` is a continuously increasing number that is incremented whenever the schema is changed, e.g., by creating a table. This allows for caching of schema information if only data is changed.
- `next_catalog_id` is a continuously increasing number that describes the next identifier for schemas, tables, views, partitions, and column name mappings. This is only changed if one of those entries is created, i.e., the schema is changing.
- `next_file_id` is a continuously increasing number that contains the next id for a data or deletion file to be added. It is only changed if data is being added or deleted, i.e., not for schema changes.
