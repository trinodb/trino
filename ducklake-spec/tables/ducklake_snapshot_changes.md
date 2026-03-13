---
layout: docu
title: ducklake_snapshot_changes
---

This table lists changes that happened in a snapshot for easier conflict detection.

| Column name         | Column type | Description                              |
| ------------------- | ----------- | ---------------------------------------- |
| `snapshot_id`       | `BIGINT`    | Primary key                              |
| `changes_made`      | `VARCHAR`   | List of changes in snapshot              |
| `author`            | `VARCHAR`   | Author of the snapshot                   |
| `commit_message`    | `VARCHAR`   | Commit message                           |
| `commit_extra_info` | `VARCHAR`   | Extra information regarding the commit   |

The `ducklake_snapshot_changes` table contains a summary of changes made by a snapshot. This table is used during [Conflict Resolution]({% link docs/preview/duckdb/advanced_features/conflict_resolution.md %}) to quickly find out if two snapshots have conflicting changesets.

* `snapshot_id` refers to a `snapshot_id` from the [`ducklake_snapshot` table]({% link docs/preview/specification/tables/ducklake_snapshot.md %}).
* `changes_made` is a comma-separated list of high-level changes made by the snapshot. The values that are contained in this list have the following format:
    * `created_schema:⟨schema_name⟩`{:.language-sql .highlight} – the snapshot created a schema with the given name.
    * `created_table:⟨table_name⟩`{:.language-sql .highlight} – the snapshot created a table with the given name.
    * `created_view:⟨view_name⟩`{:.language-sql .highlight} – the snapshot created a view with the given name.
    * `inserted_into_table:⟨table_id⟩`{:.language-sql .highlight} – the snapshot inserted data into the given table.
    * `deleted_from_table:⟨table_id⟩`{:.language-sql .highlight} – the snapshot deleted data from the given table.
    * `compacted_table:⟨table_id⟩`{:.language-sql .highlight} – the snapshot run a compaction operation on the given table.
    * `dropped_schema:⟨schema_id⟩`{:.language-sql .highlight} – the snapshot dropped the given schema.
    * `dropped_table:⟨table_id⟩`{:.language-sql .highlight} – the snapshot dropped the given table.
    * `dropped_view:⟨view_id⟩`{:.language-sql .highlight} – the snapshot dropped the given view.
    * `altered_table:⟨table_id⟩`{:.language-sql .highlight} – the snapshot altered the given table.
    * `altered_view:⟨view_id⟩`{:.language-sql .highlight} – the snapshot altered the given view.

> Names are written in quoted-format using SQL-style escapes, i.e., the name `this "table" contains quotes` is written as `"this ""table"" contains quotes"`.
