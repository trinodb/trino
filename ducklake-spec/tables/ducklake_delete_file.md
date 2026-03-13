---
layout: docu
title: ducklake_delete_file
---

Delete files contain the row ids of rows that are deleted. Each data file will have its own delete file if any deletes are present for this data file.

| Column name        | Column type |             |
| ------------------ | ----------- | ----------- |
| `delete_file_id`   | `BIGINT`    | Primary key |
| `table_id`         | `BIGINT`    |             |
| `begin_snapshot`   | `BIGINT`    |             |
| `end_snapshot`     | `BIGINT`    |             |
| `data_file_id`     | `BIGINT`    |             |
| `path`             | `VARCHAR`   |             |
| `path_is_relative` | `BOOLEAN`   |             |
| `format`           | `VARCHAR`   |             |
| `delete_count`     | `BIGINT`    |             |
| `file_size_bytes`  | `BIGINT`    |             |
| `footer_size`      | `BIGINT`    |             |
| `encryption_key`   | `VARCHAR`   |             |
| `partial_max`      | `BIGINT`    |             |

- `delete_file_id` is the numeric identifier of the delete file. It is a primary key. `delete_file_id` is incremented from `next_file_id` in the [`ducklake_snapshot` table]({% link docs/preview/specification/tables/ducklake_snapshot.md %}).
- `table_id` refers to a `table_id` from the [`ducklake_table` table]({% link docs/preview/specification/tables/ducklake_table.md %}).
- `begin_snapshot` refers to a `snapshot_id` from the [`ducklake_snapshot` table]({% link docs/preview/specification/tables/ducklake_snapshot.md %}). The delete file is part of the table *starting with* this snapshot id.
- `end_snapshot` refers to a `snapshot_id` from the [`ducklake_snapshot` table]({% link docs/preview/specification/tables/ducklake_snapshot.md %}). The delete file is part of the table *up to but not including* this snapshot id. If `end_snapshot` is `NULL`, the delete file is currently part of the table.
- `data_file_id` refers to a `data_file_id` from the `ducklake_data_file` table.
- `path` is the file name of the delete file, e.g., `my_file-deletes.parquet` for a relative path.
- `path_is_relative` whether the `path` is relative to the [`path`]({% link docs/preview/specification/tables/ducklake_table.md %}) of the table (true) or an absolute path (false).
- `format` is the storage format of the delete file. Currently, only `parquet` is allowed.
- `delete_count` is the number of deletion records in the file.
- `file_size_bytes` is the size of the file in bytes.
- `footer_size` is the size of the file metadata footer, in the case of Parquet the Thrift data. This is an optimization that allows for faster reading of the file.
- `encryption_key` contains the encryption for the file if [encryption]({% link docs/preview/duckdb/advanced_features/encryption.md %}) is enabled.
- `partial_max` is the maximum snapshot id stored in a partial deletion file. When multiple deletes target the same data file across different snapshots, the deletion file is rewritten as a partial deletion file that tracks which rows were deleted in which snapshot. This column stores the highest snapshot id present in such a file. It is `NULL` for standard (non-partial) delete files.
