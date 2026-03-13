---
layout: docu
title: ducklake_files_scheduled_for_deletion
---

Files that are no longer part of any snapshot are scheduled for deletion.

| Column name        | Column type                |             |
| ------------------ | -------------------------- | ----------- |
| `data_file_id`     | `BIGINT`                   |             |
| `path`             | `VARCHAR`                  |             |
| `path_is_relative` | `BOOLEAN`                  |             |
| `schedule_start`   | `TIMESTAMP`                |             |

- `data_file_id` refers to a `data_file_id` from the `ducklake_data_file` table.
- `path` is the file name of the file, e.g., `my_file.parquet`. The file name is either relative to the `data_path` value in `ducklake_metadata` or absolute. If relative, the `path_is_relative` field is set to `true`.
- `path_is_relative` defines whether the path is absolute or relative, see above.
- `schedule_start` is a timestamp of when this file was scheduled for deletion.
