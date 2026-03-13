---
layout: docu
title: ducklake_file_partition_value
---

This table defines which data file belongs to which partition.

| Column name           | Column type |             |
| --------------------- | ----------- | ----------- |
| `data_file_id`        | `BIGINT`    |             |
| `table_id`            | `BIGINT`    |             |
| `partition_key_index` | `BIGINT`    |             |
| `partition_value`     | `VARCHAR`   |             |


- `data_file_id` refers to a `data_file_id` from the `ducklake_data_file` table.
- `table_id` refers to a `table_id` from the [`ducklake_table` table]({% link docs/preview/specification/tables/ducklake_table.md %}).
- `partition_key_index` refers to a `partition_key_index` from the `ducklake_partition_column` table.
- `partition_value` is the value that all the rows in the data file have, encoded as a string.
