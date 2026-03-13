---
layout: docu
title: ducklake_partition_column
---

Partitions can refer to one or more columns, possibly with transformations such as hashing or bucketing.

| Column name           | Column type |             |
| --------------------- | ----------- | ----------- |
| `partition_id`        | `BIGINT`    |             |
| `table_id`            | `BIGINT`    |             |
| `partition_key_index` | `BIGINT`    |             |
| `column_id`           | `BIGINT`    |             |
| `transform`           | `VARCHAR`   |             |

- `partition_id` refers to a `partition_id` from the `ducklake_partition_info` table.
- `table_id` refers to a `table_id` from the [`ducklake_table` table]({% link docs/preview/specification/tables/ducklake_table.md %}).
- `partition_key_index` defines where in the partition key the column is using 0-based indexing. For example, in a partitioning by (`a`, `b`, `c`) the `partition_key_index` of `b` would be `1`.
- `column_id` refers to a `column_id` from the [`ducklake_column` table]({% link docs/preview/specification/tables/ducklake_column.md %}).
- `transform` defines the type of a transform that is applied to the column value, e.g., `year`.

The table of supported transforms is as follows.

| Transform             |                                    Source type(s)                                 |                           Description                        | Result&nbsp;type |
| --------------------- | --------------------------------------------------------------------------------- | ------------------------------------------------------------ | ----------- |
| `identity`            | Any                                                                               | Source value, unmodified                                     | Source type |
| `year`                | `date`, `timestamp`, `timestamptz`, `timestamp_s`, `timestamp_ms`, `timestamp_ns` | Extract a date or timestamp year, as years from 1970         | `int64`     |
| `month`               | `date`, `timestamp`, `timestamptz`, `timestamp_s`, `timestamp_ms`, `timestamp_ns` | Extract a date or timestamp month, as months from 1970-01-01 | `int64`     |
| `day`                 | `date`, `timestamp`, `timestamptz`, `timestamp_s`, `timestamp_ms`, `timestamp_ns` | Extract a date or timestamp day, as days from 1970-01-01     | `int64`     |
| `hour`                | `timestamp`, `timestamptz`, `timestamp_s`, `timestamp_ms`, `timestamp_ns`         | Extract a timestamp hour, as hours from 1970-01-01 00:00:00  | `int64`     |
