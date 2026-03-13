---
layout: docu
title: ducklake_column_mapping
---

Mappings contain the information used to map Parquet fields to column ids in the absence of `field-id`s in the Parquet file.

| Column name  | Column type |             |
| ------------ | ----------- | ----------- |
| `mapping_id` | `BIGINT`    |             |
| `table_id`   | `BIGINT`    |             |
| `type`       | `VARCHAR`   |             |

- `mapping_id` is the numeric identifier of the mapping. `mapping_id` is incremented from `next_catalog_id` in the `ducklake_snapshot` table.
- `table_id` refers to a `table_id` from the [`ducklake_table` table]({% link docs/preview/specification/tables/ducklake_table.md %}).
- `type` defines what method is used to perform the mapping.

The valid `type` values are the following:

| `type`        | Description |
| ------------- | ----------- |
| `map_by_name` | Map the columns based on the names in the Parquet file |
