---
layout: docu
title: ducklake_name_mapping
---

This table contains the information used to map a name to a [`column_id`]({% link docs/preview/specification/tables/ducklake_column.md %}) for a given [`mapping_id`]({% link docs/preview/specification/tables/ducklake_column_mapping.md %}) with the `map_by_name` type.

| Column name       | Column type |             |
| ----------------- | ----------- | ----------- |
| `mapping_id`      | `BIGINT`    |             |
| `column_id`       | `BIGINT`    |             |
| `source_name`     | `VARCHAR`   |             |
| `target_field_id` | `BIGINT`    |             |
| `parent_column`   | `BIGINT`    |             |

- `mapping_id` refers to a `mapping_id` from the [`ducklake_column_mapping` table]({% link docs/preview/specification/tables/ducklake_column_mapping.md %}).
- `column_id` refers to a `column_id` from the [`ducklake_column` table]({% link docs/preview/specification/tables/ducklake_column.md %}).
- `source_name` refers to the name of the field this mapping applies to.
- `target_field_id` refers to the `field-id` that a field with the `source_name` is mapped to.
- `parent_column` is the `column_id` of the parent column. This is `NULL` for top-level and non-nested columns. For example, for `STRUCT` types, this would refer to the "parent" struct column.
