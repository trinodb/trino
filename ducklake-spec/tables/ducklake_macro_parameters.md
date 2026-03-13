---
layout: docu
title: ducklake_macro_parameters
---

This table stores the parameters for each macro implementation.

| Column name          | Column type |
| -------------------- | ----------- |
| `macro_id`           | `BIGINT`    |
| `impl_id`            | `BIGINT`    |
| `column_id`          | `BIGINT`    |
| `parameter_name`     | `VARCHAR`   |
| `parameter_type`     | `VARCHAR`   |
| `default_value`      | `VARCHAR`   |
| `default_value_type` | `VARCHAR`   |

- `macro_id` refers to a `macro_id` from the [`ducklake_macro` table]({% link docs/preview/specification/tables/ducklake_macro.md %}).
- `impl_id` refers to an `impl_id` from the [`ducklake_macro_impl` table]({% link docs/preview/specification/tables/ducklake_macro_impl.md %}).
- `column_id` is the positional index of the parameter within the implementation.
- `parameter_name` is the name of the parameter.
- `parameter_type` is the [DuckLake type]({% link docs/preview/specification/data_types.md %}) of the parameter. Set to `unknown` if the parameter type is not specified.
- `default_value` is the default value of the parameter. Set to `NULL` if no default is specified.
- `default_value_type` is the [DuckLake type]({% link docs/preview/specification/data_types.md %}) of the default value. Set to `unknown` if no default is specified.
