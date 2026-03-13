---
layout: docu
title: Tables
---

DuckLake 0.4 uses 28 tables to store metadata and to stage data fragments for data inlining. Below we describe all those tables and their semantics.

The following figure shows the most important 11 tables defined by the DuckLake schema:

![DuckLake schema](/images/schema/ducklake-schema-v0.3.png){: .lightmode-img }
![DuckLake schema](/images/schema/dark/ducklake-schema-v0.3.png){: .darkmode-img }

## Snapshots

* [`ducklake_snapshot`]({% link docs/preview/specification/tables/ducklake_snapshot.md %})
* [`ducklake_snapshot_changes`]({% link docs/preview/specification/tables/ducklake_snapshot_changes.md %})

## DuckLake Schema

* [`ducklake_schema`]({% link docs/preview/specification/tables/ducklake_schema.md %})
* [`ducklake_table`]({% link docs/preview/specification/tables/ducklake_table.md %})
* [`ducklake_view`]({% link docs/preview/specification/tables/ducklake_view.md %})
* [`ducklake_column`]({% link docs/preview/specification/tables/ducklake_column.md %})

## Macros

* [`ducklake_macro`]({% link docs/preview/specification/tables/ducklake_macro.md %})
* [`ducklake_macro_impl`]({% link docs/preview/specification/tables/ducklake_macro_impl.md %})
* [`ducklake_macro_parameters`]({% link docs/preview/specification/tables/ducklake_macro_parameters.md %})

## Data Files and Tables

* [`ducklake_data_file`]({% link docs/preview/specification/tables/ducklake_data_file.md %})
* [`ducklake_delete_file`]({% link docs/preview/specification/tables/ducklake_delete_file.md %})
* [`ducklake_files_scheduled_for_deletion`]({% link docs/preview/specification/tables/ducklake_files_scheduled_for_deletion.md %})
* [`ducklake_inlined_data_tables`]({% link docs/preview/specification/tables/ducklake_inlined_data_tables.md %})

## Data File Mapping

* [`ducklake_column_mapping`]({% link docs/preview/specification/tables/ducklake_column_mapping.md %})
* [`ducklake_name_mapping`]({% link docs/preview/specification/tables/ducklake_name_mapping.md %})

## Statistics

DuckLake supports statistics on the table, column and file level.

* [`ducklake_table_stats`]({% link docs/preview/specification/tables/ducklake_table_stats.md %})
* [`ducklake_table_column_stats`]({% link docs/preview/specification/tables/ducklake_table_column_stats.md %})
* [`ducklake_file_column_stats`]({% link docs/preview/specification/tables/ducklake_file_column_stats.md %})
* [`ducklake_file_variant_stats`]({% link docs/preview/specification/tables/ducklake_file_variant_stats.md %})

## Partitioning Information

DuckLake supports defining explicit partitioning.

* [`ducklake_partition_info`]({% link docs/preview/specification/tables/ducklake_partition_info.md %})
* [`ducklake_partition_column`]({% link docs/preview/specification/tables/ducklake_partition_column.md %})
* [`ducklake_file_partition_value`]({% link docs/preview/specification/tables/ducklake_file_partition_value.md %})

## Sort Information

DuckLake supports defining a sort order for tables to improve query performance.

* [`ducklake_sort_info`]({% link docs/preview/specification/tables/ducklake_sort_info.md %})
* [`ducklake_sort_expression`]({% link docs/preview/specification/tables/ducklake_sort_expression.md %})

## Auxiliary Tables

* [`ducklake_metadata`]({% link docs/preview/specification/tables/ducklake_metadata.md %})
* [`ducklake_tag`]({% link docs/preview/specification/tables/ducklake_tag.md %})
* [`ducklake_column_tag`]({% link docs/preview/specification/tables/ducklake_column_tag.md %})
* [`ducklake_schema_versions`]({% link docs/preview/specification/tables/ducklake_schema_versions.md %})

## Full Schema Creation Script

Below is the full SQL script to create a DuckLake metadata database:

```sql
CREATE TABLE ducklake_metadata (key VARCHAR NOT NULL, value VARCHAR NOT NULL, scope VARCHAR, scope_id BIGINT);
CREATE TABLE ducklake_snapshot (snapshot_id BIGINT PRIMARY KEY, snapshot_time TIMESTAMPTZ, schema_version BIGINT, next_catalog_id BIGINT, next_file_id BIGINT);
CREATE TABLE ducklake_snapshot_changes (snapshot_id BIGINT PRIMARY KEY, changes_made VARCHAR, author VARCHAR, commit_message VARCHAR, commit_extra_info VARCHAR);
CREATE TABLE ducklake_schema (schema_id BIGINT PRIMARY KEY, schema_uuid UUID, begin_snapshot BIGINT, end_snapshot BIGINT, schema_name VARCHAR, path VARCHAR, path_is_relative BOOLEAN);
CREATE TABLE ducklake_table (table_id BIGINT, table_uuid UUID, begin_snapshot BIGINT, end_snapshot BIGINT, schema_id BIGINT, table_name VARCHAR, path VARCHAR, path_is_relative BOOLEAN);
CREATE TABLE ducklake_view (view_id BIGINT, view_uuid UUID, begin_snapshot BIGINT, end_snapshot BIGINT, schema_id BIGINT, view_name VARCHAR, dialect VARCHAR, sql VARCHAR, column_aliases VARCHAR);
CREATE TABLE ducklake_tag (object_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, key VARCHAR, value VARCHAR);
CREATE TABLE ducklake_column_tag (table_id BIGINT, column_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, key VARCHAR, value VARCHAR);
CREATE TABLE ducklake_data_file (data_file_id BIGINT PRIMARY KEY, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, file_order BIGINT, path VARCHAR, path_is_relative BOOLEAN, file_format VARCHAR, record_count BIGINT, file_size_bytes BIGINT, footer_size BIGINT, row_id_start BIGINT, partition_id BIGINT, encryption_key VARCHAR, mapping_id BIGINT, partial_max BIGINT);
CREATE TABLE ducklake_file_column_stats (data_file_id BIGINT, table_id BIGINT, column_id BIGINT, column_size_bytes BIGINT, value_count BIGINT, null_count BIGINT, min_value VARCHAR, max_value VARCHAR, contains_nan BOOLEAN, extra_stats VARCHAR);
CREATE TABLE ducklake_file_variant_stats (data_file_id BIGINT, table_id BIGINT, column_id BIGINT, variant_path VARCHAR, shredded_type VARCHAR, column_size_bytes BIGINT, value_count BIGINT, null_count BIGINT, min_value VARCHAR, max_value VARCHAR, contains_nan BOOLEAN, extra_stats VARCHAR);
CREATE TABLE ducklake_delete_file (delete_file_id BIGINT PRIMARY KEY, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, data_file_id BIGINT, path VARCHAR, path_is_relative BOOLEAN, format VARCHAR, delete_count BIGINT, file_size_bytes BIGINT, footer_size BIGINT, encryption_key VARCHAR);
CREATE TABLE ducklake_column (column_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, table_id BIGINT, column_order BIGINT, column_name VARCHAR, column_type VARCHAR, initial_default VARCHAR, default_value VARCHAR, nulls_allowed BOOLEAN, parent_column BIGINT);
CREATE TABLE ducklake_table_stats (table_id BIGINT, record_count BIGINT, next_row_id BIGINT, file_size_bytes BIGINT);
CREATE TABLE ducklake_table_column_stats (table_id BIGINT, column_id BIGINT, contains_null BOOLEAN, contains_nan BOOLEAN, min_value VARCHAR, max_value VARCHAR, extra_stats VARCHAR);
CREATE TABLE ducklake_partition_info (partition_id BIGINT, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT);
CREATE TABLE ducklake_partition_column (partition_id BIGINT, table_id BIGINT, partition_key_index BIGINT, column_id BIGINT, transform VARCHAR);
CREATE TABLE ducklake_file_partition_value (data_file_id BIGINT, table_id BIGINT, partition_key_index BIGINT, partition_value VARCHAR);
CREATE TABLE ducklake_files_scheduled_for_deletion (data_file_id BIGINT, path VARCHAR, path_is_relative BOOLEAN, schedule_start TIMESTAMPTZ);
CREATE TABLE ducklake_inlined_data_tables (table_id BIGINT, table_name VARCHAR, schema_version BIGINT);
CREATE TABLE ducklake_column_mapping (mapping_id BIGINT, table_id BIGINT, type VARCHAR);
CREATE TABLE ducklake_name_mapping (mapping_id BIGINT, column_id BIGINT, source_name VARCHAR, target_field_id BIGINT, parent_column BIGINT, is_partition BOOLEAN);
CREATE TABLE ducklake_schema_versions (begin_snapshot BIGINT, schema_version BIGINT);
CREATE TABLE ducklake_macro (schema_id BIGINT, macro_id BIGINT, macro_name VARCHAR, begin_snapshot BIGINT, end_snapshot BIGINT);
CREATE TABLE ducklake_macro_impl (macro_id BIGINT, impl_id BIGINT, dialect VARCHAR, sql VARCHAR, type VARCHAR);
CREATE TABLE ducklake_macro_parameters (macro_id BIGINT, impl_id BIGINT, column_id BIGINT, parameter_name VARCHAR, parameter_type VARCHAR, default_value VARCHAR, default_value_type VARCHAR);
CREATE TABLE ducklake_sort_info (sort_id BIGINT, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT);
CREATE TABLE ducklake_sort_expression (sort_id BIGINT, table_id BIGINT, sort_key_index BIGINT, expression VARCHAR, dialect VARCHAR, sort_direction VARCHAR, null_order VARCHAR);
```
