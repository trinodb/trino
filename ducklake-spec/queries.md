---
layout: docu
title: Queries
---

This page explains the queries issued to the DuckLake catalog database for reading and writing data.

## Reading Data

DuckLake specifies _tables_ and _update transactions_ to modify them. DuckLake is not a black box: all metadata is stored as SQL tables under the user's control. Of course, they can be queried in whichever way is best for a client. Below we describe a small working example to retrieve table data.

> The information below is to provide transparency to users and to aid developers making their own implementation of DuckLake.
> If you are using the `ducklake` DuckDB extension, you do not need to worry about these: the extension is running these operations in the background for you.

### Get Current Snapshot

Before anything else we need to find a snapshot ID to be queried. There can be many snapshots in the [`ducklake_snapshot` table]({% link docs/preview/specification/tables/ducklake_snapshot.md %}). A snapshot ID is a continuously increasing number that identifies a snapshot. In most cases, you would query the most recent one like so:

```sql
SELECT snapshot_id
FROM ducklake_snapshot
WHERE snapshot_id =
    (SELECT max(snapshot_id) FROM ducklake_snapshot);
```

### List Schemas

A DuckLake catalog can contain many SQL-style schemas, which each can contain many tables.
These are listed in the [`ducklake_schema` table]({% link docs/preview/specification/tables/ducklake_schema.md %}).
Here's how we get the list of valid schemas for a given snapshot:

```sql
SELECT schema_id, schema_name
FROM ducklake_schema
WHERE
    ⟨snapshot_id⟩ >= begin_snapshot AND
    (⟨snapshot_id⟩ < end_snapshot OR end_snapshot IS NULL);
```

where

- `⟨snapshot_id⟩`{:.language-sql .highlight} is a `BIGINT` referring to the `snapshot_id` column in the [`ducklake_snapshot` table]({% link docs/preview/specification/tables/ducklake_snapshot.md %}).

### List Tables

We can list the tables available in a schema for a specific snapshot using the [`ducklake_table` table]({% link docs/preview/specification/tables/ducklake_table.md %}):

```sql
SELECT table_id, table_name
FROM ducklake_table
WHERE
    schema_id = ⟨schema_id⟩ AND
    ⟨snapshot_id⟩ >= begin_snapshot AND
    (⟨snapshot_id⟩ < end_snapshot OR end_snapshot IS NULL);
```

where

- `⟨schema_id⟩`{:.language-sql .highlight} is a `BIGINT` referring to the `schema_id` column in the [`ducklake_schema` table]({% link docs/preview/specification/tables/ducklake_schema.md %}).
- `⟨snapshot_id⟩`{:.language-sql .highlight} is a `BIGINT` referring to the `snapshot_id` column in the [`ducklake_snapshot` table]({% link docs/preview/specification/tables/ducklake_snapshot.md %}).

### Show the Structure of a Table

For each given table, we can list the available top-level columns using the [`ducklake_column` table]({% link docs/preview/specification/tables/ducklake_column.md %}):

```sql
SELECT column_id, column_name, column_type
FROM ducklake_column
WHERE
    table_id = ⟨table_id⟩ AND
    parent_column IS NULL AND
    ⟨snapshot_id⟩ >= begin_snapshot AND
    (⟨snapshot_id⟩ < end_snapshot OR end_snapshot IS NULL)
ORDER BY column_order;
```

where

- `⟨table_id⟩`{:.language-sql .highlight} is a `BIGINT` referring to the `table_id` column in the [`ducklake_table` table]({% link docs/preview/specification/tables/ducklake_table.md %}).
- `⟨snapshot_id⟩`{:.language-sql .highlight} is a `BIGINT` referring to the `snapshot_id` column in the [`ducklake_snapshot` table]({% link docs/preview/specification/tables/ducklake_snapshot.md %}).

> DuckLake supports nested columns – the filter for `parent_column IS NULL` only shows the top-level columns.

For the list of supported data types, please refer to the [“Data Types” page]({% link docs/preview/specification/data_types.md %}).

### `SELECT`

Now that we know the table structure we can query actual data from the Parquet files that store table data. We need to join the list of data files with the list of delete files (if any). There can be at most one delete file per file in a single snapshot.

```sql
SELECT data.path AS data_file_path, del.path AS delete_file_path
FROM ducklake_data_file AS data
LEFT JOIN (
    SELECT *
    FROM ducklake_delete_file
    WHERE
        ⟨snapshot_id⟩ >= begin_snapshot AND
        (⟨snapshot_id⟩ < end_snapshot OR end_snapshot IS NULL)
    ) AS del
USING (data_file_id)
WHERE
    data.table_id = ⟨table_id⟩ AND
    ⟨snapshot_id⟩ >= data.begin_snapshot AND
    (⟨snapshot_id⟩ < data.end_snapshot OR data.end_snapshot IS NULL)
ORDER BY file_order;
```

where

- `⟨table_id⟩`{:.language-sql .highlight} is a `BIGINT` referring to the `table_id` column in the [`ducklake_table` table]({% link docs/preview/specification/tables/ducklake_table.md %}).
- `⟨snapshot_id⟩`{:.language-sql .highlight} is a `BIGINT` referring to the `snapshot_id` column in the [`ducklake_snapshot` table]({% link docs/preview/specification/tables/ducklake_snapshot.md %}).

Now we have a list of files. In order to reconstruct actual table rows, we need to read all rows from the `data_file_path` files and remove the rows labeled as deleted in the `delete_file_path`.

Not all files have to contain all the columns currently defined in the table, some files may also have columns that existed previously but have been removed.

> DuckLake also supports changing the schema, see [schema evolution]({% link docs/preview/duckdb/usage/schema_evolution.md %}).

#### Note on Paths

In DuckLake, paths can be relative to the initially specified data path. Whether a path is relative or not to the `data_path` prefix from [`ducklake_metadata`]({% link docs/preview/specification/tables/ducklake_metadata.md %}), is stored in the [`ducklake_data_file`]({% link docs/preview/specification/tables/ducklake_data_file.md %}) and [`ducklake_delete_file`]({% link docs/preview/specification/tables/ducklake_delete_file.md %}) entries (`path_is_relative`).

### `SELECT` with File Pruning

One of the main strengths of lakehouse formats is the ability to *prune* files that cannot contain data relevant to the query.
The [`ducklake_file_column_stats` table]({% link docs/preview/specification/tables/ducklake_file_column_stats.md %}) contains the file-level statistics.
We can use the information there to prune the list of files to be read if a filter predicate is given.

We can get a list of all files that are part of a given table like described above. We can then reduce that list to only relevant files by querying the per-file column statistics. For example, for scalar equality we can find the relevant files using the query below:

```sql
SELECT data_file_id
FROM ducklake_file_column_stats
WHERE
    table_id = ⟨table_id⟩ AND
    column_id = ⟨column_id⟩ AND
    (⟨scalar⟩ >= min_value OR min_value IS NULL) AND
    (⟨scalar⟩ <= max_value OR max_value IS NULL);
```

where

- `⟨table_id⟩`{:.language-sql .highlight} is a `BIGINT` referring to the `table_id` column in the [`ducklake_table` table]({% link docs/preview/specification/tables/ducklake_table.md %}).
- `⟨column_id⟩`{:.language-sql .highlight} is a `BIGINT` referring to the `column_id` column in the [`ducklake_column` table]({% link docs/preview/specification/tables/ducklake_column.md %}).
- `⟨scalar⟩`{:.language-sql .highlight} is the scalar comparison value for the pruning.

Of course, other filter predicates like “greater than” will require slightly different filtering here.

> The minimum and maximum values for each column are stored as strings and need to be cast for correct range filters on numeric columns.

## Writing Data

### Snapshot Creation

Any changes to data stored in DuckLake require the creation of a new snapshot. We need to:

- create a new snapshot in [`ducklake_snapshot`]({% link docs/preview/specification/tables/ducklake_snapshot.md %}) and
- log the changes a snapshot made in [`ducklake_snapshot_changes`]({% link docs/preview/specification/tables/ducklake_snapshot_changes.md %})

```sql
INSERT INTO ducklake_snapshot (
    snapshot_id,
    snapshot_timestamp,
    schema_version,
    next_catalog_id,
    next_file_id
)
VALUES (
    ⟨snapshot_id⟩,
    now(),
    ⟨schema_version⟩,
    ⟨next_catalog_id⟩,
    ⟨next_file_id⟩
);

INSERT INTO ducklake_snapshot_changes (
    snapshot_id,
    snapshot_changes,
    author,
    commit_message,
    commit_extra_info
)
VALUES (
    ⟨snapshot_id⟩,
    ⟨changes⟩,
    ⟨author⟩,
    ⟨commit_message⟩,
    ⟨commit_extra_info⟩
);
```

where

- `⟨snapshot_id⟩`{:.language-sql .highlight} is the new snapshot identifier. This should be `max(snapshot_id) + 1`.
- `⟨schema_version⟩`{:.language-sql .highlight} is the schema version for the new snapshot. If any schema changes are made, this needs to be incremented. Otherwise the previous snapshot's `schema_version` can be re-used.
- `⟨next_catalog_id⟩`{:.language-sql .highlight} gives the next unused identifier for tables, schemas, or views. This only has to be incremented if new catalog entries are created.
- `⟨next_file_id⟩`{:.language-sql .highlight} is the same but for data or delete files.
- `⟨changes⟩`{:.language-sql .highlight} contains a list of changes performed by the snapshot. See the list of possible values in the [`ducklake_snapshot_changes` table's documentation]({% link docs/preview/specification/tables/ducklake_snapshot_changes.md %}).
- `⟨author⟩`{:.language-sql .highlight} contains information about the author of the commit (optional).
- `⟨commit_message⟩`{:.language-sql .highlight} attaches a commit message to the transaction (optional).
- `⟨commit_extra_info⟩`{:.language-sql .highlight} attaches extra information to the transaction (optional).

### `CREATE SCHEMA`

A schema is a collection of tables. In order to create a new schema, we can just insert into the [`ducklake_schema` table]({% link docs/preview/specification/tables/ducklake_schema.md %}):

```sql
INSERT INTO ducklake_schema (
    schema_id,
    schema_uuid,
    begin_snapshot,
    end_snapshot,
    schema_name
)
VALUES (
    ⟨schema_id⟩,
    uuid(),
    ⟨snapshot_id⟩,
    NULL,
    ⟨schema_name⟩
);
```

where

- `⟨schema_id⟩`{:.language-sql .highlight} is the new schema identifier. This should be created by incrementing `next_catalog_id` from the previous snapshot.
- `⟨snapshot_id⟩`{:.language-sql .highlight} is the snapshot identifier of the new snapshot as described above.
- `⟨schema_name⟩`{:.language-sql .highlight} is the name of the new schema.

### `CREATE TABLE`

Creating a table in a schema is very similar to creating a schema. We insert into the [`ducklake_table` table]({% link docs/preview/specification/tables/ducklake_table.md %}):

```sql
INSERT INTO ducklake_table (
    table_id,
    table_uuid,
    begin_snapshot,
    end_snapshot,
    schema_id,
    table_name
)
VALUES (
    ⟨table_id⟩,
    uuid(),
    ⟨snapshot_id⟩,
    NULL,
    ⟨schema_id⟩,
    ⟨table_name⟩
);
```

where

- `⟨table_id⟩`{:.language-sql .highlight} is the new table identifier. This should be created by further incrementing `next_catalog_id` from the previous snapshot.
- `⟨snapshot_id⟩`{:.language-sql .highlight} is the snapshot identifier of the new snapshot as described above.
- `⟨schema_id⟩`{:.language-sql .highlight} is a `BIGINT` referring to the `schema_id` column in the [`ducklake_schema` table]({% link docs/preview/specification/tables/ducklake_schema.md %}) table.
- `⟨table_name⟩`{:.language-sql .highlight} is the name of the new table.

A table needs some columns, we can add columns to the new table by inserting into the [`ducklake_column` table]({% link docs/preview/specification/tables/ducklake_column.md %}) table.
For each column to be added, we run the following query:

```sql
INSERT INTO ducklake_column (
    column_id,
    begin_snapshot,
    end_snapshot,
    table_id,
    column_order,
    column_name,
    column_type,
    nulls_allowed
)
VALUES (
    ⟨column_id⟩,
    ⟨snapshot_id⟩,
    NULL,
    ⟨table_id⟩,
    ⟨column_order⟩,
    ⟨column_name⟩,
    ⟨column_type⟩,
    ⟨nulls_allowed⟩
);
```

where

- `⟨column_id⟩`{:.language-sql .highlight} is the new column identifier. This ID must be unique *within the table* over its entire life time.
- `⟨snapshot_id⟩`{:.language-sql .highlight} is the snapshot identifier of the new snapshot as described above.
- `⟨table_id⟩`{:.language-sql .highlight} is a `BIGINT` referring to the `table_id` column in the [`ducklake_table` table]({% link docs/preview/specification/tables/ducklake_table.md %}).
- `⟨column_order⟩`{:.language-sql .highlight} is a number that defines where the column is placed in an ordered list of columns.
- `⟨column_name⟩`{:.language-sql .highlight} is the name of the column.
- `⟨column_type⟩`{:.language-sql .highlight} is the data type of the column. See the [“Data Types” page]({% link docs/preview/specification/data_types.md %}) for details.
- `⟨nulls_allowed⟩`{:.language-sql .highlight} is a boolean that defines if `NULL` values can be stored in the column. Typically set to `true`.

> We skipped some complexity in this example around default values and nested types and just left those fields as `NULL`.
> See the table schema definition for additional details.

### `DROP TABLE`

Dropping a table in DuckLake requires an update to the `end_snapshot` field in all metadata entries corresponding to the dropped table ID.

```sql
UPDATE ducklake_table
SET end_snapshot = ⟨snapshot_id⟩ WHERE table_id = ⟨table_id⟩ AND end_snapshot IS NULL;

UPDATE ducklake_partition_info
SET end_snapshot = ⟨snapshot_id⟩ WHERE table_id = ⟨table_id⟩ AND end_snapshot IS NULL;

UPDATE ducklake_column
SET end_snapshot = ⟨snapshot_id⟩ WHERE table_id = ⟨table_id⟩ AND end_snapshot IS NULL;

UPDATE ducklake_column_tag
SET end_snapshot = ⟨snapshot_id⟩ WHERE table_id = ⟨table_id⟩ AND end_snapshot IS NULL;

UPDATE ducklake_data_file
SET end_snapshot = ⟨snapshot_id⟩ WHERE table_id = ⟨table_id⟩ AND end_snapshot IS NULL;

UPDATE ducklake_delete_file
SET end_snapshot = ⟨snapshot_id⟩ WHERE table_id = ⟨table_id⟩ AND end_snapshot IS NULL;

UPDATE ducklake_tag
SET end_snapshot = ⟨snapshot_id⟩ WHERE object_id  = ⟨table_id⟩ AND end_snapshot IS NULL;
```

where

- `⟨snapshot_id⟩`{:.language-sql .highlight} is the snapshot identifier of the new snapshot as described above.
- `⟨table_id⟩`{:.language-sql .highlight} is the identifier of the table that will be dropped.

### `DROP SCHEMA`

Dropping a schema in DuckLake requires updating the `end_snapshot` in the `ducklake_schema` table.

```sql
UPDATE ducklake_schema
SET
    end_snapshot = ⟨snapshot_id⟩
WHERE
    schema_id = ⟨schema_id⟩ AND
    end_snapshot IS NULL;
```

where

- `⟨snapshot_id⟩`{:.language-sql .highlight} is the snapshot identifier of the new snapshot as described above.
- `⟨schema_id⟩`{:.language-sql .highlight} is the identifier of the schema that will be dropped.

> `DROP SCHEMA` is only allowed on empty schemas. Ensure that all tables within the schema are dropped beforehand.

### `INSERT`

Inserting data into a DuckLake table consists of two main steps:
first, we need to write a Parquet file containing the actual row data to storage, and
second, we need to register that file in the metadata tables and update global statistics.
Let's assume the file has already been written.

```sql
INSERT INTO ducklake_data_file (
    data_file_id,
    table_id,
    begin_snapshot,
    end_snapshot,
    path,
    path_is_relative,
    file_format,
    record_count,
    file_size_bytes,
    footer_size,
    row_id_start
)
VALUES (
    ⟨data_file_id⟩,
    ⟨table_id⟩,
    ⟨snapshot_id⟩,
    NULL,
    ⟨path⟩,
    true,
    'parquet',
    ⟨record_count⟩,
    ⟨file_size_bytes⟩,
    ⟨footer_size⟩,
    ⟨row_id_start⟩
);
```

where

- `⟨data_file_id⟩`{:.language-sql .highlight} is the new data file identifier. This ID must be unique *within the table* over its entire life time.
- `⟨table_id⟩`{:.language-sql .highlight} is a `BIGINT` referring to the `table_id` column in the [`ducklake_table` table]({% link docs/preview/specification/tables/ducklake_table.md %}).
- `⟨snapshot_id⟩`{:.language-sql .highlight} is the snapshot identifier of the new snapshot as described above.
- `⟨path⟩`{:.language-sql .highlight} is the file name relative to the DuckLake data path from the top-level metadata.
- `⟨record_count⟩`{:.language-sql .highlight} is the number of rows in the file.
- `⟨file_size_bytes⟩`{:.language-sql .highlight} is the file size.
- `⟨footer_size⟩`{:.language-sql .highlight} is the position of the Parquet footer. This helps with efficiently reading the file.
- `⟨row_id_start⟩`{:.language-sql .highlight} is the first logical row ID from the file. This number can be read from the [`ducklake_table_stats` table]({% link docs/preview/specification/tables/ducklake_table_stats.md %}) via column `next_row_id`.

We have omitted some complexity around relative paths, encrypted files, partitioning and partial files in this example.
Refer to the [`ducklake_data_file` table]({% link docs/preview/specification/tables/ducklake_data_file.md %}) documentation for details.

> DuckLake also supports changing the schema, see [schema evolution]({% link docs/preview/duckdb/usage/schema_evolution.md %}).

We also have to update some statistics in the [`ducklake_table_stats` table]({% link docs/preview/specification/tables/ducklake_table_stats.md %}) and [`ducklake_table_column_stats` table]({% link docs/preview/specification/tables/ducklake_table_column_stats.md %}) tables.

```sql
UPDATE ducklake_table_stats
SET
    record_count = record_count + ⟨record_count⟩,
    next_row_id = next_row_id + ⟨record_count⟩,
    file_size_bytes = file_size_bytes + ⟨file_size_bytes⟩
WHERE table_id = ⟨table_id⟩;

UPDATE ducklake_table_column_stats
SET
    contains_null = contains_null OR ⟨null_count⟩ > 0,
    contains_nan = contains_nan,
    min_value = min(min_value, ⟨min_value⟩),
    max_value = max(max_value, ⟨max_value⟩)
WHERE
    table_id = ⟨table_id⟩ AND
    column_id = ⟨column_id⟩;

INSERT INTO ducklake_file_column_stats (
    data_file_id,
    table_id,
    column_id,
    value_count,
    null_count,
    min_value,
    max_value,
    contains_nan
)
VALUES (
    ⟨data_file_id⟩,
    ⟨table_id⟩,
    ⟨column_id⟩,
    ⟨record_count⟩,
    ⟨null_count⟩,
    ⟨min_value⟩,
    ⟨max_value⟩,
    ⟨contains_nan⟩;
);
```

where

- `⟨table_id⟩`{:.language-sql .highlight} is a `BIGINT` referring to the `table_id` column in the [`ducklake_table` table]({% link docs/preview/specification/tables/ducklake_table.md %}).
- `⟨column_id⟩`{:.language-sql .highlight} is a `BIGINT` referring to the `column_id` column in the [`ducklake_column` table]({% link docs/preview/specification/tables/ducklake_column.md %}).
- `⟨data_file_id⟩`{:.language-sql .highlight} is a `BIGINT` referring to the `data_file_id` column in the [`ducklake_data_file` table]({% link docs/preview/specification/tables/ducklake_data_file.md %}).
- `⟨record_count⟩`{:.language-sql .highlight} is the number of values (including `NULL` and `NaN` values) in the file column.
- `⟨null_count⟩`{:.language-sql .highlight} is the number of `NULL` values in the file column.
- `⟨min_value⟩`{:.language-sql .highlight} is the *minimum* value in the file column as a string.
- `⟨max_value⟩`{:.language-sql .highlight} is the *maximum* value in the file column as a string.
- `⟨file_size_bytes⟩`{:.language-sql .highlight} is the size of the new Parquet file.
- `⟨contains_nan⟩`{:.language-sql .highlight} is a flag whether the column contains any `NaN` values. This is only relevant for floating-point types.

> This example assumes there are already rows in the table. If there are none, we need to use `INSERT` instead of `UPDATE` here.
> We also skipped the `column_size_bytes` column here, it can safely be set to `NULL`.

### `DELETE`

Deleting data from a DuckLake table consists of two main steps:
first, we need to write a _Parquet delete file_ containing the row index to be deleted to storage, and
second, we need to register that delete file in the metadata tables.
Let's assume the file has already been written.

```sql
INSERT INTO ducklake_delete_file (
    delete_file_id,
    table_id,
    begin_snapshot,
    end_snapshot,
    data_file_id,
    path,
    path_is_relative,
    format,
    delete_count,
    file_size_bytes,
    footer_size
)
VALUES (
    ⟨delete_file_id⟩,
    ⟨table_id⟩,
    ⟨snapshot_id⟩,
    NULL,
    ⟨data_file_id⟩,
    ⟨path⟩,
    true,
    'parquet',
    ⟨delete_count⟩,
    ⟨file_size_bytes⟩,
    ⟨footer_size⟩
);
```

where

- `⟨delete_file_id⟩`{:.language-sql .highlight} is the identifier for the new delete file.
- `⟨table_id⟩`{:.language-sql .highlight} is a `BIGINT` referring to the `table_id` column in the [`ducklake_table` table]({% link docs/preview/specification/tables/ducklake_table.md %}).
- `⟨snapshot_id⟩`{:.language-sql .highlight} is the snapshot identifier of the new snapshot as described above.
- `⟨data_file_id⟩`{:.language-sql .highlight} is the identifier of the data file from which the rows are to be deleted.
- `⟨path⟩`{:.language-sql .highlight} is the file name relative to the DuckLake data path from the top-level metadata.
- `⟨delete_count⟩`{:.language-sql .highlight} is the number of deletion records in the file.
- `⟨file_size_bytes⟩`{:.language-sql .highlight} is the file size.
- `⟨footer_size⟩`{:.language-sql .highlight} is the position of the Parquet footer. This helps with efficiently reading the file.

Notes:

- We have omitted some complexity around relative paths and encrypted files in this example. Refer to the [`ducklake_delete_file` table]({% link docs/preview/specification/tables/ducklake_delete_file.md %}) documentation for details.

- In DuckLake, the strategy used for `DELETE` operations is **merge-on-read**. Delete files are referenced in the [`ducklake_delete_file` table]({% link docs/preview/specification/tables/ducklake_delete_file.md %}).

- Please note that `DELETE` operations also do not require updates to table statistics, as the statistics are maintained as upper bounds, and deletions do not violate these bounds.

### `UPDATE`

In DuckLake, `UPDATE` operations are expressed as a combination of a `DELETE` followed by an `INSERT`. Specifically, the outdated row is marked for deletion, and the updated version of that row is inserted. As a result, the changes to the metadata tables are equivalent to performing a `DELETE` and an `INSERT` operation sequentially within the same transaction.
