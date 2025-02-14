# Delta Lake connector

```{raw} html
<img src="../_static/img/delta-lake.png" class="connector-logo">
```

The Delta Lake connector allows querying data stored in the [Delta Lake](https://delta.io) format, including [Databricks Delta Lake](https://docs.databricks.com/delta/index.html). The connector can natively
read the Delta Lake transaction log and thus detect when external systems change
data.

## Requirements

To connect to Databricks Delta Lake, you need:

- Tables written by Databricks Runtime 7.3 LTS, 9.1 LTS, 10.4 LTS, 11.3 LTS,
  12.2 LTS, 13.3 LTS, 14.3 LTS and 15.4 LTS are supported.
- Deployments using AWS, HDFS, Azure Storage, and Google Cloud Storage (GCS) are
  fully supported.
- Network access from the coordinator and workers to the Delta Lake storage.
- Access to the Hive metastore service (HMS) of Delta Lake or a separate HMS,
  or a Glue metastore.
- Network access to the HMS from the coordinator and workers. Port 9083 is the
  default port for the Thrift protocol used by the HMS.
- Data files stored in the [Parquet file format](parquet-format-configuration)
  on a [supported file system](delta-lake-file-system-configuration).

## General configuration

To configure the Delta Lake connector, create a catalog properties file
`etc/catalog/example.properties` that references the `delta_lake` connector.

You must configure a [metastore for metadata](/object-storage/metastores).

You must select and configure one of the [supported file
systems](delta-lake-file-system-configuration).

```properties
connector.name=delta_lake
hive.metastore.uri=thrift://example.net:9083
fs.x.enabled=true
```

Replace the `fs.x.enabled` configuration property with the desired file system.

If you are using {ref}`AWS Glue <hive-glue-metastore>` as your metastore, you
must instead set `hive.metastore` to `glue`:

```properties
connector.name=delta_lake
hive.metastore=glue
```

Each metastore type has specific configuration properties along with
{ref}`general metastore configuration properties <general-metastore-properties>`.

The connector recognizes Delta Lake tables created in the metastore by the Databricks
runtime. If non-Delta Lake tables are present in the metastore as well, they are not
visible to the connector.

(delta-lake-file-system-configuration)=
## File system access configuration

The connector supports accessing the following file systems:

* [](/object-storage/file-system-azure)
* [](/object-storage/file-system-gcs)
* [](/object-storage/file-system-s3)
* [](/object-storage/file-system-hdfs)

You must enable and configure the specific file system access. [Legacy
support](file-system-legacy) is not recommended and will be removed.

### Delta Lake general configuration properties

The following configuration properties are all using reasonable, tested default
values. Typical usage does not require you to configure them.

:::{list-table} Delta Lake configuration properties
:widths: 30, 55, 15
:header-rows: 1

* - Property name
  - Description
  - Default
* - `delta.metadata.cache-ttl`
  - Caching duration for Delta Lake tables metadata.
  - `30m`
* - `delta.metadata.cache-max-retained-size`
  - Maximum retained size of Delta table metadata stored in cache. Must be
    specified in [](prop-type-data-size) values such as `64MB`. Default is
    calculated to 5% of the maximum memory allocated to the JVM.
  - 
* - `delta.metadata.live-files.cache-size`
  - Amount of memory allocated for caching information about files. Must be
    specified in [](prop-type-data-size) values such as `64MB`. Default is
    calculated to 10% of the maximum memory allocated to the JVM.
  -
* - `delta.metadata.live-files.cache-ttl`
  - Caching duration for active files that correspond to the Delta Lake tables.
  - `30m`
* - `delta.compression-codec`
  - The compression codec to be used when writing new data files. Possible
    values are:

    * `NONE`
    * `SNAPPY`
    * `ZSTD`
    * `GZIP`

    The equivalent catalog session property is `compression_codec`.
  - `ZSTD`
* - `delta.max-partitions-per-writer`
  - Maximum number of partitions per writer.
  - `100`
* - `delta.hide-non-delta-lake-tables`
  - Hide information about tables that are not managed by Delta Lake. Hiding
    only applies to tables with the metadata managed in a Glue catalog, and does
    not apply to usage with a Hive metastore service.
  - `false`
* - `delta.enable-non-concurrent-writes`
  - Enable [write support](delta-lake-data-management) for all supported file
    systems. Specifically, take note of the warning about concurrency and
    checkpoints.
  - `false`
* - `delta.default-checkpoint-writing-interval`
  - Default integer count to write transaction log checkpoint entries. If the
    value is set to N, then checkpoints are written after every Nth statement
    performing table writes. The value can be overridden for a specific table
    with the `checkpoint_interval` table property.
  - `10`
* - `delta.hive-catalog-name`
  - Name of the catalog to which `SELECT` queries are redirected when a
    Hive table is detected.
  -
* - `delta.checkpoint-row-statistics-writing.enabled`
  - Enable writing row statistics to checkpoint files.
  - `true`
* - `delta.checkpoint-filtering.enabled`
  - Enable pruning of data file entries as well as data file statistics columns
    which are irrelevant for the query when reading Delta Lake checkpoint files.
    Reading only the relevant active file data from the checkpoint, directly
    from the storage, instead of relying on the active files caching, likely
    results in decreased memory pressure on the coordinator. The equivalent
    catalog session property is `checkpoint_filtering_enabled`.
  - `true`
* - `delta.dynamic-filtering.wait-timeout`
  - Duration to wait for completion of [dynamic
    filtering](/admin/dynamic-filtering) during split generation. The equivalent
    catalog session property is `dynamic_filtering_wait_timeout`.
  -
* - `delta.table-statistics-enabled`
  - Enables [Table statistics](delta-lake-table-statistics) for performance
    improvements. The equivalent catalog session property is
    `statistics_enabled`.
  - `true`
* - `delta.extended-statistics.enabled`
  - Enable statistics collection with [](/sql/analyze) and use of extended
    statistics. The equivalent catalog session property is
    `extended_statistics_enabled`.
  - `true`
* - `delta.extended-statistics.collect-on-write`
  - Enable collection of extended statistics for write operations. The
    equivalent catalog session property is
    `extended_statistics_collect_on_write`.
  - `true`
* - `delta.per-transaction-metastore-cache-maximum-size`
  - Maximum number of metastore data objects per transaction in the Hive
    metastore cache.
  - `1000`
* - `delta.metastore.store-table-metadata`
  - Store table comments and colum definitions in the metastore. The write
    permission is required to update the metastore.
  - `false`
* - `delta.metastore.store-table-metadata-threads`
  - Number of threads used for storing table metadata in metastore.
  - `5`
* - `delta.delete-schema-locations-fallback`
  - Whether schema locations are deleted when Trino can't determine whether they
    contain external files.
  - `false`
* - `delta.parquet.time-zone`
  - Time zone for Parquet read and write.
  - JVM default
* - `delta.target-max-file-size`
  - Target maximum size of written files; the actual size could be larger. The
    equivalent catalog session property is `target_max_file_size`.
  - `1GB`
* - `delta.unique-table-location`
  - Use randomized, unique table locations.
  - `true`
* - `delta.register-table-procedure.enabled`
  - Enable to allow users to call the [`register_table` procedure](delta-lake-register-table).
  - `false`
* - `delta.vacuum.min-retention`
  - Minimum retention threshold for the files taken into account for removal by
    the [VACUUM](delta-lake-vacuum) procedure. The equivalent catalog session
    property is `vacuum_min_retention`.
  - `7 DAYS`
* - `delta.deletion-vectors-enabled`
  - Set to `true` for enabling deletion vectors by default when creating new tables.
  - `false`
* - `delta.metadata.parallelism`
  - Number of threads used for retrieving metadata. Currently, only table loading 
    is parallelized.
  - `8`
:::

### Catalog session properties

The following table describes {ref}`catalog session properties
<session-properties-definition>` supported by the Delta Lake connector:

:::{list-table} Catalog session properties
:widths: 40, 60, 20
:header-rows: 1

* - Property name
  - Description
  - Default
* - `parquet_max_read_block_size`
  - The maximum block size used when reading Parquet files.
  - `16MB`
* - `parquet_writer_block_size`
  - The maximum block size created by the Parquet writer.
  - `128MB`
* - `parquet_writer_page_size`
  - The maximum page size created by the Parquet writer.
  - `1MB`
* - `parquet_writer_page_value_count`
  - The maximum value count of pages created by the Parquet writer.
  - `60000`
* - `parquet_writer_batch_size`
  - Maximum number of rows processed by the Parquet writer in a batch.
  - `10000`
* - `projection_pushdown_enabled`
  - Read only projected fields from row columns while performing `SELECT`
    queries.
  - `true`
:::

(delta-lake-fte-support)=
### Fault-tolerant execution support

The connector supports {doc}`/admin/fault-tolerant-execution` of query
processing. Read and write operations are both supported with any retry policy.


(delta-lake-type-mapping)=
## Type mapping

Because Trino and Delta Lake each support types that the other does not, this
connector {ref}`modifies some types <type-mapping-overview>` when reading or
writing data. Data types might not map the same way in both directions between
Trino and the data source. Refer to the following sections for type mapping in
each direction.

See the [Delta Transaction Log specification](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#primitive-types)
for more information about supported data types in the Delta Lake table format
specification.

### Delta Lake to Trino type mapping

The connector maps Delta Lake types to the corresponding Trino types following
this table:

:::{list-table} Delta Lake to Trino type mapping
:widths: 40, 60
:header-rows: 1

* - Delta Lake type
  - Trino type
* - `BOOLEAN`
  - `BOOLEAN`
* - `INTEGER`
  - `INTEGER`
* - `BYTE`
  - `TINYINT`
* - `SHORT`
  - `SMALLINT`
* - `LONG`
  - `BIGINT`
* - `FLOAT`
  - `REAL`
* - `DOUBLE`
  - `DOUBLE`
* - `DECIMAL(p,s)`
  - `DECIMAL(p,s)`
* - `STRING`
  - `VARCHAR`
* - `BINARY`
  - `VARBINARY`
* - `DATE`
  - `DATE`
* - `TIMESTAMPNTZ` (`TIMESTAMP_NTZ`)
  - `TIMESTAMP(6)`
* - `TIMESTAMP`
  - `TIMESTAMP(3) WITH TIME ZONE`
* - `ARRAY`
  - `ARRAY`
* - `MAP`
  - `MAP`
* - `STRUCT(...)`
  - `ROW(...)`
:::

No other types are supported.

### Trino to Delta Lake type mapping

The connector maps Trino types to the corresponding Delta Lake types following
this table:

:::{list-table} Trino to Delta Lake type mapping
:widths: 60, 40
:header-rows: 1

* - Trino type
  - Delta Lake type
* - `BOOLEAN`
  - `BOOLEAN`
* - `INTEGER`
  - `INTEGER`
* - `TINYINT`
  - `BYTE`
* - `SMALLINT`
  - `SHORT`
* - `BIGINT`
  - `LONG`
* - `REAL`
  - `FLOAT`
* - `DOUBLE`
  - `DOUBLE`
* - `DECIMAL(p,s)`
  - `DECIMAL(p,s)`
* - `VARCHAR`
  - `STRING`
* - `VARBINARY`
  - `BINARY`
* - `DATE`
  - `DATE`
* - `TIMESTAMP`
  - `TIMESTAMPNTZ` (`TIMESTAMP_NTZ`)
* - `TIMESTAMP(3) WITH TIME ZONE`
  - `TIMESTAMP`
* - `ARRAY`
  - `ARRAY`
* - `MAP`
  - `MAP`
* - `ROW(...)`
  - `STRUCT(...)`
:::

No other types are supported.

## Delta Lake table features

The connector supports the following [Delta Lake table
features](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#table-features):

:::{list-table} Table features
:widths: 70, 30
:header-rows: 1

* - Feature
  - Description
* - Append-only tables
  - Writers only
* - Column invariants
  - Writers only
* - CHECK constraints
  - Writers only
* - Change data feed
  - Writers only
* - Column mapping
  - Readers and writers
* - Deletion vectors
  - Readers and writers
* - Iceberg compatibility V1 & V2
  - Readers only
* - Invariants
  - Writers only
* - Timestamp without time zone
  - Readers and writers
* - Type widening
  - Readers only
* - Vacuum protocol check
  - Readers and writers
* - V2 checkpoint
  - Readers only
:::

No other features are supported.

## Security

The Delta Lake connector allows you to choose one of several means of providing
authorization at the catalog level. You can select a different type of
authorization check in different Delta Lake catalog files.

(delta-lake-authorization)=
### Authorization checks

Enable authorization checks for the connector by setting the `delta.security`
property in the catalog properties file. This property must be one of the
security values in the following table:

:::{list-table} Delta Lake security values
:widths: 30, 60
:header-rows: 1

* - Property value
  - Description
* - `ALLOW_ALL` (default value)
  - No authorization checks are enforced.
* - `SYSTEM`
  - The connector relies on system-level access control.
* - `READ_ONLY`
  - Operations that read data or metadata, such as [](/sql/select) are
    permitted. No operations that write data or metadata, such as
    [](/sql/create-table), [](/sql/insert), or [](/sql/delete) are allowed.
* - `FILE`
  - Authorization checks are enforced using a catalog-level access control
    configuration file whose path is specified in the `security.config-file`
    catalog configuration property. See [](catalog-file-based-access-control)
    for information on the authorization configuration file.
:::

(delta-lake-sql-support)=
## SQL support

The connector provides read and write access to data and metadata in
Delta Lake. In addition to the {ref}`globally available
<sql-globally-available>` and {ref}`read operation <sql-read-operations>`
statements, the connector supports the following features:

- {ref}`sql-write-operations`:

  - {ref}`sql-data-management`, see details for  {ref}`Delta Lake data
    management <delta-lake-data-management>`
  - {ref}`sql-schema-table-management`, see details for  {ref}`Delta Lake schema
    and table management <delta-lake-schema-table-management>`
  - {ref}`sql-view-management`

(delta-time-travel)=
### Time travel queries

The connector offers the ability to query historical data. This allows to
query the table as it was when a previous snapshot of the table was taken, even
if the data has since been modified or deleted.

The historical data of the table can be retrieved by specifying the version
number corresponding to the version of the table to be retrieved:

```sql
SELECT *
FROM example.testdb.customer_orders FOR VERSION AS OF 3
```

Use the `$history` metadata table to determine the snapshot ID of the
table like in the following query:

```sql
SELECT version, operation
FROM example.testdb."customer_orders$history"
ORDER BY version DESC
```

### Procedures

Use the {doc}`/sql/call` statement to perform data manipulation or
administrative tasks. Procedures are available in the system schema of each
catalog. The following code snippet displays how to call the
`example_procedure` in the `examplecatalog` catalog:

```sql
CALL examplecatalog.system.example_procedure()
```

(delta-lake-register-table)=
#### Register table

The connector can register existing Delta Lake tables into the metastore if
`delta.register-table-procedure.enabled` is set to `true` for the catalog.

The `system.register_table` procedure allows the caller to register an
existing Delta Lake table in the metastore, using its existing transaction logs
and data files:

```sql
CALL example.system.register_table(schema_name => 'testdb', table_name => 'customer_orders', table_location => 's3://my-bucket/a/path')
```

To prevent unauthorized users from accessing data, this procedure is disabled by
default. The procedure is enabled only when
`delta.register-table-procedure.enabled` is set to `true`.

(delta-lake-unregister-table)=
#### Unregister table

The connector can remove existing Delta Lake tables from the metastore. Once
unregistered, you can no longer query the table from Trino.

The procedure `system.unregister_table` allows the caller to unregister an
existing Delta Lake table from the metastores without deleting the data:

```sql
CALL example.system.unregister_table(schema_name => 'testdb', table_name => 'customer_orders')
```

(delta-lake-flush-metadata-cache)=
#### Flush metadata cache

- `system.flush_metadata_cache()`

  Flushes all metadata caches.

- `system.flush_metadata_cache(schema_name => ..., table_name => ...)`

  Flushes metadata cache entries of a specific table.
  Procedure requires passing named parameters.

(delta-lake-vacuum)=
#### `VACUUM`

The `VACUUM` procedure removes all old files that are not in the transaction
log, as well as files that are not needed to read table snapshots newer than the
current time minus the retention period defined by the `retention period`
parameter.

Users with `INSERT` and `DELETE` permissions on a table can run `VACUUM`
as follows:

```sql
CALL example.system.vacuum('exampleschemaname', 'exampletablename', '7d');
```

All parameters are required and must be presented in the following order:

- Schema name
- Table name
- Retention period

The `delta.vacuum.min-retention` configuration property provides a safety
measure to ensure that files are retained as expected. The minimum value for
this property is `0s`. There is a minimum retention session property as well,
`vacuum_min_retention`.

(delta-lake-data-management)=
### Data management

You can use the connector to {doc}`/sql/insert`, {doc}`/sql/delete`,
{doc}`/sql/update`, and {doc}`/sql/merge` data in Delta Lake tables.

Write operations are supported for tables stored on the following systems:

- Azure ADLS Gen2, Google Cloud Storage

  Writes to the Azure ADLS Gen2 and Google Cloud Storage are
  enabled by default. Trino detects write collisions on these storage systems
  when writing from multiple Trino clusters, or from other query engines.

- S3 and S3-compatible storage

  Writes to Amazon S3 and S3-compatible storage must be enabled
  with the `delta.enable-non-concurrent-writes` property. Writes to S3 can
  safely be made from multiple Trino clusters; however, write collisions are not
  detected when writing concurrently from other Delta Lake engines. You must
  make sure that no concurrent data modifications are run to avoid data
  corruption.

(delta-lake-schema-table-management)=
### Schema and table management

The {ref}`sql-schema-table-management` functionality includes support for:

- {doc}`/sql/create-table`
- {doc}`/sql/create-table-as`
- {doc}`/sql/drop-table`
- {doc}`/sql/alter-table`, see details for {ref}`Delta Lake ALTER TABLE
  <delta-lake-alter-table>`
- {doc}`/sql/create-schema`
- {doc}`/sql/drop-schema`
- {doc}`/sql/alter-schema`
- {doc}`/sql/comment`

The connector supports creating schemas. You can create a schema with or without
a specified location.

You can create a schema with the {doc}`/sql/create-schema` statement and the
`location` schema property. Tables in this schema are located in a
subdirectory under the schema location. Data files for tables in this schema
using the default location are cleaned up if the table is dropped:

```sql
CREATE SCHEMA example.example_schema
WITH (location = 's3://my-bucket/a/path');
```

Optionally, the location can be omitted. Tables in this schema must have a
location included when you create them. The data files for these tables are not
removed if the table is dropped:

```sql
CREATE SCHEMA example.example_schema;
```

When Delta Lake tables exist in storage but not in the metastore, Trino can be
used to register the tables:

```sql
CALL example.system.register_table(schema_name => 'testdb', table_name => 'example_table', table_location => 's3://my-bucket/a/path')
```

The table schema is read from the transaction log instead. If the
schema is changed by an external system, Trino automatically uses the new
schema.

:::{warning}
Using `CREATE TABLE` with an existing table content is disallowed,
use the `system.register_table` procedure instead.
:::

If the specified location does not already contain a Delta table, the connector
automatically writes the initial transaction log entries and registers the table
in the metastore. As a result, any Databricks engine can write to the table:

```sql
CREATE TABLE example.default.new_table (id BIGINT, address VARCHAR);
```

The Delta Lake connector also supports creating tables using the {doc}`CREATE
TABLE AS </sql/create-table-as>` syntax.

(delta-lake-alter-table)=
The connector supports the following [](/sql/alter-table) statements.

(delta-lake-create-or-replace)=
#### Replace tables

The connector supports replacing an existing table as an atomic operation.
Atomic table replacement creates a new snapshot with the new table definition as
part of the [table history](#delta-lake-history-table).

To replace a table, use [`CREATE OR REPLACE TABLE`](/sql/create-table) or
[`CREATE OR REPLACE TABLE AS`](/sql/create-table-as).

In this example, a table `example_table` is replaced by a completely new
definition and data from the source table:

```sql
CREATE OR REPLACE TABLE example_table
WITH (partitioned_by = ARRAY['a'])
AS SELECT * FROM another_table;
```

(delta-lake-alter-table-execute)=
#### ALTER TABLE EXECUTE

The connector supports the following commands for use with {ref}`ALTER TABLE
EXECUTE <alter-table-execute>`.

```{include} optimize.fragment
```

(delta-lake-alter-table-rename-to)=
#### ALTER TABLE RENAME TO

The connector only supports the `ALTER TABLE RENAME TO` statement when met with
one of the following conditions:

* The table type is external.
* The table is backed by a metastore that does not perform object storage
  operations, for example, AWS Glue.

#### Table properties

The following table properties are available for use:

:::{list-table} Delta Lake table properties
:widths: 40, 60
:header-rows: 1

* - Property name
  - Description
* - `location`
  - File system location URI for the table.
* - `partitioned_by`
  - Set partition columns.
* - `checkpoint_interval`
  - Set the checkpoint interval in number of table writes.
* - `change_data_feed_enabled`
  - Enables storing change data feed entries.
* - `column_mapping_mode`
  - Column mapping mode. Possible values are:

    * `ID`
    * `NAME`
    * `NONE`

    Defaults to `NONE`.
* - `deletion_vectors_enabled`
  - Enables deletion vectors.
:::

The following example uses all available table properties:

```sql
CREATE TABLE example.default.example_partitioned_table
WITH (
  location = 's3://my-bucket/a/path',
  partitioned_by = ARRAY['regionkey'],
  checkpoint_interval = 5,
  change_data_feed_enabled = false,
  column_mapping_mode = 'name',
  deletion_vectors_enabled = false
)
AS SELECT name, comment, regionkey FROM tpch.tiny.nation;
```

(delta-lake-shallow-clone)=
#### Shallow cloned tables

The connector supports read and write operations on shallow cloned tables. Trino
does not support creating shallow clone tables. More information about shallow
cloning is available in the [Delta Lake
documentation](https://docs.delta.io/latest/delta-utility.html#shallow-clone-a-delta-table).

Shallow cloned tables let you test queries or experiment with changes to a table
without duplicating data.

#### Metadata tables

The connector exposes several metadata tables for each Delta Lake table.
These metadata tables contain information about the internal structure
of the Delta Lake table. You can query each metadata table by appending the
metadata table name to the table name:

```sql
SELECT * FROM "test_table$history"
```

(delta-lake-history-table)=
##### `$history` table

The `$history` table provides a log of the metadata changes performed on
the Delta Lake table.

You can retrieve the changelog of the Delta Lake table `test_table`
by using the following query:

```sql
SELECT * FROM "test_table$history"
```

```text
 version |               timestamp               | user_id | user_name |  operation   |         operation_parameters          |                 cluster_id      | read_version |  isolation_level  | is_blind_append | operation_metrics              
---------+---------------------------------------+---------+-----------+--------------+---------------------------------------+---------------------------------+--------------+-------------------+-----------------+-------------------
       2 | 2023-01-19 07:40:54.684 Europe/Vienna | trino   | trino     | WRITE        | {queryId=20230119_064054_00008_4vq5t} | trino-406-trino-coordinator     |            2 | WriteSerializable | true            | {}
       1 | 2023-01-19 07:40:41.373 Europe/Vienna | trino   | trino     | ADD COLUMNS  | {queryId=20230119_064041_00007_4vq5t} | trino-406-trino-coordinator     |            0 | WriteSerializable | true            | {}
       0 | 2023-01-19 07:40:10.497 Europe/Vienna | trino   | trino     | CREATE TABLE | {queryId=20230119_064010_00005_4vq5t} | trino-406-trino-coordinator     |            0 | WriteSerializable | true            | {}
```

The output of the query has the following history columns:

:::{list-table} History columns
:widths: 30, 30, 40
:header-rows: 1

* - Name
  - Type
  - Description
* - `version`
  - `BIGINT`
  - The version of the table corresponding to the operation
* - `timestamp`
  - `TIMESTAMP(3) WITH TIME ZONE`
  - The time when the table version became active
* - `user_id`
  - `VARCHAR`
  - The identifier for the user which performed the operation
* - `user_name`
  - `VARCHAR`
  - The username for the user which performed the operation
* - `operation`
  - `VARCHAR`
  - The name of the operation performed on the table
* - `operation_parameters`
  - `map(VARCHAR, VARCHAR)`
  - Parameters of the operation
* - `cluster_id`
  - `VARCHAR`
  - The ID of the cluster which ran the operation
* - `read_version`
  - `BIGINT`
  - The version of the table which was read in order to perform the operation
* - `isolation_level`
  - `VARCHAR`
  - The level of isolation used to perform the operation
* - `is_blind_append`
  - `BOOLEAN`
  - Whether or not the operation appended data
* - `operation_metrics`
  - `map(VARCHAR, VARCHAR)`
  - Metrics of the operation
  :::

(delta-lake-partitions-table)=
##### `$partitions` table

The `$partitions` table provides a detailed overview of the partitions of the
Delta Lake table.

You can retrieve the information about the partitions of the Delta Lake table
`test_table` by using the following query:

```sql
SELECT * FROM "test_table$partitions"
```

```text
           partition           | file_count | total_size |                     data                     |
-------------------------------+------------+------------+----------------------------------------------+
{_bigint=1, _date=2021-01-12}  |          2 |        884 | {_decimal={min=1.0, max=2.0, null_count=0}}  |
{_bigint=1, _date=2021-01-13}  |          1 |        442 | {_decimal={min=1.0, max=1.0, null_count=0}}  |
```

The output of the query has the following columns:

:::{list-table} Partitions columns
:widths: 20, 30, 50
:header-rows: 1

* - Name
  - Type
  - Description
* - `partition`
  - `ROW(...)`
  - A row that contains the mapping of the partition column names to the
    partition column values.
* - `file_count`
  - `BIGINT`
  - The number of files mapped in the partition.
* - `total_size`
  - `BIGINT`
  - The size of all the files in the partition.
* - `data`
  - `ROW(... ROW (min ..., max ... , null_count BIGINT))`
  - Partition range and null counts.
:::

##### `$properties` table

The `$properties` table provides access to Delta Lake table configuration,
table features and table properties. The table rows are key/value pairs.

You can retrieve the properties of the Delta
table `test_table` by using the following query:

```sql
SELECT * FROM "test_table$properties"
```

```text
 key                        | value           |
----------------------------+-----------------+
delta.minReaderVersion      | 1               |
delta.minWriterVersion      | 4               |
delta.columnMapping.mode    | name            |
delta.feature.columnMapping | supported       |
```

(delta-lake-special-columns)=
#### Metadata columns

In addition to the defined columns, the Delta Lake connector automatically
exposes metadata in a number of hidden columns in each table. You can use these
columns in your SQL statements like any other column, e.g., they can be selected
directly or used in conditional statements.

- `$path`
  : Full file system path name of the file for this row.
- `$file_modified_time`
  : Date and time of the last modification of the file for this row.
- `$file_size`
  : Size of the file for this row.

### Table functions

The connector provides the following table functions:

#### table_changes

Allows reading Change Data Feed (CDF) entries to expose row-level changes
between two versions of a Delta Lake table. When the `change_data_feed_enabled`
table property is set to `true` on a specific Delta Lake table,
the connector records change events for all data changes on the table.
This is how these changes can be read:

```sql
SELECT
  *
FROM
  TABLE(
    system.table_changes(
      schema_name => 'test_schema',
      table_name => 'tableName',
      since_version => 0
    )
  );
```

`schema_name` - type `VARCHAR`, required, name of the schema for which the function is called

`table_name` - type `VARCHAR`, required, name of the table for which the function is called

`since_version` - type `BIGINT`, optional, version from which changes are shown, exclusive

In addition to returning the columns present in the table, the function
returns the following values for each change event:

- `_change_type`
  : Gives the type of change that occurred. Possible values are `insert`,
    `delete`, `update_preimage` and `update_postimage`.
- `_commit_version`
  : Shows the table version for which the change occurred.
- `_commit_timestamp`
  : Represents the timestamp for the commit in which the specified change happened.

This is how it would be normally used:

Create table:

```sql
CREATE TABLE test_schema.pages (page_url VARCHAR, domain VARCHAR, views INTEGER)
    WITH (change_data_feed_enabled = true);
```

Insert data:

```sql
INSERT INTO test_schema.pages
    VALUES
        ('url1', 'domain1', 1),
        ('url2', 'domain2', 2),
        ('url3', 'domain1', 3);
INSERT INTO test_schema.pages
    VALUES
        ('url4', 'domain1', 400),
        ('url5', 'domain2', 500),
        ('url6', 'domain3', 2);
```

Update data:

```sql
UPDATE test_schema.pages
    SET domain = 'domain4'
    WHERE views = 2;
```

Select changes:

```sql
SELECT
  *
FROM
  TABLE(
    system.table_changes(
      schema_name => 'test_schema',
      table_name => 'pages',
      since_version => 1
    )
  )
ORDER BY _commit_version ASC;
```

The preceding sequence of SQL statements returns the following result:

```text
page_url    |     domain     |    views    |    _change_type     |    _commit_version    |    _commit_timestamp
url4        |     domain1    |    400      |    insert           |     2                 |    2023-03-10T21:22:23.000+0000
url5        |     domain2    |    500      |    insert           |     2                 |    2023-03-10T21:22:23.000+0000
url6        |     domain3    |    2        |    insert           |     2                 |    2023-03-10T21:22:23.000+0000
url2        |     domain2    |    2        |    update_preimage  |     3                 |    2023-03-10T22:23:24.000+0000
url2        |     domain4    |    2        |    update_postimage |     3                 |    2023-03-10T22:23:24.000+0000
url6        |     domain3    |    2        |    update_preimage  |     3                 |    2023-03-10T22:23:24.000+0000
url6        |     domain4    |    2        |    update_postimage |     3                 |    2023-03-10T22:23:24.000+0000
```

The output shows what changes happen in which version.
For example in version 3 two rows were modified, first one changed from
`('url2', 'domain2', 2)` into `('url2', 'domain4', 2)` and the second from
`('url6', 'domain2', 2)` into `('url6', 'domain4', 2)`.

If `since_version` is not provided the function produces change events
starting from when the table was created.

```sql
SELECT
  *
FROM
  TABLE(
    system.table_changes(
      schema_name => 'test_schema',
      table_name => 'pages'
    )
  )
ORDER BY _commit_version ASC;
```

The preceding SQL statement returns the following result:

```text
page_url    |     domain     |    views    |    _change_type     |    _commit_version    |    _commit_timestamp
url1        |     domain1    |    1        |    insert           |     1                 |    2023-03-10T20:21:22.000+0000
url2        |     domain2    |    2        |    insert           |     1                 |    2023-03-10T20:21:22.000+0000
url3        |     domain1    |    3        |    insert           |     1                 |    2023-03-10T20:21:22.000+0000
url4        |     domain1    |    400      |    insert           |     2                 |    2023-03-10T21:22:23.000+0000
url5        |     domain2    |    500      |    insert           |     2                 |    2023-03-10T21:22:23.000+0000
url6        |     domain3    |    2        |    insert           |     2                 |    2023-03-10T21:22:23.000+0000
url2        |     domain2    |    2        |    update_preimage  |     3                 |    2023-03-10T22:23:24.000+0000
url2        |     domain4    |    2        |    update_postimage |     3                 |    2023-03-10T22:23:24.000+0000
url6        |     domain3    |    2        |    update_preimage  |     3                 |    2023-03-10T22:23:24.000+0000
url6        |     domain4    |    2        |    update_postimage |     3                 |    2023-03-10T22:23:24.000+0000
```

You can see changes that occurred at version 1 as three inserts. They are
not visible in the previous statement when `since_version` value was set to 1.

## Performance

The connector includes a number of performance improvements detailed in the
following sections:

- Support for {doc}`write partitioning </admin/properties-write-partitioning>`.

(delta-lake-table-statistics)=
### Table statistics

Use {doc}`/sql/analyze` statements in Trino to populate data size and
number of distinct values (NDV) extended table statistics in Delta Lake.
The minimum value, maximum value, value count, and null value count
statistics are computed on the fly out of the transaction log of the
Delta Lake table. The {doc}`cost-based optimizer
</optimizer/cost-based-optimizations>` then uses these statistics to improve
query performance.

Extended statistics enable a broader set of optimizations, including join
reordering. The controlling catalog property `delta.table-statistics-enabled`
is enabled by default. The equivalent {ref}`catalog session property
<session-properties-definition>` is `statistics_enabled`.

Each `ANALYZE` statement updates the table statistics incrementally, so only
the data changed since the last `ANALYZE` is counted. The table statistics are
not automatically updated by write operations such as `INSERT`, `UPDATE`,
and `DELETE`. You must manually run `ANALYZE` again to update the table
statistics.

To collect statistics for a table, execute the following statement:

```sql
ANALYZE table_schema.table_name;
```

To recalculate from scratch the statistics for the table use additional parameter `mode`:

> ANALYZE table_schema.table_name WITH(mode = 'full_refresh');

There are two modes available `full_refresh` and `incremental`.
The procedure use `incremental` by default.

To gain the most benefit from cost-based optimizations, run periodic `ANALYZE`
statements on every large table that is frequently queried.

#### Fine-tuning

The `files_modified_after` property is useful if you want to run the
`ANALYZE` statement on a table that was previously analyzed. You can use it to
limit the amount of data used to generate the table statistics:

```sql
ANALYZE example_table WITH(files_modified_after = TIMESTAMP '2021-08-23
16:43:01.321 Z')
```

As a result, only files newer than the specified time stamp are used in the
analysis.

You can also specify a set or subset of columns to analyze using the `columns`
property:

```sql
ANALYZE example_table WITH(columns = ARRAY['nationkey', 'regionkey'])
```

To run `ANALYZE` with `columns` more than once, the next `ANALYZE` must
run on the same set or a subset of the original columns used.

To broaden the set of `columns`, drop the statistics and reanalyze the table.

#### Disable and drop extended statistics

You can disable extended statistics with the catalog configuration property
`delta.extended-statistics.enabled` set to `false`. Alternatively, you can
disable it for a session, with the {doc}`catalog session property
</sql/set-session>` `extended_statistics_enabled` set to `false`.

If a table is changed with many delete and update operation, calling `ANALYZE`
does not result in accurate statistics. To correct the statistics, you have to
drop the extended statistics and analyze the table again.

Use the `system.drop_extended_stats` procedure in the catalog to drop the
extended statistics for a specified table in a specified schema:

```sql
CALL example.system.drop_extended_stats('example_schema', 'example_table')
```

### Memory usage

The Delta Lake connector is memory intensive and the amount of required memory
grows with the size of Delta Lake transaction logs of any accessed tables. It is
important to take that into account when provisioning the coordinator.

You must decrease memory usage by keeping the number of active data files in
the table low by regularly running `OPTIMIZE` and `VACUUM` in Delta Lake.

#### Memory monitoring

When using the Delta Lake connector, you must monitor memory usage on the
coordinator. Specifically, monitor JVM heap utilization using standard tools as
part of routine operation of the cluster.

A good proxy for memory usage is the cache utilization of Delta Lake caches. It
is exposed by the connector with the
`plugin.deltalake.transactionlog:name=<catalog-name>,type=transactionlogaccess`
JMX bean.

You can access it with any standard monitoring software with JMX support, or use
the {doc}`/connector/jmx` with the following query:

```sql
SELECT * FROM jmx.current."*.plugin.deltalake.transactionlog:name=<catalog-name>,type=transactionlogaccess"
```

Following is an example result:

```text
datafilemetadatacachestats.hitrate      | 0.97
datafilemetadatacachestats.missrate     | 0.03
datafilemetadatacachestats.requestcount | 3232
metadatacachestats.hitrate              | 0.98
metadatacachestats.missrate             | 0.02
metadatacachestats.requestcount         | 6783
node                                    | trino-master
object_name                             | io.trino.plugin.deltalake.transactionlog:type=TransactionLogAccess,name=delta
```

In a healthy system, both `datafilemetadatacachestats.hitrate` and
`metadatacachestats.hitrate` are close to `1.0`.

(delta-lake-table-redirection)=
### Table redirection

```{include} table-redirection.fragment
```

The connector supports redirection from Delta Lake tables to Hive tables
with the `delta.hive-catalog-name` catalog configuration property.

### Performance tuning configuration properties

The following table describes performance tuning catalog properties specific to
the Delta Lake connector.

:::{warning}
Performance tuning configuration properties are considered expert-level
features. Altering these properties from their default values is likely to
cause instability and performance degradation. It is strongly suggested that
you use them only to address non-trivial performance issues, and that you
keep a backup of the original values if you change them.
:::

:::{list-table} Delta Lake performance tuning configuration properties
:widths: 30, 50, 20
:header-rows: 1

* - Property name
  - Description
  - Default
* - `delta.domain-compaction-threshold`
  - Minimum size of query predicates above which Trino compacts the predicates.
    Pushing a large list of predicates down to the data source can compromise
    performance. For optimization in that situation, Trino can compact the large
    predicates. If necessary, adjust the threshold to ensure a balance between
    performance and predicate pushdown.
  - `1000`
* - `delta.max-outstanding-splits`
  - The target number of buffered splits for each table scan in a query, before
    the scheduler tries to pause.
  - `1000`
* - `delta.max-splits-per-second`
  - Sets the maximum number of splits used per second to access underlying
    storage. Reduce this number if your limit is routinely exceeded, based on
    your filesystem limits. This is set to the absolute maximum value, which
    results in Trino maximizing the parallelization of data access by default.
    Attempting to set it higher results in Trino not being able to start.
  - `Integer.MAX_VALUE`
* - `delta.max-split-size`
  - Sets the largest [](prop-type-data-size) for a single read section
    assigned to a worker after `max-initial-splits` have been processed. You can
    also use the corresponding catalog session property
    `<catalog-name>.max_split_size`.
  - `64MB`
* - `delta.minimum-assigned-split-weight`
  - A decimal value in the range (0, 1] used as a minimum for weights assigned
    to each split. A low value might improve performance on tables with small
    files. A higher value might improve performance for queries with highly
    skewed aggregations or joins.
  - `0.05`
* - `delta.projection-pushdown-enabled`
  - Read only projected fields from row columns while performing `SELECT` queries
  - `true`
* - `delta.query-partition-filter-required`
  - Set to `true` to force a query to use a partition filter. You can use the
    `query_partition_filter_required` catalog session property for temporary,
    catalog specific use.
  - `false`
:::

### File system cache

The connector supports configuring and using [file system
caching](/object-storage/file-system-cache).

The following table describes file system cache properties specific to 
the Delta Lake connector.

:::{list-table} Delta Lake file system cache configuration properties
:widths: 30, 50, 20
:header-rows: 1

* - Property name
  - Description
  - Default
* - `delta.fs.cache.disable-transaction-log-caching`
  - Set to `true` to disable caching of the `_delta_log` directory of 
    Delta Tables. This is useful in those cases when Delta Tables are 
    destroyed and recreated, and the files inside the transaction log 
    directory get overwritten and cannot be safely cached. Effective 
    only when `fs.cache.enabled=true`.
  - `false`
:::
