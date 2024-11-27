# Iceberg connector

```{raw} html
<img src="../_static/img/iceberg.png" class="connector-logo">
```

Apache Iceberg is an open table format for huge analytic datasets. The Iceberg
connector allows querying data stored in files written in Iceberg format, as
defined in the [Iceberg Table Spec](https://iceberg.apache.org/spec/). The
connector supports Apache Iceberg table spec versions 1 and 2.

The table state is maintained in metadata files. All changes to table
state create a new metadata file and replace the old metadata with an atomic
swap. The table metadata file tracks the table schema, partitioning
configuration, custom properties, and snapshots of the table contents.

Iceberg data files are stored in either Parquet, ORC, or Avro format, as
determined by the `format` property in the table definition.

Iceberg is designed to improve on the known scalability limitations of Hive,
which stores table metadata in a metastore that is backed by a relational
database such as MySQL.  It tracks partition locations in the metastore, but not
individual data files.  Trino queries using the {doc}`/connector/hive` must
first call the metastore to get partition locations, then call the underlying
file system to list all data files inside each partition, and then read metadata
from each data file.

Since Iceberg stores the paths to data files in the metadata files, it only
consults the underlying file system for files that must be read.

## Requirements

To use Iceberg, you need:

- Network access from the Trino coordinator and workers to the distributed
  object storage.

- Access to a {ref}`Hive metastore service (HMS) <hive-thrift-metastore>`, an
  {ref}`AWS Glue catalog <iceberg-glue-catalog>`, a {ref}`JDBC catalog
  <iceberg-jdbc-catalog>`, a {ref}`REST catalog <iceberg-rest-catalog>`,
  a {ref}`Nessie server <iceberg-nessie-catalog>`, or a
  {ref}`Snowflake catalog <iceberg-snowflake-catalog>`.

- Data files stored in the file formats
  [Parquet](parquet-format-configuration)(default),
  [ORC](orc-format-configuration), or Avro on a [supported file
  system](iceberg-file-system-configuration).

## General configuration

To configure the Iceberg connector, create a catalog properties file
`etc/catalog/example.properties` that references the `iceberg` connector.

The [Hive metastore catalog](hive-thrift-metastore) is the default
implementation.

You must select and configure one of the [supported file
systems](iceberg-file-system-configuration).

```properties
connector.name=iceberg
hive.metastore.uri=thrift://example.net:9083
fs.x.enabled=true
```

Replace the `fs.x.enabled` configuration property with the desired file system.

Other metadata catalog types as listed in the requirements section of this topic
are available. Each metastore type has specific configuration properties along
with {ref}`general metastore configuration properties
<general-metastore-properties>`.

The following configuration properties are independent of which catalog
implementation is used:

:::{list-table} Iceberg general configuration properties
:widths: 30, 58, 12
:header-rows: 1

* - Property name
  - Description
  - Default
* - `iceberg.catalog.type`
  - Define the [metastore type](general-metastore-properties) to use. Possible
    values are:

    * `hive_metastore`
    * `glue`
    * `jdbc`
    * `rest`
    * `nessie`
    * `snowflake`
  - `hive_metastore`
* - `iceberg.file-format`
  - Define the data storage file format for Iceberg tables. Possible values are:

    * `PARQUET`
    * `ORC`
    * `AVRO`
  - `PARQUET`
* - `iceberg.compression-codec`
  - The compression codec used when writing files. Possible values are:

      * `NONE`
      * `SNAPPY`
      * `LZ4`
      * `ZSTD`
      * `GZIP`
  - `ZSTD`
* - `iceberg.use-file-size-from-metadata`
  - Read file sizes from metadata instead of file system. This property must
    only be used as a workaround for [this
    issue](https://github.com/apache/iceberg/issues/1980). The problem was fixed
    in Iceberg version 0.11.0.
  - `true`
* - `iceberg.max-partitions-per-writer`
  - Maximum number of partitions handled per writer.
  - `100`
* - `iceberg.target-max-file-size`
  - Target maximum size of written files; the actual size may be larger.
  - `1GB`
* - `iceberg.unique-table-location`
  - Use randomized, unique table locations.
  - `true`
* - `iceberg.dynamic-filtering.wait-timeout`
  - Maximum duration to wait for completion of dynamic filters during split
    generation.
  - `1s`
* - `iceberg.delete-schema-locations-fallback`
  - Whether schema locations are deleted when Trino can't determine whether
    they contain external files.
  - `false`
* - `iceberg.minimum-assigned-split-weight`
  - A decimal value in the range `(0, 1]` used as a minimum for weights assigned
    to each split. A low value may improve performance on tables with small
    files. A higher value may improve performance for queries with highly skewed
    aggregations or joins.
  - 0.05
* - `iceberg.table-statistics-enabled`
  - Enable [](/optimizer/statistics). The equivalent [catalog session
    property](/sql/set-session) is `statistics_enabled` for session specific
    use. Set to `false` to prevent statistics usage by the
    [](/optimizer/cost-based-optimizations) to make better decisions about the
    query plan and therefore improve query processing performance. Setting to
    `false` is not recommended and does not disable statistics gathering.
  - `true`
* - `iceberg.extended-statistics.enabled`
  - Enable statistics collection with [](/sql/analyze) and use of extended
    statistics. The equivalent catalog session property is
    `extended_statistics_enabled`.
  - `true`
* - `iceberg.extended-statistics.collect-on-write`
  - Enable collection of extended statistics for write operations. The
    equivalent catalog session property is
    `collect_extended_statistics_on_write`.
  - `true`
* - `iceberg.projection-pushdown-enabled`
  - Enable [projection pushdown](/optimizer/pushdown)
  - `true`
* - `iceberg.hive-catalog-name`
  - Catalog to redirect to when a Hive table is referenced.
  -
* - `iceberg.materialized-views.storage-schema`
  - Schema for creating materialized views storage tables. When this property is
    not configured, storage tables are created in the same schema as the
    materialized view definition. When the `storage_schema` materialized view
    property is specified, it takes precedence over this catalog property.
  - Empty
* - `iceberg.register-table-procedure.enabled`
  - Enable to allow user to call [`register_table` procedure](iceberg-register-table).
  - `false`
* - `iceberg.add-files-procedure.enabled`
  - Enable to allow user to call [`add_files` procedure](iceberg-add-files).
  - `false`
* - `iceberg.query-partition-filter-required`
  - Set to `true` to force a query to use a partition filter for schemas
    specified with `iceberg.query-partition-filter-required-schemas`. Equivalent
    catalog session property is `query_partition_filter_required`.
  - `false`
* - `iceberg.query-partition-filter-required-schemas`
  - Specify the list of schemas for which Trino can enforce that queries use a
    filter on partition keys for source tables. Equivalent session property is
    `query_partition_filter_required_schemas`. The list is used if the
    `iceberg.query-partition-filter-required` configuration property or the
    `query_partition_filter_required` catalog session property is set to `true`.
  - `[]`
* - `iceberg.incremental-refresh-enabled`
  - Set to `false` to force the materialized view refresh operation to always
    perform a full refresh. You can use the `incremental_refresh_enabled`
    catalog session property for temporary, catalog specific use. In the
    majority of cases, using incremental refresh, as compared to a full refresh,
    is beneficial since a much smaller subset of the source tables needs to be
    scanned. While incremental refresh may scan less data, it may result in the
    creation of more data files, since it uses the append operation to insert
    the new records.
  - `true`
* - `iceberg.metadata-cache.enabled`
  - Set to `false` to disable in-memory caching of metadata files on the 
    coordinator. This cache is not used when `fs.cache.enabled` is set to true.
  - `true`
* - `iceberg.object-store-layout.enabled`
  - Set to `true` to enable Iceberg's [object store file layout](https://iceberg.apache.org/docs/latest/aws/#object-store-file-layout). 
    Enabling the object store file layout appends a deterministic hash directly 
    after the data write path.
  - `false`
* - `iceberg.expire-snapshots.min-retention`
  -  Minimal retention period for the
     [`expire_snapshot` command](iceberg-expire-snapshots).
     Equivalent session property is `expire_snapshots_min_retention`.
  -  `7d` 
* - `iceberg.remove-orphan-files.min-retention`
  -  Minimal retention period for the 
     [`remove_orphan_files` command](iceberg-remove-orphan-files).
     Equivalent session property is `remove_orphan_files_min_retention`.
  -  `7d`
* - `iceberg.idle-writer-min-file-size`
  -  Minimum data written by a single partition writer before it can
     be considered as idle and can be closed by the engine. Equivalent
     session property is `idle_writer_min_file_size`.
  -  `16MB`
* - `iceberg.sorted-writing-enabled`
  -  Enable [sorted writing](iceberg-sorted-files) to tables with a specified sort order. Equivalent
     session property is `sorted_writing_enabled`.
  -  `true` 
* - `iceberg.allowed-extra-properties`
  -  List of extra properties that are allowed to be set on Iceberg tables.
     Use `*` to allow all properties.
  - `[]`
* - `iceberg.split-manager-threads`
  -  Number of threads to use for generating splits.
  -  Double the number of processors on the coordinator node.
* - `iceberg.metadata.parallelism`
  - Number of threads used for retrieving metadata. Currently, only table loading 
    is parallelized.
  - `8`
:::

(iceberg-fte-support)=
### Fault-tolerant execution support

The connector supports {doc}`/admin/fault-tolerant-execution` of query
processing. Read and write operations are both supported with any retry policy.


(iceberg-file-system-configuration)=
## File system access configuration

The connector supports accessing the following file systems:

* [](/object-storage/file-system-azure)
* [](/object-storage/file-system-gcs)
* [](/object-storage/file-system-s3)
* [](/object-storage/file-system-hdfs)

You must enable and configure the specific file system access. [Legacy
support](file-system-legacy) is not recommended and will be removed.

## Type mapping

The connector reads and writes data into the supported data file formats Avro,
ORC, and Parquet, following the Iceberg specification.

Because Trino and Iceberg each support types that the other does not, this
connector {ref}`modifies some types <type-mapping-overview>` when reading or
writing data. Data types may not map the same way in both directions between
Trino and the data source. Refer to the following sections for type mapping in
each direction.

The Iceberg specification includes supported data types and the mapping to the
formating in the Avro, ORC, or Parquet files:

- [Iceberg to Avro](https://iceberg.apache.org/spec/#avro)
- [Iceberg to ORC](https://iceberg.apache.org/spec/#orc)
- [Iceberg to Parquet](https://iceberg.apache.org/spec/#parquet)

### Iceberg to Trino type mapping

The connector maps Iceberg types to the corresponding Trino types according to
the following table:

:::{list-table} Iceberg to Trino type mapping
:widths: 40, 60
:header-rows: 1

* - Iceberg type
  - Trino type
* - `BOOLEAN`
  - `BOOLEAN`
* - `INT`
  - `INTEGER`
* - `LONG`
  - `BIGINT`
* - `FLOAT`
  - `REAL`
* - `DOUBLE`
  - `DOUBLE`
* - `DECIMAL(p,s)`
  - `DECIMAL(p,s)`
* - `DATE`
  - `DATE`
* - `TIME`
  - `TIME(6)`
* - `TIMESTAMP`
  - `TIMESTAMP(6)`
* - `TIMESTAMPTZ`
  - `TIMESTAMP(6) WITH TIME ZONE`
* - `STRING`
  - `VARCHAR`
* - `UUID`
  - `UUID`
* - `BINARY`
  - `VARBINARY`
* - `FIXED (L)`
  - `VARBINARY`
* - `STRUCT(...)`
  - `ROW(...)`
* - `LIST(e)`
  - `ARRAY(e)`
* - `MAP(k,v)`
  - `MAP(k,v)`
:::

No other types are supported.

### Trino to Iceberg type mapping

The connector maps Trino types to the corresponding Iceberg types according to
the following table:

:::{list-table} Trino to Iceberg type mapping
:widths: 40, 60
:header-rows: 1

* - Trino type
  - Iceberg type
* - `BOOLEAN`
  - `BOOLEAN`
* - `INTEGER`
  - `INT`
* - `BIGINT`
  - `LONG`
* - `REAL`
  - `FLOAT`
* - `DOUBLE`
  - `DOUBLE`
* - `DECIMAL(p,s)`
  - `DECIMAL(p,s)`
* - `DATE`
  - `DATE`
* - `TIME(6)`
  - `TIME`
* - `TIMESTAMP(6)`
  - `TIMESTAMP`
* - `TIMESTAMP(6) WITH TIME ZONE`
  - `TIMESTAMPTZ`
* - `VARCHAR`
  - `STRING`
* - `UUID`
  - `UUID`
* - `VARBINARY`
  - `BINARY`
* - `ROW(...)`
  - `STRUCT(...)`
* - `ARRAY(e)`
  - `LIST(e)`
* - `MAP(k,v)`
  - `MAP(k,v)`
:::

No other types are supported.

## Security

### Kerberos authentication

The Iceberg connector supports Kerberos authentication for the Hive metastore
and HDFS and is configured using the same parameters as the Hive connector. Find
more information in the [](/object-storage/file-system-hdfs) section.

(iceberg-authorization)=
### Authorization

The Iceberg connector allows you to choose one of several means of providing
authorization at the catalog level.

You can enable authorization checks for the connector by setting the
`iceberg.security` property in the catalog properties file. This property must
be one of the following values:

:::{list-table} Iceberg security values
:widths: 30, 60
:header-rows: 1

* - Property value
  - Description
* - `ALLOW_ALL`
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

(iceberg-sql-support)=
## SQL support

This connector provides read access and write access to data and metadata in
Iceberg. In addition to the {ref}`globally available <sql-globally-available>`
and {ref}`read operation <sql-read-operations>` statements, the connector
supports the following features:

- {ref}`sql-write-operations`:

  - {ref}`iceberg-schema-table-management` and {ref}`iceberg-tables`
  - {ref}`iceberg-data-management`
  - {ref}`sql-view-management`
  - {ref}`sql-materialized-view-management`, see also {ref}`iceberg-materialized-views`

### Basic usage examples

The connector supports creating schemas. You can create a schema with or without
a specified location.

You can create a schema with the {doc}`/sql/create-schema` statement and the
`location` schema property. The tables in this schema, which have no explicit
`location` set in {doc}`/sql/create-table` statement, are located in a
subdirectory under the directory corresponding to the schema location.

Create a schema on S3:

```
CREATE SCHEMA example.example_s3_schema
WITH (location = 's3://my-bucket/a/path/');
```

Create a schema on an S3-compatible object storage such as MinIO:

```
CREATE SCHEMA example.example_s3a_schema
WITH (location = 's3a://my-bucket/a/path/');
```

Create a schema on HDFS:

```
CREATE SCHEMA example.example_hdfs_schema
WITH (location='hdfs://hadoop-master:9000/user/hive/warehouse/a/path/');
```

Optionally, on HDFS, the location can be omitted:

```
CREATE SCHEMA example.example_hdfs_schema;
```

The Iceberg connector supports creating tables using the {doc}`CREATE TABLE
</sql/create-table>` syntax. Optionally, specify the {ref}`table properties
<iceberg-table-properties>` supported by this connector:

```
CREATE TABLE example_table (
    c1 INTEGER,
    c2 DATE,
    c3 DOUBLE
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['c1', 'c2'],
    sorted_by = ARRAY['c3'],
    location = 's3://my-bucket/a/path/'
);
```

When the `location` table property is omitted, the content of the table is
stored in a subdirectory under the directory corresponding to the schema
location.

The Iceberg connector supports creating tables using the {doc}`CREATE TABLE AS
</sql/create-table-as>` with {doc}`SELECT </sql/select>` syntax:

```
CREATE TABLE tiny_nation
WITH (
    format = 'PARQUET'
)
AS
    SELECT *
    FROM nation
    WHERE nationkey < 10;
```

Another flavor of creating tables with {doc}`CREATE TABLE AS
</sql/create-table-as>` is with {doc}`VALUES </sql/values>` syntax:

```
CREATE TABLE yearly_clicks (
    year,
    clicks
)
WITH (
    partitioning = ARRAY['year']
)
AS VALUES
    (2021, 10000),
    (2022, 20000);
```

### Procedures

Use the {doc}`/sql/call` statement to perform data manipulation or
administrative tasks. Procedures are available in the system schema of each
catalog. The following code snippet displays how to call the
`example_procedure` in the `examplecatalog` catalog:

```
CALL examplecatalog.system.example_procedure()
```

(iceberg-register-table)=
#### Register table

The connector can register existing Iceberg tables into the metastore if
`iceberg.register-table-procedure.enabled` is set to `true` for the catalog.

The procedure `system.register_table` allows the caller to register an
existing Iceberg table in the metastore, using its existing metadata and data
files:

```sql
CALL example.system.register_table(schema_name => 'testdb', table_name => 'customer_orders', table_location => 'hdfs://hadoop-master:9000/user/hive/warehouse/customer_orders-581fad8517934af6be1857a903559d44')
```

In addition, you can provide a file name to register a table with specific
metadata. This may be used to register the table with some specific table state,
or may be necessary if the connector cannot automatically figure out the
metadata version to use:

```sql
CALL example.system.register_table(schema_name => 'testdb', table_name => 'customer_orders', table_location => 'hdfs://hadoop-master:9000/user/hive/warehouse/customer_orders-581fad8517934af6be1857a903559d44', metadata_file_name => '00003-409702ba-4735-4645-8f14-09537cc0b2c8.metadata.json')
```

To prevent unauthorized users from accessing data, this procedure is disabled by
default. The procedure is enabled only when
`iceberg.register-table-procedure.enabled` is set to `true`.

(iceberg-unregister-table)=
#### Unregister table

The connector can remove existing Iceberg tables from the metastore. Once
unregistered, you can no longer query the table from Trino.

The procedure `system.unregister_table` allows the caller to unregister an
existing Iceberg table from the metastores without deleting the data:

```sql
CALL example.system.unregister_table(schema_name => 'testdb', table_name => 'customer_orders')
```

#### Migrate table

The connector can read from or write to Hive tables that have been migrated to
Iceberg.

Use the procedure `system.migrate` to move a table from the Hive format to the
Iceberg format, loaded with the source’s data files. Table schema, partitioning,
properties, and location are copied from the source table. A bucketed Hive table
will be migrated as a non-bucketed Iceberg table. The data files in the Hive table
must use the Parquet, ORC, or Avro file format.

The procedure must be called for a specific catalog `example` with the
relevant schema and table names supplied with the required parameters
`schema_name` and `table_name`:

```
CALL example.system.migrate(
    schema_name => 'testdb',
    table_name => 'customer_orders')
```

Migrate fails if any table partition uses an unsupported file format.

In addition, you can provide a `recursive_directory` argument to migrate a
Hive table that contains subdirectories:

```
CALL example.system.migrate(
    schema_name => 'testdb',
    table_name => 'customer_orders',
    recursive_directory => 'true')
```

The default value is `fail`, which causes the migrate procedure to throw an
exception if subdirectories are found. Set the value to `true` to migrate
nested directories, or `false` to ignore them.

(iceberg-add-files)=
#### Add files

The connector can add files from tables or locations to an existing Iceberg
table if `iceberg.add-files-procedure.enabled` is set to `true` for the catalog.

Use the procedure `add_files_from_table` to add existing files from a Hive table
in the current catalog, or `add_files` to add existing files from a specified
location, to an existing Iceberg table.
 
The data files must be the Parquet, ORC, or Avro file format.

The procedure adds the files to the target table, specified after `ALTER TABLE`,
and loads them from the source table specified with the required parameters
`schema_name` and `table_name`. The source table must be accessible in the same
catalog as the target table and use the Hive format. The target table must use
the Iceberg format. The catalog must use the Iceberg connector.

The following examples copy data from the Hive table `hive_customer_orders` in
the `legacy` schema of the `example` catalog into the Iceberg table
`iceberg_customer_orders` in the `lakehouse` schema of the `example` catalog:

```sql
ALTER TABLE example.lakehouse.iceberg_customer_orders 
EXECUTE add_files_from_table(
    schema_name => 'legacy',
    table_name => 'customer_orders')
```

Alternatively, you can set the current catalog and schema with a `USE`
statement, and omit catalog and schema information:

```sql
USE example.lakehouse;
ALTER TABLE iceberg_customer_orders 
EXECUTE add_files_from_table(
    schema_name => 'legacy',
    table_name => 'customer_orders')
```

Use a `partition_filter` argument to add files from specified partitions. The
following example adds files from a partition where the `region` is `ASIA` and
`country` is `JAPAN`:

```sql
ALTER TABLE example.lakehouse.iceberg_customer_orders 
EXECUTE add_files_from_table(
    schema_name => 'legacy',
    table_name => 'customer_orders',
    partition_filter => map(ARRAY['region', 'country'], ARRAY['ASIA', 'JAPAN']))
```

In addition, you can provide a `recursive_directory` argument to migrate a
Hive table that contains subdirectories:

```sql
ALTER TABLE example.lakehouse.iceberg_customer_orders 
EXECUTE add_files_from_table(
    schema_name => 'legacy',
    table_name => 'customer_orders',
    recursive_directory => 'true')
```

The default value of `recursive_directory` is `fail`, which causes the procedure
to throw an exception if subdirectories are found. Set the value to `true` to
add files from nested directories, or `false` to ignore them.

The `add_files` procedure supports adding files, and therefore the contained
data, to a target table, specified after `ALTER TABLE`. It loads the files from
a object storage path specified with the required `location` parameter. The
files must use the specified `format`, with `ORC` and `PARQUET` as valid values.
The target Iceberg table must use the same format as the added files. The
procedure does not validate file schemas for compatibility with the target
Iceberg table. The `location` property is supported for partitioned tables.

The following examples copy `ORC`-format files from the location
`s3://my-bucket/a/path` into the Iceberg table `iceberg_customer_orders` in the
`lakehouse` schema of the `example` catalog:

```sql
ALTER TABLE example.lakehouse.iceberg_customer_orders 
EXECUTE add_files(
    location => 's3://my-bucket/a/path',
    format => 'ORC')
```

(iceberg-data-management)=
### Data management

The {ref}`sql-data-management` functionality includes support for `INSERT`,
`UPDATE`, `DELETE`, and `MERGE` statements.

(iceberg-delete)=
#### Deletion by partition

For partitioned tables, the Iceberg connector supports the deletion of entire
partitions if the `WHERE` clause specifies filters only on the
identity-transformed partitioning columns, that can match entire partitions.
Given the table definition from {ref}`Partitioned Tables <iceberg-tables>`
section, the following SQL statement deletes all partitions for which
`country` is `US`:

```
DELETE FROM example.testdb.customer_orders
WHERE country = 'US'
```

A partition delete is performed if the `WHERE` clause meets these conditions.

#### Row level deletion

Tables using v2 of the Iceberg specification support deletion of individual rows
by writing position delete files.

(iceberg-schema-table-management)=
### Schema and table management

The {ref}`sql-schema-table-management` functionality includes support for:

- {doc}`/sql/create-schema`
- {doc}`/sql/drop-schema`
- {doc}`/sql/alter-schema`
- {doc}`/sql/create-table`
- {doc}`/sql/create-table-as`
- {doc}`/sql/drop-table`
- {doc}`/sql/alter-table`
- {doc}`/sql/comment`

#### Schema evolution

Iceberg supports schema evolution, with safe column add, drop, reorder, and
rename operations, including in nested structures.

Iceberg supports updating column types only for widening operations:

- `INTEGER` to `BIGINT`
- `REAL` to `DOUBLE`
- `DECIMAL(p,s)` to `DECIMAL(p2,s)` when `p2` > `p` (scale cannot change)

Partitioning can also be changed and the connector can still query data
created before the partitioning change.

(iceberg-alter-table-execute)=
#### ALTER TABLE EXECUTE

The connector supports the following commands for use with {ref}`ALTER TABLE
EXECUTE <alter-table-execute>`.

```{include} optimize.fragment
```

(iceberg-expire-snapshots)=
##### expire_snapshots

The `expire_snapshots` command removes all snapshots and all related metadata
and data files. Regularly expiring snapshots is recommended to delete data files
that are no longer needed, and to keep the size of table metadata small. The
procedure affects all snapshots that are older than the time period configured
with the `retention_threshold` parameter.

`expire_snapshots` can be run as follows:

```sql
ALTER TABLE test_table EXECUTE expire_snapshots(retention_threshold => '7d')
```

The value for `retention_threshold` must be higher than or equal to
`iceberg.expire-snapshots.min-retention` in the catalog, otherwise the
procedure fails with a similar message: `Retention specified (1.00d) is shorter
than the minimum retention configured in the system (7.00d)`. The default value
for this property is `7d`.

(iceberg-remove-orphan-files)=
##### remove_orphan_files

The `remove_orphan_files` command removes all files from a table's data
directory that are not linked from metadata files and that are older than the
value of `retention_threshold` parameter. Deleting orphan files from time to
time is recommended to keep size of a table's data directory under control.

`remove_orphan_files` can be run as follows:

```sql
ALTER TABLE test_table EXECUTE remove_orphan_files(retention_threshold => '7d')
```

The value for `retention_threshold` must be higher than or equal to
`iceberg.remove-orphan-files.min-retention` in the catalog otherwise the
procedure fails with a similar message: `Retention specified (1.00d) is shorter
than the minimum retention configured in the system (7.00d)`. The default value
for this property is `7d`.

(drop-extended-stats)=
##### drop_extended_stats

The `drop_extended_stats` command removes all extended statistics information
from the table.

`drop_extended_stats` can be run as follows:

```sql
ALTER TABLE test_table EXECUTE drop_extended_stats
```

(iceberg-alter-table-set-properties)=
#### ALTER TABLE SET PROPERTIES

The connector supports modifying the properties on existing tables using
{ref}`ALTER TABLE SET PROPERTIES <alter-table-set-properties>`.

The following table properties can be updated after a table is created:

- `format`
- `format_version`
- `partitioning`
- `sorted_by`
- `object_store_layout_enabled`
- `data_location`

For example, to update a table from v1 of the Iceberg specification to v2:

```sql
ALTER TABLE table_name SET PROPERTIES format_version = 2;
```

Or to set the column `my_new_partition_column` as a partition column on a
table:

```sql
ALTER TABLE table_name SET PROPERTIES partitioning = ARRAY[<existing partition columns>, 'my_new_partition_column'];
```

The current values of a table's properties can be shown using {doc}`SHOW CREATE
TABLE </sql/show-create-table>`.

(iceberg-table-properties)=
##### Table properties

Table properties supply or set metadata for the underlying tables. This is key
for {doc}`/sql/create-table-as` statements. Table properties are passed to the
connector using a {doc}`WITH </sql/create-table-as>` clause.


:::{list-table} Iceberg table properties
:widths: 40, 60
:header-rows: 1

* - Property name
  - Description
* - `format`
  - Optionally specifies the format of table data files; either `PARQUET`,
    `ORC`, or `AVRO`. Defaults to the value of the `iceberg.file-format` catalog
    configuration property, which defaults to `PARQUET`.
* - `partitioning`
  - Optionally specifies table partitioning. If a table is partitioned by
    columns `c1` and `c2`, the partitioning property is `partitioning =
    ARRAY['c1', 'c2']`.
* - `sorted_by`
  - The sort order to be applied during writes to the content of
    each file written to the table. If the table files are sorted by columns
    `c1` and `c2`, the sort order property is `sorted_by = ARRAY['c1', 'c2']`.
    The sort order applies to the contents written within each output file
    independently and not the entire dataset.
* - `location`
  - Optionally specifies the file system location URI for the table.
* - `format_version`
  - Optionally specifies the format version of the Iceberg specification to use
    for new tables; either `1` or `2`. Defaults to `2`. Version `2` is required
    for row level deletes.
* - `orc_bloom_filter_columns`
  - Comma-separated list of columns to use for ORC bloom filter. It improves the
    performance of queries using Equality and IN predicates when reading ORC
    files. Requires ORC format. Defaults to `[]`.
* - `orc_bloom_filter_fpp`
  - The ORC bloom filters false positive probability. Requires ORC format.
    Defaults to `0.05`.
* - `parquet_bloom_filter_columns`
  - Comma-separated list of columns to use for Parquet bloom filter. It improves
    the performance of queries using Equality and IN predicates when reading
    Parquet files. Requires Parquet format. Defaults to `[]`.
* - `object_store_layout_enabled`
  - Whether Iceberg's [object store file layout](https://iceberg.apache.org/docs/latest/aws/#object-store-file-layout) is enabled. 
* - `data_location`
  - Optionally specifies the file system location URI for the table's data files
* - `extra_properties`
  - Additional properties added to a Iceberg table. The properties are not used by Trino,
    and are available in the `$properties` metadata table.
    The properties are not included in the output of `SHOW CREATE TABLE` statements.
:::

The table definition below specifies to use Parquet files, partitioning by columns
`c1` and `c2`, and a file system location of
`/var/example_tables/test_table`:

```
CREATE TABLE test_table (
    c1 INTEGER,
    c2 DATE,
    c3 DOUBLE)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['c1', 'c2'],
    location = '/var/example_tables/test_table')
```

The table definition below specifies to use ORC files, bloom filter index by columns
`c1` and `c2`, fpp is 0.05, and a file system location of
`/var/example_tables/test_table`:

```
CREATE TABLE test_table (
    c1 INTEGER,
    c2 DATE,
    c3 DOUBLE)
WITH (
    format = 'ORC',
    location = '/var/example_tables/test_table',
    orc_bloom_filter_columns = ARRAY['c1', 'c2'],
    orc_bloom_filter_fpp = 0.05)
```

The table definition below specifies to use Avro files, partitioning
by `child1` field in `parent` column:

```
CREATE TABLE test_table (
    data INTEGER,
    parent ROW(child1 DOUBLE, child2 INTEGER))
WITH (
    format = 'AVRO',
    partitioning = ARRAY['"parent.child1"'])
```

(iceberg-metadata-tables)=
#### Metadata tables

The connector exposes several metadata tables for each Iceberg table. These
metadata tables contain information about the internal structure of the Iceberg
table. You can query each metadata table by appending the metadata table name to
the table name:

```
SELECT * FROM "test_table$properties"
```

##### `$properties` table

The `$properties` table provides access to general information about Iceberg
table configuration and any additional metadata key/value pairs that the table
is tagged with.

You can retrieve the properties of the current snapshot of the Iceberg table
`test_table` by using the following query:

```
SELECT * FROM "test_table$properties"
```

```text
 key                   | value    |
-----------------------+----------+
write.format.default   | PARQUET  |
```

(iceberg-history-table)=
##### `$history` table

The `$history` table provides a log of the metadata changes performed on the
Iceberg table.

You can retrieve the changelog of the Iceberg table `test_table` by using the
following query:

```
SELECT * FROM "test_table$history"
```

```text
 made_current_at                  | snapshot_id          | parent_id            | is_current_ancestor
----------------------------------+----------------------+----------------------+--------------------
2022-01-10 08:11:20 Europe/Vienna | 8667764846443717831  |  <null>              |  true
2022-01-10 08:11:34 Europe/Vienna | 7860805980949777961  | 8667764846443717831  |  true
```

The output of the query has the following columns:

:::{list-table} History columns
:widths: 30, 30, 40
:header-rows: 1

* - Name
  - Type
  - Description
* - `made_current_at`
  - `TIMESTAMP(3) WITH TIME ZONE`
  - The time when the snapshot became active.
* - `snapshot_id`
  - `BIGINT`
  - The identifier of the snapshot.
* - `parent_id`
  - `BIGINT`
  - The identifier of the parent snapshot.
* - `is_current_ancestor`
  - `BOOLEAN`
  - Whether or not this snapshot is an ancestor of the current snapshot.
:::

##### `$metadata_log_entries` table

The `$metadata_log_entries` table provides a view of metadata log entries
of the Iceberg table.

You can retrieve the information about the metadata log entries of the Iceberg
table `test_table` by using the following query:

```
SELECT * FROM "test_table$metadata_log_entries"
```

```text
             timestamp                 |                                                              file                                                          | latest_snapshot_id  | latest_schema_id | latest_sequence_number
---------------------------------------+----------------------------------------------------------------------------------------------------------------------------+---------------------+------------------+------------------------
 2024-01-16 15:55:31.172 Europe/Vienna | hdfs://hadoop-master:9000/user/hive/warehouse/test_table/metadata/00000-39174715-be2a-48fa-9949-35413b8b736e.metadata.json | 1221802298419195590 |                0 |                      1
 2024-01-16 17:19:56.118 Europe/Vienna | hdfs://hadoop-master:9000/user/hive/warehouse/test_table/metadata/00001-e40178c9-271f-4a96-ad29-eed5e7aef9b0.metadata.json | 7124386610209126943 |                0 |                      2
```

The output of the query has the following columns:

:::{list-table} Metadata log entries columns
:widths: 30, 30, 40
:header-rows: 1

* - Name
  - Type
  - Description
* - `timestamp`
  - `TIMESTAMP(3) WITH TIME ZONE`
  - The time when the metadata was created.
* - `file`
  - `VARCHAR`
  - The location of the metadata file.
* - `latest_snapshot_id`
  - `BIGINT`
  - The identifier of the latest snapshot when the metadata was updated.
* - `latest_schema_id`
  - `INTEGER`
  - The identifier of the latest schema when the metadata was updated.
* - `latest_sequence_number`
  - `BIGINT`
  - The data sequence number of the metadata file.
:::


##### `$snapshots` table

The `$snapshots` table provides a detailed view of snapshots of the Iceberg
table. A snapshot consists of one or more file manifests, and the complete table
contents are represented by the union of all the data files in those manifests.

You can retrieve the information about the snapshots of the Iceberg table
`test_table` by using the following query:

```
SELECT * FROM "test_table$snapshots"
```

```text
 committed_at                      | snapshot_id          | parent_id            | operation          |  manifest_list                                                                                                                           |   summary
----------------------------------+----------------------+----------------------+--------------------+------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
2022-01-10 08:11:20 Europe/Vienna | 8667764846443717831  |  <null>              |  append            |   hdfs://hadoop-master:9000/user/hive/warehouse/test_table/metadata/snap-8667764846443717831-1-100cf97e-6d56-446e-8961-afdaded63bc4.avro | {changed-partition-count=0, total-equality-deletes=0, total-position-deletes=0, total-delete-files=0, total-files-size=0, total-records=0, total-data-files=0}
2022-01-10 08:11:34 Europe/Vienna | 7860805980949777961  | 8667764846443717831  |  append            |   hdfs://hadoop-master:9000/user/hive/warehouse/test_table/metadata/snap-7860805980949777961-1-faa19903-1455-4bb8-855a-61a1bbafbaa7.avro | {changed-partition-count=1, added-data-files=1, total-equality-deletes=0, added-records=1, total-position-deletes=0, added-files-size=442, total-delete-files=0, total-files-size=442, total-records=1, total-data-files=1}
```

The output of the query has the following columns:

:::{list-table} Snapshots columns
:widths: 20, 30, 50
:header-rows: 1

* - Name
  - Type
  - Description
* - `committed_at`
  - `TIMESTAMP(3) WITH TIME ZONE`
  - The time when the snapshot became active.
* - `snapshot_id`
  - `BIGINT`
  - The identifier for the snapshot.
* - `parent_id`
  - `BIGINT`
  - The identifier for the parent snapshot.
* - `operation`
  - `VARCHAR`
  - The type of operation performed on the Iceberg table. The supported
    operation types in Iceberg are:

    * `append` when new data is appended.
    * `replace` when files are removed and replaced without changing the
      data in the table.
    * `overwrite` when new data is added to overwrite existing data.
    * `delete` when data is deleted from the table and no new data is added.
* - `manifest_list`
  - `VARCHAR`
  - The list of Avro manifest files containing the detailed information about
    the snapshot changes.
* - `summary`
  - `map(VARCHAR, VARCHAR)`
  - A summary of the changes made from the previous snapshot to the current
    snapshot.
:::

##### `$manifests` table

The `$manifests` table provides a detailed overview of the manifests
corresponding to the snapshots performed in the log of the Iceberg table.

You can retrieve the information about the manifests of the Iceberg table
`test_table` by using the following query:

```
SELECT * FROM "test_table$manifests"
```

```text
 path                                                                                                           | length          | partition_spec_id    | added_snapshot_id     | added_data_files_count  | added_rows_count | existing_data_files_count   | existing_rows_count | deleted_data_files_count    | deleted_rows_count | partitions
----------------------------------------------------------------------------------------------------------------+-----------------+----------------------+-----------------------+-------------------------+------------------+-----------------------------+---------------------+-----------------------------+--------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------
 hdfs://hadoop-master:9000/user/hive/warehouse/test_table/metadata/faa19903-1455-4bb8-855a-61a1bbafbaa7-m0.avro |  6277           |   0                  | 7860805980949777961   | 1                       | 100              | 0                           | 0                   | 0                           | 0                  | {{contains_null=false, contains_nan= false, lower_bound=1, upper_bound=1},{contains_null=false, contains_nan= false, lower_bound=2021-01-12, upper_bound=2021-01-12}}
```

The output of the query has the following columns:

:::{list-table} Manifests columns
:widths: 30, 30, 40
:header-rows: 1

* - Name
  - Type
  - Description
* - `path`
  - `VARCHAR`
  - The manifest file location.
* - `length`
  - `BIGINT`
  - The manifest file length.
* - `partition_spec_id`
  - `INTEGER`
  - The identifier for the partition specification used to write the manifest
    file.
* - `added_snapshot_id`
  - `BIGINT`
  - The identifier of the snapshot during which this manifest entry has been
    added.
* - `added_data_files_count`
  - `INTEGER`
  - The number of data files with status `ADDED` in the manifest file.
* - `added_rows_count`
  - `BIGINT`
  - The total number of rows in all data files with status `ADDED` in the
    manifest file.
* - `existing_data_files_count`
  - `INTEGER`
  - The number of data files with status `EXISTING` in the manifest file.
* - `existing_rows_count`
  - `BIGINT`
  - The total number of rows in all data files with status `EXISTING` in the
    manifest file.
* - `deleted_data_files_count`
  - `INTEGER`
  - The number of data files with status `DELETED` in the manifest file.
* - `deleted_rows_count`
  - `BIGINT`
  - The total number of rows in all data files with status `DELETED` in the
    manifest file.
* - `partitions`
  - `ARRAY(row(contains_null BOOLEAN, contains_nan BOOLEAN, lower_bound VARCHAR, upper_bound VARCHAR))`
  - Partition range metadata.
:::

##### `$partitions` table

The `$partitions` table provides a detailed overview of the partitions of the
Iceberg table.

You can retrieve the information about the partitions of the Iceberg table
`test_table` by using the following query:

```
SELECT * FROM "test_table$partitions"
```

```text
 partition             | record_count  | file_count    | total_size    |  data
-----------------------+---------------+---------------+---------------+------------------------------------------------------
{c1=1, c2=2021-01-12}  |  2            | 2             |  884          | {c3={min=1.0, max=2.0, null_count=0, nan_count=NULL}}
{c1=1, c2=2021-01-13}  |  1            | 1             |  442          | {c3={min=1.0, max=1.0, null_count=0, nan_count=NULL}}
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
* - `record_count`
  - `BIGINT`
  - The number of records in the partition.
* - `file_count`
  - `BIGINT`
  - The number of files mapped in the partition.
* - `total_size`
  - `BIGINT`
  - The size of all the files in the partition.
* - `data`
  - `ROW(... ROW (min ..., max ... , null_count BIGINT, nan_count BIGINT))`
  - Partition range metadata.
:::

##### `$files` table

The `$files` table provides a detailed overview of the data files in current
snapshot of the  Iceberg table.

To retrieve the information about the data files of the Iceberg table
`test_table`, use the following query:

```
SELECT * FROM "test_table$files"
```

```text
 content  | file_path                                                                                                                     | record_count    | file_format   | file_size_in_bytes   |  column_sizes        |  value_counts     |  null_value_counts | nan_value_counts  | lower_bounds                |  upper_bounds               |  key_metadata  | split_offsets  |  equality_ids
----------+-------------------------------------------------------------------------------------------------------------------------------+-----------------+---------------+----------------------+----------------------+-------------------+--------------------+-------------------+-----------------------------+-----------------------------+----------------+----------------+---------------
 0        | hdfs://hadoop-master:9000/user/hive/warehouse/test_table/data/c1=3/c2=2021-01-14/af9872b2-40f3-428f-9c87-186d2750d84e.parquet |  1              |  PARQUET      |  442                 | {1=40, 2=40, 3=44}   |  {1=1, 2=1, 3=1}  |  {1=0, 2=0, 3=0}   | <null>            |  {1=3, 2=2021-01-14, 3=1.3} |  {1=3, 2=2021-01-14, 3=1.3} |  <null>        | <null>         |   <null>
```

The output of the query has the following columns:

:::{list-table} Files columns
:widths: 25, 30, 45
:header-rows: 1

* - Name
  - Type
  - Description
* - `content`
  - `INTEGER`
  - Type of content stored in the file. The supported content types in Iceberg
    are:

    * `DATA(0)`
    * `POSITION_DELETES(1)`
    * `EQUALITY_DELETES(2)`
* - `file_path`
  - `VARCHAR`
  - The data file location.
* - `file_format`
  - `VARCHAR`
  - The format of the data file.
* - `spec_id`
  - `INTEGER`
  - Spec ID used to track the file containing a row.
* - `partition`
  - `ROW(...)`
  - A row that contains the mapping of the partition column names to the
    partition column values.
* - `record_count`
  - `BIGINT`
  - The number of entries contained in the data file.
* - `file_size_in_bytes`
  - `BIGINT`
  - The data file size
* - `column_sizes`
  - `map(INTEGER, BIGINT)`
  - Mapping between the Iceberg column ID and its corresponding size in the
    file.
* - `value_counts`
  - `map(INTEGER, BIGINT)`
  - Mapping between the Iceberg column ID and its corresponding count of entries
    in the file.
* - `null_value_counts`
  - `map(INTEGER, BIGINT)`
  - Mapping between the Iceberg column ID and its corresponding count of `NULL`
    values in the file.
* - `nan_value_counts`
  - `map(INTEGER, BIGINT)`
  - Mapping between the Iceberg column ID and its corresponding count of non-
    numerical values in the file.
* - `lower_bounds`
  - `map(INTEGER, BIGINT)`
  - Mapping between the Iceberg column ID and its corresponding lower bound in
    the file.
* - `upper_bounds`
  - `map(INTEGER, BIGINT)`
  - Mapping between the Iceberg column ID and its corresponding upper bound in
    the file.
* - `key_metadata`
  - `VARBINARY`
  - Metadata about the encryption key used to encrypt this file, if applicable.
* - `split_offsets`
  - `array(BIGINT)`
  - List of recommended split locations.
* - `equality_ids`
  - `array(INTEGER)`
  - The set of field IDs used for equality comparison in equality delete files.
* - `sort_order_id`
  - `INTEGER`
  - ID representing sort order for this file.
* - `readable_metrics`
  - `JSON`
  - File metrics in human-readable form.
:::

##### `$refs` table

The `$refs` table provides information about Iceberg references including
branches and tags.

You can retrieve the references of the Iceberg table `test_table` by using the
following query:

```
SELECT * FROM "test_table$refs"
```

```text
name            | type   | snapshot_id | max_reference_age_in_ms | min_snapshots_to_keep | max_snapshot_age_in_ms |
----------------+--------+-------------+-------------------------+-----------------------+------------------------+
example_tag     | TAG    | 10000000000 | 10000                   | null                  | null                   |
example_branch  | BRANCH | 20000000000 | 20000                   | 2                     | 30000                  |
```

The output of the query has the following columns:

:::{list-table} Refs columns
:widths: 20, 30, 50
:header-rows: 1

* - Name
  - Type
  - Description
* - `name`
  - `VARCHAR`
  - Name of the reference.
* - `type`
  - `VARCHAR`
  - Type of the reference, either `BRANCH` or `TAG`.
* - `snapshot_id`
  - `BIGINT`
  - The snapshot ID of the reference.
* - `max_reference_age_in_ms`
  - `BIGINT`
  - The maximum age of the reference before it could be expired.
* - `min_snapshots_to_keep`
  - `INTEGER`
  - For branch only, the minimum number of snapshots to keep in a branch.
* - `max_snapshot_age_in_ms`
  - `BIGINT`
  - For branch only, the max snapshot age allowed in a branch. Older snapshots
    in the branch will be expired.
:::

(iceberg-metadata-columns)=
#### Metadata columns

In addition to the defined columns, the Iceberg connector automatically exposes
path metadata as a hidden column in each table:

- `$path`: Full file system path name of the file for this row
- `$file_modified_time`: Timestamp of the last modification of the file for
  this row

You can use these columns in your SQL statements like any other column. This can
be selected directly, or used in conditional statements. For example, you can
inspect the file path for each record:

```
SELECT *, "$path", "$file_modified_time"
FROM example.web.page_views;
```

Retrieve all records that belong to a specific file using `"$path"` filter:

```
SELECT *
FROM example.web.page_views
WHERE "$path" = '/usr/iceberg/table/web.page_views/data/file_01.parquet'
```

Retrieve all records that belong to a specific file using
`"$file_modified_time"` filter:

```
SELECT *
FROM example.web.page_views
WHERE "$file_modified_time" = CAST('2022-07-01 01:02:03.456 UTC' AS TIMESTAMP WITH TIME ZONE)
```

#### DROP TABLE

The Iceberg connector supports dropping a table by using the
{doc}`/sql/drop-table` syntax. When the command succeeds, both the data of the
Iceberg table and also the information related to the table in the metastore
service are removed. Dropping tables that have their data/metadata stored in a
different location than the table's corresponding base directory on the object
store is not supported.

(iceberg-comment)=
#### COMMENT

The Iceberg connector supports setting comments on the following objects:

- tables
- views
- table columns
- materialized view columns

The `COMMENT` option is supported on both the table and the table columns for
the {doc}`/sql/create-table` operation.

The `COMMENT` option is supported for adding table columns through the
{doc}`/sql/alter-table` operations.

The connector supports the command {doc}`COMMENT </sql/comment>` for setting
comments on existing entities.

(iceberg-tables)=
#### Partitioned tables

Iceberg supports partitioning by specifying transforms over the table columns. A
partition is created for each unique tuple value produced by the transforms.
Identity transforms are simply the column name. Other transforms are:

:::{list-table} Iceberg column transforms
:widths: 40, 60
:header-rows: 1

* - Transform
  - Description
* - `year(ts)`
  - A partition is created for each year. The partition value is the integer
    difference in years between `ts` and January 1 1970.
* - `month(ts)`
  - A partition is created for each month of each year. The partition value is
    the integer difference in months between `ts` and January 1 1970.
* - `day(ts)`
  - A partition is created for each day of each year. The partition value is the
    integer difference in days between `ts` and January 1 1970.
* - `hour(ts)`
  - A partition is created hour of each day. The partition value is a timestamp
    with the minutes and seconds set to zero.
* - `bucket(x, nbuckets)`
  - The data is hashed into the specified number of buckets. The partition value
    is an integer hash of `x`, with a value between 0 and `nbuckets - 1`
    inclusive.
* - `truncate(s, nchars)`
  - The partition value is the first `nchars` characters of `s`.
:::

In this example, the table is partitioned by the month of `order_date`, a hash
of `account_number` (with 10 buckets), and `country`:

```
CREATE TABLE example.testdb.customer_orders (
    order_id BIGINT,
    order_date DATE,
    account_number BIGINT,
    customer VARCHAR,
    country VARCHAR)
WITH (partitioning = ARRAY['month(order_date)', 'bucket(account_number, 10)', 'country'])
```

(iceberg-sorted-files)=
#### Sorted tables

The connector supports sorted files as a performance improvement. Data is sorted
during writes within each file based on the specified array of one or more
columns.

Sorting is particularly beneficial when the sorted columns show a high
cardinality and are used as a filter for selective reads.

The sort order is configured with the `sorted_by` table property. Specify an
array of one or more columns to use for sorting when creating the table. The
following example configures the `order_date` column of the `orders` table
in the `customers` schema in the `example` catalog:

```
CREATE TABLE example.customers.orders (
    order_id BIGINT,
    order_date DATE,
    account_number BIGINT,
    customer VARCHAR,
    country VARCHAR)
WITH (sorted_by = ARRAY['order_date'])
```

Sorting can be combined with partitioning on the same column. For example:

```
CREATE TABLE example.customers.orders (
    order_id BIGINT,
    order_date DATE,
    account_number BIGINT,
    customer VARCHAR,
    country VARCHAR)
WITH (
    partitioning = ARRAY['month(order_date)'],
    sorted_by = ARRAY['order_date']
)
```

You can disable sorted writing with the session property
`sorted_writing_enabled` set to `false`.

#### Using snapshots

Iceberg supports a snapshot model of data, where table snapshots are
identified by a snapshot ID.

The connector provides a system table exposing snapshot information for every
Iceberg table. Snapshots are identified by `BIGINT` snapshot IDs. For example,
you can find the snapshot IDs for the `customer_orders` table by running the
following query:

```
SELECT snapshot_id
FROM example.testdb."customer_orders$snapshots"
ORDER BY committed_at DESC
```

(iceberg-create-or-replace)=
#### Replace tables

The connector supports replacing an existing table, as an atomic operation.
Atomic table replacement creates a new snapshot with the new table definition as
part of the [table history](#iceberg-history-table).

To replace a table, use [`CREATE OR REPLACE TABLE`](/sql/create-table) or
[`CREATE OR REPLACE TABLE AS`](/sql/create-table-as).

Earlier snapshots of the table can be queried through [](iceberg-time-travel).

In the following example, a table `example_table` can be replaced by a
completely new definition and data from the source table:

```sql
CREATE OR REPLACE TABLE example_table
WITH (sorted_by = ARRAY['a'])
AS SELECT * FROM another_table;
```

(iceberg-time-travel)=
##### Time travel queries

The connector offers the ability to query historical data. This allows you to
query the table as it was when a previous snapshot of the table was taken, even
if the data has since been modified or deleted.

The historical data of the table can be retrieved by specifying the snapshot
identifier corresponding to the version of the table to be retrieved:

```
SELECT *
FROM example.testdb.customer_orders FOR VERSION AS OF 8954597067493422955
```

A different approach of retrieving historical data is to specify a point in time
in the past, such as a day or week ago. The latest snapshot of the table taken
before or at the specified timestamp in the query is internally used for
providing the previous state of the table:

```
SELECT *
FROM example.testdb.customer_orders FOR TIMESTAMP AS OF TIMESTAMP '2022-03-23 09:59:29.803 Europe/Vienna'
```

The connector allows to create a new snapshot through Iceberg's [replace table](iceberg-create-or-replace).

```
CREATE OR REPLACE TABLE example.testdb.customer_orders AS
SELECT *
FROM example.testdb.customer_orders FOR TIMESTAMP AS OF TIMESTAMP '2022-03-23 09:59:29.803 Europe/Vienna'
```

You can use a date to specify a point a time in the past for using a snapshot of a table in a query.
Assuming that the session time zone is `Europe/Vienna` the following queries are equivalent:

```
SELECT *
FROM example.testdb.customer_orders FOR TIMESTAMP AS OF DATE '2022-03-23'
```

```
SELECT *
FROM example.testdb.customer_orders FOR TIMESTAMP AS OF TIMESTAMP '2022-03-23 00:00:00'
```

```
SELECT *
FROM example.testdb.customer_orders FOR TIMESTAMP AS OF TIMESTAMP '2022-03-23 00:00:00.000 Europe/Vienna'
```

Iceberg supports named references of snapshots via branches and tags.
Time travel can be performed to branches and tags in the table.

```
SELECT *
FROM example.testdb.customer_orders FOR VERSION AS OF 'historical-tag'

SELECT *
FROM example.testdb.customer_orders FOR VERSION AS OF 'test-branch'
```

##### Rolling back to a previous snapshot

Use the `$snapshots` metadata table to determine the latest snapshot ID of the
table like in the following query:

```
SELECT snapshot_id
FROM example.testdb."customer_orders$snapshots"
ORDER BY committed_at DESC LIMIT 1
```

The procedure `system.rollback_to_snapshot` allows the caller to roll back the
state of the table to a previous snapshot id:

```
CALL example.system.rollback_to_snapshot('testdb', 'customer_orders', 8954597067493422955)
```

#### `NOT NULL` column constraint

The Iceberg connector supports setting `NOT NULL` constraints on the table
columns.

The `NOT NULL` constraint can be set on the columns, while creating tables by
using the {doc}`CREATE TABLE </sql/create-table>` syntax:

```
CREATE TABLE example_table (
    year INTEGER NOT NULL,
    name VARCHAR NOT NULL,
    age INTEGER,
    address VARCHAR
);
```

When trying to insert/update data in the table, the query fails if trying to set
`NULL` value on a column having the `NOT NULL` constraint.

(iceberg-materialized-views)=
### Materialized views

The Iceberg connector supports {ref}`sql-materialized-view-management`. In the
underlying system, each materialized view consists of a view definition and an
Iceberg storage table. The storage table name is stored as a materialized view
property. The data is stored in that storage table.

You can use the {ref}`iceberg-table-properties` to control the created storage
table and therefore the layout and performance. For example, you can use the
following clause with {doc}`/sql/create-materialized-view` to use the ORC format
for the data files and partition the storage per day using the column
`event_date`:

```
WITH ( format = 'ORC', partitioning = ARRAY['event_date'] )
```

By default, the storage table is created in the same schema as the materialized
view definition. The `iceberg.materialized-views.storage-schema` catalog
configuration property or `storage_schema` materialized view property can be
used to specify the schema where the storage table is created.

Creating a materialized view does not automatically populate it with data. You
must run {doc}`/sql/refresh-materialized-view` to populate data in the
materialized view.

Updating the data in the materialized view can be achieved using the `REFRESH
MATERIALIZED VIEW` command. This operation may perform either an incremental or
a full refresh, depending on the complexity of the materialized view definition
and the snapshot history of the source tables. For a full refresh, the operation
deletes the data from the storage table, and inserts the data that is the result
of executing the materialized view query into the existing table. For
incremental refresh, the existing data is not deleted from the storage table and
only the delta records are processed from the source tables and appended into
the storage table as needed. In both cases, data is replaced or appended
atomically, so users can continue to query the materialized view while it is
being refreshed. Refreshing a materialized view also stores the snapshot-ids of
all Iceberg tables that are part of the materialized view's query in the
materialized view metadata. When the materialized view is queried, the
snapshot-ids are used to check if the data in the storage table is up to date.

Materialized views that use non-Iceberg tables in the query show the [default
behavior around grace periods](mv-grace-period). If all tables are Iceberg
tables, the connector can determine if the data has not changed and continue to
use the data from the storage tables, even after the grace period expired.

Dropping a materialized view with {doc}`/sql/drop-materialized-view` removes
the definition and the storage table.

### Table functions

The connector supports the table functions described in the following sections.

#### table_changes

Allows reading row-level changes between two versions of an Iceberg table.
The following query shows an example of displaying the changes of the `t1`
table in the `default` schema in the current catalog.
All changes between the start and end snapshots are returned.

```sql
SELECT
  *
FROM
  TABLE(
    system.table_changes(
      schema_name => 'default',
      table_name => 't1',
      start_snapshot_id => 6541165659943306573,
      end_snapshot_id => 6745790645714043599
    )
  );
```

The function takes the following required parameters:

- `schema_name`
  : Name of the schema for which the function is called.
- `table_name`
  : Name of the table for which the function is called.
- `start_snapshot_id`
  : The identifier of the exclusive starting snapshot.
- `end_snapshot_id`
  : The identifier of the inclusive end snapshot.

Use the `$snapshots` metadata table to determine the snapshot IDs of the
table.

The function returns the columns present in the table, and the following values
for each change:

- `_change_type`
  : The type of change that occurred. Possible values are `insert` and `delete`.
- `_change_version_id`
  : The identifier of the snapshot in which the change occurred.
- `_change_timestamp`
  : Timestamp when the snapshot became active.
- `_change_ordinal`
  : Order number of the change, useful for sorting the results.

**Example:**

Create a table:

```
CREATE TABLE test_schema.pages (page_url VARCHAR, domain VARCHAR, views INTEGER);
```

Insert some data:

```
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

Retrieve the snapshot identifiers of the changes performed on the table:

```
SELECT
    snapshot_id,
    parent_id,
    operation
FROM test_schema."pages$snapshots";
```

```text
     snapshot_id     |      parent_id      | operation
---------------------+---------------------+-----------
 2009020668682716382 |                NULL | append
 2135434251890923160 | 2009020668682716382 | append
 3108755571950643966 | 2135434251890923160 | append
(3 rows)

```

Select the changes performed in the previously-mentioned `INSERT` statements:

```
SELECT
    *
FROM
    TABLE(
            system.table_changes(
                    schema_name => 'test_schema',
                    table_name => 'pages',
                    start_snapshot_id => 2009020668682716382,
                    end_snapshot_id => 3108755571950643966
            )
    )
ORDER BY _change_ordinal ASC;
```

```text
 page_url | domain  | views | _change_type | _change_version_id  |      _change_timestamp      | _change_ordinal
----------+---------+-------+--------------+---------------------+-----------------------------+-----------------
 url1     | domain1 |     1 | insert       | 2135434251890923160 | 2024-04-04 21:24:26.105 UTC |               0
 url2     | domain2 |     2 | insert       | 2135434251890923160 | 2024-04-04 21:24:26.105 UTC |               0
 url3     | domain1 |     3 | insert       | 2135434251890923160 | 2024-04-04 21:24:26.105 UTC |               0
 url4     | domain1 |   400 | insert       | 3108755571950643966 | 2024-04-04 21:24:28.318 UTC |               1
 url5     | domain2 |   500 | insert       | 3108755571950643966 | 2024-04-04 21:24:28.318 UTC |               1
 url6     | domain3 |     2 | insert       | 3108755571950643966 | 2024-04-04 21:24:28.318 UTC |               1
(6 rows)
```

## Performance

The connector includes a number of performance improvements, detailed in the
following sections.

### Table statistics

The Iceberg connector can collect column statistics using {doc}`/sql/analyze`
statement. This can be disabled using `iceberg.extended-statistics.enabled`
catalog configuration property, or the corresponding
`extended_statistics_enabled` session property.

(iceberg-analyze)=
#### Updating table statistics

If your queries are complex and include joining large data sets, running
{doc}`/sql/analyze` on tables may improve query performance by collecting
statistical information about the data:

```
ANALYZE table_name
```

This query collects statistics for all columns.

On wide tables, collecting statistics for all columns can be expensive. It is
also typically unnecessary - statistics are only useful on specific columns,
like join keys, predicates, or grouping keys. You can specify a subset of
columns to analyzed with the optional `columns` property:

```
ANALYZE table_name WITH (columns = ARRAY['col_1', 'col_2'])
```

This query collects statistics for columns `col_1` and `col_2`.

Note that if statistics were previously collected for all columns, they must be
dropped using the {ref}`drop_extended_stats <drop-extended-stats>` command
before re-analyzing.

(iceberg-table-redirection)=
### Table redirection

```{include} table-redirection.fragment
```

The connector supports redirection from Iceberg tables to Hive tables with the
`iceberg.hive-catalog-name` catalog configuration property.

### File system cache

The connector supports configuring and using [file system
caching](/object-storage/file-system-cache).
