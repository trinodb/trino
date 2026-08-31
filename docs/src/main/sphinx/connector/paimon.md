# Paimon connector

The Paimon connector enables Trino to read and write
[Apache Paimon](https://paimon.apache.org/) tables. It uses the Paimon 2.0
catalog and table APIs together with Trino's native ORC, Parquet, and file
system implementations.

Apache Paimon is a streaming-native lake format that supports high-speed
appends and updates, as well as streaming and batch reads. Paimon supports
primary-key tables with merge engines, schema evolution, partitioning,
bucketing, time travel, and branches/tags for table version management.

## Requirements

To use the connector, you need:

- A Paimon catalog and warehouse reachable from every Trino coordinator and
  worker.
- Network access to the metadata service used by the catalog, when applicable.
- Native file system configuration for the warehouse. The recommended S3
  configuration uses Trino native S3 and does not require Hadoop.

For object storage configuration, see [](/object-storage),
[](/object-storage/file-system-azure), [](/object-storage/file-system-gcs),
[](/object-storage/file-system-s3), and [](/object-storage/file-system-local).

## General configuration

To configure the Paimon connector, create a catalog properties file
`etc/catalog/paimon.properties` that references the `paimon` connector.

You must select and configure one of the [supported file
systems](paimon-file-system-configuration).

### Filesystem catalog on S3

The following example configures a filesystem Paimon catalog on an S3-compatible
object store:

```properties
connector.name=paimon
warehouse=s3://example-bucket/warehouse
metastore=filesystem

fs.native-s3.enabled=true
fs.hadoop.enabled=false
s3.endpoint=https://s3.example.net
s3.access-key=EXAMPLE_ACCESS_KEY
s3.secret-key=EXAMPLE_SECRET_KEY
s3.region=us-east-1
s3.path-style-access=true
```

The connector accepts `s3.access-key` and `s3.secret-key`. Existing deployments
using the compatible `s3.aws-access-key`, `s3.aws-secret-key`, `s3a.*`, or
`fs.s3a.*` names are also accepted and normalized before Paimon creates the
catalog.

### Filesystem catalog on local storage

The following example configures a filesystem Paimon catalog using a local
or shared file system mount point:

```properties
connector.name=paimon
warehouse=file:///opt/paimon/warehouse
metastore=filesystem

fs.local.enabled=true
```

The warehouse path must be accessible from every coordinator and worker node.
Support for local file system is not enabled by default.

### JDBC catalog with concurrent writers

For a JDBC catalog, configure the JDBC connection and a catalog lock. This is
required for safe concurrent `KEY_DYNAMIC` primary-key writes from independent
Trino coordinators.

```properties
connector.name=paimon
warehouse=s3://example-bucket/warehouse
metastore=jdbc
uri=jdbc:postgresql://metadata.example.net:5432/paimon
jdbc.user=paimon
jdbc.password=secret
catalog-key=production-paimon

lock.enabled=true
lock.type=jdbc
lock-acquire-timeout=10m
lock-check-max-sleep=1s

fs.native-s3.enabled=true
fs.hadoop.enabled=false
s3.region=us-east-1
```

The connector validates `KEY_DYNAMIC` global primary keys inside Paimon's atomic
snapshot-commit path. Catalog and lock combinations that cannot provide that
atomic validation fail before the write instead of accepting a potentially stale
key route.

### Hive metastore catalog

For a Hive metastore catalog, configure the Hive metastore URI or Hive
configuration directory. The connector reads and writes data through Trino's
native file system (no Hadoop required), while table metadata is managed by
the Hive metastore.

```properties
connector.name=paimon
warehouse=hdfs://nameservice/warehouse
metastore=hive
uri=thrift://hive-metastore.example.net:9083

fs.native-s3.enabled=true
fs.hadoop.enabled=false
s3.region=us-east-1
```

Alternatively, provide the Hive configuration directory:

```properties
connector.name=paimon
metastore=hive
hive-conf-dir=/etc/hive/conf
```

When using a Hive metastore catalog, set `hive.config.resources` to the
paths of Hadoop XML configuration files (such as `core-site.xml` and
`hdfs-site.xml`) so that the Paimon catalog can resolve HDFS paths:

```properties
hive.config.resources=/etc/hadoop/conf/core-site.xml,/etc/hadoop/conf/hdfs-site.xml
```

(paimon-file-system-configuration)=
## File system access configuration

The connector supports accessing the following file systems:

* [](/object-storage/file-system-azure)
* [](/object-storage/file-system-gcs)
* [](/object-storage/file-system-local)
* [](/object-storage/file-system-s3)
* [](/object-storage/file-system-hdfs)

Enable and configure the file system that your catalog uses. Use
`fs.hadoop.enabled` only for HDFS; see [legacy file system
support](file-system-legacy) for migration details.

## Configuration properties

The connector supports the configuration properties listed below. Properties
not explicitly listed here, such as catalog-specific options, are forwarded to
Paimon as-is.

::::{list-table} Paimon configuration properties
:widths: 32, 53, 15
:header-rows: 1

* - Property name
  - Description
  - Default
* - `warehouse`
  - Paimon warehouse location. Required by the selected Paimon catalog.
  -
* - `metastore`
  - Paimon catalog type, for example `filesystem` or `jdbc`.
  - Paimon default
* - `uri`
  - Catalog URI. For a JDBC catalog this is the JDBC connection URL. For a
    Hive metastore catalog this is the Thrift URI.
  -
* - `jdbc.user`
  - JDBC catalog user.
  -
* - `jdbc.password`
  - JDBC catalog password.
  -
* - `catalog-key`
  - Stable Paimon catalog identity used for catalog and lock coordination.
  -
* - `lock.enabled`
  - Enable the Paimon catalog lock.
  - Paimon default
* - `lock.type`
  - Paimon lock implementation, such as `jdbc` for a JDBC catalog.
  - Paimon default
* - `lock-acquire-timeout`
  - Maximum time to acquire the catalog lock.
  - Paimon default
* - `lock-check-max-sleep`
  - Maximum interval between catalog-lock checks.
  - Paimon default
* - `table.type`
  - Paimon table type for newly created tables, for example
    `TABLE` or `EXTERNAL`.
  - Paimon default
* - `hive-conf-dir`
  - Path to the Hive configuration directory for a Hive metastore catalog.
  -
* - `hadoop-conf-dir`
  - Path to the Hadoop configuration directory.
  -
* - `case-sensitive`
  - Enable case-sensitive table and column name resolution.
  - Paimon default
* - `allow-upper-case`
  - Allow uppercase table and column names.
  - Paimon default
* - `fs.native-s3.enabled`
  - Enable Trino native S3 access for the Paimon warehouse.
  - `false`
* - `fs.hadoop.enabled`
  - Enable Paimon Hadoop file system support. Set to `false` when using Trino
    native S3.
  - Paimon default
* - `s3.endpoint`, `s3.region`, `s3.path-style-access`
  - Native S3 endpoint, region, and path-style setting forwarded to Paimon.
  -
* - `s3.access-key`, `s3.secret-key`
  - Access credentials for S3-compatible object storage. The secret key is a
    sensitive catalog property.
  -
* - `write.spill-path`
  - Local directory used by spillable Paimon writers.
  -
* - `catalog.session-cache.maximum-size`
  - Maximum number of session-specific Paimon catalog instances retained by the
    connector.
  - `1000`
* - `cache-enabled`
  - Enable Paimon catalog caching.
  - Paimon default
* - `client-pool-size`
  - Paimon catalog client pool size.
  - Paimon default
::::

### Fault-tolerant execution support

The connector supports {doc}`/admin/fault-tolerant-execution` of query
processing. Read and write operations are both supported with any retry policy.
Spillable Paimon writers use the `write.spill-path` directory for intermediate
data.

## Catalog session properties

The following catalog session properties can be set per query with
{doc}`/sql/set-session`:

::::{list-table} Paimon catalog session properties
:widths: 32, 53, 15
:header-rows: 1

* - Property name
  - Description
  - Default
* - `scan_timestamp_millis`
  - Read the latest snapshot committed at or before this Unix timestamp in
    milliseconds.
  -
* - `scan_snapshot_id`
  - Read a specific Paimon snapshot.
  -
* - `scan_tag_name`
  - Read the snapshot referenced by a Paimon tag.
  -
* - `scan_file_creation_time_millis`
  - Read the latest snapshot whose data files were created at or before this
    Unix timestamp in milliseconds.
  -
* - `scan_creation_time_millis`
  - Read the latest snapshot created at or before this Unix timestamp in
    milliseconds.
  -
* - `insert_existing_partitions_behavior`
  - Behavior for inserts into an existing partition: `ERROR`, `APPEND`, or
    `OVERWRITE`.
  - `APPEND`
* - `minimum_split_weight`
  - Minimum scheduling weight assigned to a split. Must be a decimal value in
    the range `(0, 1]`.
  - `0.05`
* - `dynamic_filtering_wait_timeout`
  - Maximum time split generation waits for dynamic filters.
  - `0s`
::::

## Table properties

Use the `WITH` clause of `CREATE TABLE` or `ALTER TABLE SET PROPERTIES` to
define Paimon option properties. The connector exposes the documented Paimon 2.0
`CoreOptions` as string-valued properties, converting periods and hyphens in a
Paimon option name to underscores. For example, Paimon's `file.format` and
`merge-engine` options are `file_format` and `merge_engine` in Trino. Scan and
other runtime-only Paimon options are not table properties; use the catalog
session properties instead.

In addition to Paimon options, the connector provides these structural table
properties:

::::{list-table} Paimon structural table properties
:widths: 30, 20, 50
:header-rows: 1

* - Property name
  - Type
  - Description
* - `primary_key`
  - `array(varchar)`
  - Columns that form the primary key. Omit the property for append-only
    tables. This property is set when the table is created.
* - `partitioned_by`
  - `array(varchar)`
  - Columns used to partition the table. This property is set when the table
    is created.
::::

For example:

```sql
CREATE TABLE paimon.sales.orders (
    order_id BIGINT,
    order_date DATE,
    customer_id BIGINT,
    total_amount DECIMAL(12, 2)
)
WITH (
    primary_key = ARRAY['order_id', 'order_date'],
    partitioned_by = ARRAY['order_date'],
    bucket = '4',
    file_format = 'PARQUET',
    merge_engine = 'DEDUPLICATE'
);
```

Consult the Paimon 2.0 documentation for the valid values and semantics of
forwarded Paimon options such as `bucket`, `file_format`, `merge_engine`,
`changelog_producer`, and snapshot-retention settings.

## Type mapping

The connector maps Paimon logical types to Trino types as follows. The file
format restrictions described below still apply when a table is read or written
through a Trino format provider.

### Paimon to Trino type mapping

::::{list-table} Paimon to Trino type mapping
:widths: 45, 55
:header-rows: 1

* - Paimon type
  - Trino type
* - `BOOLEAN`, `TINYINT`, `SMALLINT`, `INT`, `BIGINT`, `FLOAT`, `DOUBLE`
  - Corresponding Trino scalar type (`INT` maps to `INTEGER` and `FLOAT` to
    `REAL`).
* - `DECIMAL(p, s)`, `CHAR(n)`, `VARCHAR(n)`, `STRING`
  - Corresponding `DECIMAL`, `CHAR`, or `VARCHAR` type.
* - `BINARY`, `VARBINARY`, `BLOB`
  - `VARBINARY`
* - `DATE`, `TIME(p)`, `TIMESTAMP(p)`
  - Corresponding `DATE`, `TIME(min(p, 3))`, or `TIMESTAMP(p)` type.
* - `TIMESTAMP WITH LOCAL TIME ZONE(p)`
  - `TIMESTAMP(p) WITH TIME ZONE`
* - `ARRAY(T)`, `MAP(K, V)`, `ROW(...)`
  - Corresponding Trino collection or row type.
* - `VARIANT`
  - `JSON`
* - `VECTOR(T)`, `MULTISET(T)`
  - `ARRAY(T)` and `MAP(T, INTEGER)`, respectively.
::::

### Trino to Paimon type mapping

When creating tables, Trino maps types to Paimon types as follows:

::::{list-table} Trino to Paimon type mapping
:widths: 45, 55
:header-rows: 1

* - Trino type
  - Paimon type
* - `BOOLEAN`
  - `BOOLEAN`
* - `TINYINT`
  - `TINYINT`
* - `SMALLINT`
  - `SMALLINT`
* - `INTEGER`
  - `INT`
* - `BIGINT`
  - `BIGINT`
* - `REAL`
  - `FLOAT`
* - `DOUBLE`
  - `DOUBLE`
* - `DECIMAL(p, s)`
  - `DECIMAL(p, s)`
* - `VARCHAR`, `VARCHAR(n)`
  - `STRING`, `VARCHAR(n)`
* - `CHAR(n)`
  - `CHAR(n)`
* - `VARBINARY`
  - `VARBINARY`
* - `DATE`
  - `DATE`
* - `TIME(p)`
  - `TIME(p)` (precision capped at 3 for writes)
* - `TIMESTAMP(p)`
  - `TIMESTAMP(p)`
* - `TIMESTAMP(p) WITH TIME ZONE`
  - `TIMESTAMP WITH LOCAL TIME ZONE(p)`
* - `JSON`
  - `VARIANT`
* - `ARRAY(T)`
  - `ARRAY(T)`
* - `MAP(K, V)`
  - `MAP(K, V)`
* - `ROW(...)`
  - `ROW(...)`
::::

Paimon stores time values with millisecond precision, so writes of `TIME(p)`
require `p` to be at most `3`.

## SQL support

This connector provides read access and write access to data and metadata in
Paimon. In addition to the {ref}`globally available <sql-globally-available>`
and {ref}`read operation <sql-read-operations>` statements, the connector
supports the following features:

- {ref}`sql-write-operations`:

  - {ref}`Schema and table management <paimon-schema-table-management>`
  - {ref}`Data management <paimon-data-management>`
  - {ref}`sql-view-management` (JDBC and Hive catalogs only)

### Basic usage examples

The connector supports creating schemas. You can create a schema with or
without a specified location:

```sql
CREATE SCHEMA paimon.sales
WITH (location = 's3://example-bucket/warehouse/sales.db');
```

Create a table:

```sql
CREATE TABLE paimon.sales.orders (
    order_id BIGINT,
    order_date DATE,
    customer_id BIGINT,
    total_amount DECIMAL(12, 2)
)
WITH (
    primary_key = ARRAY['order_id', 'order_date'],
    partitioned_by = ARRAY['order_date'],
    bucket = '4',
    file_format = 'PARQUET'
);
```

Create a table with `CREATE TABLE AS SELECT`:

```sql
CREATE TABLE paimon.sales.customer_orders
WITH (primary_key = ARRAY['order_id'])
AS
    SELECT order_id, customer_id, total_amount
    FROM paimon.sales.orders
    WHERE total_amount > 100;
```

Insert data:

```sql
INSERT INTO paimon.sales.orders
VALUES (1, DATE '2024-01-01', 1001, 99.95);
```

Update and delete rows in primary-key tables:

```sql
UPDATE paimon.sales.orders SET total_amount = 109.95 WHERE order_id = 1;
DELETE FROM paimon.sales.orders WHERE order_id = 1;
```

Merge rows:

```sql
MERGE INTO paimon.sales.orders AS t
USING (VALUES (1, DATE '2024-01-01', 1001, 109.95)) AS s(order_id, order_date, customer_id, total_amount)
ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET total_amount = s.total_amount
WHEN NOT MATCHED THEN INSERT (order_id, order_date, customer_id, total_amount) VALUES (s.order_id, s.order_date, s.customer_id, s.total_amount);
```

(paimon-schema-table-management)=
### Schema and table management

The {ref}`sql-schema-table-management` functionality includes support for:

- {doc}`/sql/create-schema`
- {doc}`/sql/drop-schema`
- {doc}`/sql/create-table`
- {doc}`/sql/create-table-as`
- {doc}`/sql/drop-table`
- {doc}`/sql/alter-table`
- {doc}`/sql/comment`
- {doc}`/sql/truncate`

Paimon supports schema evolution, including column add, drop, rename, and type
changes. The connector supports `ALTER TABLE ADD COLUMN`, `DROP COLUMN`,
`RENAME COLUMN`, and `ALTER COLUMN SET DATA TYPE` for widening operations.

(paimon-data-management)=
### Data management

The connector supports the following data management operations:

- {doc}`/sql/insert`
- {doc}`/sql/delete`
- {doc}`/sql/update`
- {doc}`/sql/merge`
- {doc}`/sql/truncate`
- {doc}`/sql/create-table-as`

Row-level `DELETE`, `UPDATE`, and `MERGE` operations require a primary-key
table. Append-only tables support `INSERT`, `TRUNCATE`, and partition-level
deletes.

### Time travel

Set one of the catalog session properties before reading a table to read a
specific snapshot:

```sql
SET SESSION paimon.scan_snapshot_id = 42;
SELECT * FROM paimon.sales.orders;
```

Read by timestamp:

```sql
SET SESSION paimon.scan_timestamp_millis = 1704067200000;
SELECT * FROM paimon.sales.orders;
```

Read by tag:

```sql
SET SESSION paimon.scan_tag_name = 'daily-2024-01-01';
SELECT * FROM paimon.sales.orders;
```

The `scan_timestamp_millis`, `scan_snapshot_id`, `scan_tag_name`,
`scan_file_creation_time_millis`, and `scan_creation_time_millis` session
properties expose the corresponding Paimon scan options. Do not combine
conflicting version selectors in one query.

### Write behavior

`insert_existing_partitions_behavior` controls writes to existing partitions:

```sql
SET SESSION paimon.insert_existing_partitions_behavior = 'APPEND';
```

Supported values are `ERROR`, `APPEND`, and `OVERWRITE`. `KEY_DYNAMIC` tables
require an atomic-capable Paimon catalog lock as described in the JDBC example.

### System tables

Paimon system tables, such as `$snapshots`, `$tags`, and `$manifests`, are
available with Paimon's quoted table-name syntax. For example:

```sql
SELECT * FROM paimon.sales."orders$snapshots";
```

The connector also exposes global system tables in the `sys` schema:

- `sys.tables` — lists all tables across all schemas.
- `sys.partitions` — lists all partitions.
- `sys.all_table_options` — lists all table options.
- `sys.catalog_options` — lists catalog-level options.

### Branches and tags

Paimon branches and tags provide named references to specific snapshots. The
connector supports reading from a branch or tag using the `scan_tag_name`
session property. Create and manage branches and tags using Paimon's API or
external tools, then reference them in Trino:

```sql
SET SESSION paimon.scan_tag_name = 'branch_a';
SELECT * FROM paimon.sales.orders;
```

### Table functions

The connector supports the `table_changes` table function for reading
incremental changes from a Paimon table.

#### table_changes

The function reads changes between two snapshots or timestamps, or reads
changes up to an automatically selected tag. The function is exposed in the
catalog `system` schema:

```sql
SELECT *
FROM TABLE(
    paimon.system.table_changes(
        schema_name => 'sales',
        table_name => 'orders',
        incremental_between => '1,5'
    )
);
```

The function takes these arguments:

- `schema_name`
  : Required name of the source table's schema.
- `table_name`
  : Required name of the source table.
- `incremental_between`
  : Optional pair of snapshot IDs or tag names in the form `start,end`.
    The start snapshot is exclusive and the end snapshot is inclusive.
- `incremental_between_timestamp`
  : Optional pair of start and end timestamps in the form `start,end`.
    The start timestamp is exclusive and the end timestamp is inclusive.
- `incremental_to_auto_tag`
  : Optional end tag or date for reading changes from the preceding automatic
    tag. This requires the table's automatic tag configuration.
- `incremental_between_scan_mode`
  : Scan mode for `incremental_between` or
    `incremental_between_timestamp`: `AUTO`, `DELTA`, `CHANGELOG`, or `DIFF`.
    The default is `AUTO`.
- `incremental_between_tag_to_snapshot`
  : Optional boolean. When `true`, resolve tag names in
    `incremental_between` to their corresponding snapshots before reading.
    The default is `false`.

Exactly one of `incremental_between`, `incremental_between_timestamp`, or
`"<table>$snapshots"` metadata table to determine snapshot IDs, for example:

```sql
SELECT * FROM paimon.sales."orders$snapshots";
```

The function returns the source table's columns. It does not add synthetic
change metadata columns; the returned rows follow the selected Paimon
incremental scan mode.

## File formats and type limitations

The connector reads and writes Paimon Parquet and ORC tables through Trino's
format providers. Paimon `BLOB`, `VARIANT`, `VECTOR`, and `MULTISET` values are
not supported by these providers. ORC writes with Paimon `TIME` columns are
rejected; use Parquet or Paimon's native writer for such tables.

Paimon 2.0 defaults to `zstd` compression for data files. CSV and other text
file formats rely on Hadoop's native compression codec for `zstd`, which is
not available in the no-Hadoop build. Use `file_compression = 'none'` for CSV
tables when native Hadoop libraries are unavailable.

## Performance

The connector maps Paimon snapshot statistics to Trino table and column
statistics. If a historical Paimon snapshot does not contain a statistics file,
or contains no usable table row count, the connector derives a row count from
the planned Paimon splits only when that count is exact. Primary-key tables
without merged split row counts remain unknown rather than receiving an unsafe
estimate.

Manifest-level statistics are retained during split planning so Paimon can
prune files using pushed-down predicates before splits are sent to workers.

`SHOW STATS` and `EXPLAIN` are useful to verify the cardinalities visible to the
optimizer. Paimon column NDV statistics are required for join-output estimates;
without them Trino intentionally keeps the join output unknown while retaining
the exact input cardinalities.

## Known limitations

- `ALTER SCHEMA RENAME` is not supported. Paimon's catalog API does not expose a
  schema rename primitive.
- `COMMENT ON COLUMN view.col` is not supported. Paimon `ViewChange` does not
  expose view column comment mutations.
- Query retries are not supported.
- Partial partition `DELETE` is not supported. Only complete partition deletes
  are optimized through Paimon `truncatePartitions`.
- Materialized views are not supported.
- Role-based access control (`GRANT`/`REVOKE`) is not implemented.
- Views are only supported on JDBC and Hive catalogs, not on the filesystem
  catalog.
- The `TRY` function with column references from Paimon tables is not supported
  due to connector column resolution limitations.
