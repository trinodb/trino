# Hive connector

```{raw} html
<img src="../_static/img/hive.png" class="connector-logo">
```

```{toctree}
:hidden: true
:maxdepth: 1

Metastores <metastores>
Security <hive-security>
Amazon S3 <hive-s3>
Azure Storage <hive-azure>
Google Cloud Storage <hive-gcs-tutorial>
IBM Cloud Object Storage <hive-cos>
Storage Caching <hive-caching>
Alluxio <hive-alluxio>
Object storage file formats <object-storage-file-formats>
```

The Hive connector allows querying data stored in an
[Apache Hive](https://hive.apache.org/)
data warehouse. Hive is a combination of three components:

- Data files in varying formats, that are typically stored in the
  Hadoop Distributed File System (HDFS) or in object storage systems
  such as Amazon S3.
- Metadata about how the data files are mapped to schemas and tables. This
  metadata is stored in a database, such as MySQL, and is accessed via the Hive
  metastore service.
- A query language called HiveQL. This query language is executed on a
  distributed computing framework such as MapReduce or Tez.

Trino only uses the first two components: the data and the metadata.
It does not use HiveQL or any part of Hive's execution environment.

## Requirements

The Hive connector requires a
{ref}`Hive metastore service <hive-thrift-metastore>` (HMS), or a compatible
implementation of the Hive metastore, such as
{ref}`AWS Glue <hive-glue-metastore>`.

Apache Hadoop HDFS 2.x and 3.x are supported.

Many distributed storage systems including HDFS,
{doc}`Amazon S3 <hive-s3>` or S3-compatible systems,
[Google Cloud Storage](hive-gcs-tutorial),
{doc}`Azure Storage <hive-azure>`, and
{doc}`IBM Cloud Object Storage<hive-cos>` can be queried with the Hive
connector.

The coordinator and all workers must have network access to the Hive metastore
and the storage system. Hive metastore access with the Thrift protocol defaults
to using port 9083.

Data files must be in a supported file format. Some file formats can be
configured using file format configuration properties per catalog:

- {ref}`ORC <hive-orc-configuration>`
- {ref}`Parquet <hive-parquet-configuration>`
- Avro
- RCText (RCFile using ColumnarSerDe)
- RCBinary (RCFile using LazyBinaryColumnarSerDe)
- SequenceFile
- JSON (using org.apache.hive.hcatalog.data.JsonSerDe)
- CSV (using org.apache.hadoop.hive.serde2.OpenCSVSerde)
- TextFile

## General configuration

To configure the Hive connector, create a catalog properties file
`etc/catalog/example.properties` that references the `hive`
connector and defines a metastore. You must configure a metastore for table
metadata. If you are using a {ref}`Hive metastore <hive-thrift-metastore>`,
`hive.metastore.uri` must be configured:

```properties
connector.name=hive
hive.metastore.uri=thrift://example.net:9083
```

If you are using {ref}`AWS Glue <hive-glue-metastore>` as your metastore, you
must instead set `hive.metastore` to `glue`:

```properties
connector.name=hive
hive.metastore=glue
```

Each metastore type has specific configuration properties along with
{ref}`general metastore configuration properties <general-metastore-properties>`.

### Multiple Hive clusters

You can have as many catalogs as you need, so if you have additional
Hive clusters, simply add another properties file to `etc/catalog`
with a different name, making sure it ends in `.properties`. For
example, if you name the property file `sales.properties`, Trino
creates a catalog named `sales` using the configured connector.

### HDFS configuration

For basic setups, Trino configures the HDFS client automatically and
does not require any configuration files. In some cases, such as when using
federated HDFS or NameNode high availability, it is necessary to specify
additional HDFS client options in order to access your HDFS cluster. To do so,
add the `hive.config.resources` property to reference your HDFS config files:

```text
hive.config.resources=/etc/hadoop/conf/core-site.xml,/etc/hadoop/conf/hdfs-site.xml
```

Only specify additional configuration files if necessary for your setup.
We recommend reducing the configuration files to have the minimum
set of required properties, as additional properties may cause problems.

The configuration files must exist on all Trino nodes. If you are
referencing existing Hadoop config files, make sure to copy them to
any Trino nodes that are not running Hadoop.

### HDFS username and permissions

Before running any `CREATE TABLE` or `CREATE TABLE AS` statements
for Hive tables in Trino, you must check that the user Trino is
using to access HDFS has access to the Hive warehouse directory. The Hive
warehouse directory is specified by the configuration variable
`hive.metastore.warehouse.dir` in `hive-site.xml`, and the default
value is `/user/hive/warehouse`.

When not using Kerberos with HDFS, Trino accesses HDFS using the
OS user of the Trino process. For example, if Trino is running as
`nobody`, it accesses HDFS as `nobody`. You can override this
username by setting the `HADOOP_USER_NAME` system property in the
Trino {ref}`jvm-config`, replacing `hdfs_user` with the
appropriate username:

```text
-DHADOOP_USER_NAME=hdfs_user
```

The `hive` user generally works, since Hive is often started with
the `hive` user and this user has access to the Hive warehouse.

Whenever you change the user Trino is using to access HDFS, remove
`/tmp/presto-*` on HDFS, as the new user may not have access to
the existing temporary directories.

(hive-configuration-properties)=

### Hive general configuration properties

The following table lists general configuration properties for the Hive
connector. There are additional sets of configuration properties throughout the
Hive connector documentation.

```{eval-rst}
.. list-table:: Hive general configuration properties
    :widths: 35, 50, 15
    :header-rows: 1

    * - Property Name
      - Description
      - Default
    * - ``hive.config.resources``
      - An optional comma-separated list of HDFS configuration files. These
        files must exist on the machines running Trino. Only specify this if
        absolutely necessary to access HDFS. Example: ``/etc/hdfs-site.xml``
      -
    * - ``hive.recursive-directories``
      - Enable reading data from subdirectories of table or partition locations.
        If disabled, subdirectories are ignored. This is equivalent to the
        ``hive.mapred.supports.subdirectories`` property in Hive.
      - ``false``
    * - ``hive.ignore-absent-partitions``
      - Ignore partitions when the file system location does not exist rather
        than failing the query. This skips data that may be expected to be part
        of the table.
      - ``false``
    * - ``hive.storage-format``
      - The default file format used when creating new tables.
      - ``ORC``
    * - ``hive.compression-codec``
      - The compression codec to use when writing files. Possible values are
        ``NONE``, ``SNAPPY``, ``LZ4``, ``ZSTD``, or ``GZIP``.
      - ``GZIP``
    * - ``hive.force-local-scheduling``
      - Force splits to be scheduled on the same node as the Hadoop DataNode
        process serving the split data. This is useful for installations where
        Trino is collocated with every DataNode.
      - ``false``
    * - ``hive.respect-table-format``
      - Should new partitions be written using the existing table format or the
        default Trino format?
      - ``true``
    * - ``hive.immutable-partitions``
      - Can new data be inserted into existing partitions? If ``true`` then
        setting ``hive.insert-existing-partitions-behavior`` to ``APPEND`` is
        not allowed. This also affects the ``insert_existing_partitions_behavior``
        session property in the same way.
      - ``false``
    * - ``hive.insert-existing-partitions-behavior``
      - What happens when data is inserted into an existing partition? Possible
        values are

            * ``APPEND`` - appends data to existing partitions
            * ``OVERWRITE`` - overwrites existing partitions
            * ``ERROR`` - modifying existing partitions is not allowed
      - ``APPEND``
    * - ``hive.target-max-file-size``
      - Best effort maximum size of new files.
      - ``1GB``
    * - ``hive.create-empty-bucket-files``
      - Should empty files be created for buckets that have no data?
      - ``false``
    * - ``hive.validate-bucketing``
      - Enables validation that data is in the correct bucket when reading
        bucketed tables.
      - ``true``
    * - ``hive.partition-statistics-sample-size``
      - Specifies the number of partitions to analyze when computing table
        statistics.
      - 100
    * - ``hive.max-partitions-per-writers``
      - Maximum number of partitions per writer.
      - 100
    * - ``hive.max-partitions-for-eager-load``
      - The maximum number of partitions for a single table scan to load eagerly
        on the coordinator. Certain optimizations are not possible without eager
        loading.
      - 100,000
    * - ``hive.max-partitions-per-scan``
      - Maximum number of partitions for a single table scan.
      - 1,000,000
    * - ``hive.dfs.replication``
      - Hadoop file system replication factor.
      -
    * - ``hive.security``
      - See :doc:`hive-security`.
      -
    * - ``security.config-file``
      - Path of config file to use when ``hive.security=file``. See
        :ref:`catalog-file-based-access-control` for details.
      -
    * - ``hive.non-managed-table-writes-enabled``
      - Enable writes to non-managed (external) Hive tables.
      - ``false``
    * - ``hive.non-managed-table-creates-enabled``
      - Enable creating non-managed (external) Hive tables.
      - ``true``
    * - ``hive.collect-column-statistics-on-write``
      - Enables automatic column level statistics collection on write. See
        `Table Statistics <#table-statistics>`__ for details.
      - ``true``
    * - ``hive.s3select-pushdown.enabled``
      - Enable query pushdown to JSON files using the AWS S3 Select service.
      - ``false``
    * - ``hive.s3select-pushdown.experimental-textfile-pushdown-enabled``
      - Enable query pushdown to TEXTFILE tables using the AWS S3 Select service.
      - ``false``
    * - ``hive.s3select-pushdown.max-connections``
      - Maximum number of simultaneously open connections to S3 for
        :ref:`s3selectpushdown`.
      - 500
    * - ``hive.file-status-cache-tables``
      - Cache directory listing for specific tables. Examples:

            * ``fruit.apple,fruit.orange`` to cache listings only for tables
              ``apple`` and ``orange`` in schema ``fruit``
            * ``fruit.*,vegetable.*`` to cache listings for all tables
              in schemas ``fruit`` and ``vegetable``
            * ``*`` to cache listings for all tables in all schemas
      -
    * - ``hive.file-status-cache.max-retained-size``
      - Maximum retained size of cached file status entries.
      - ``1GB``
    * - ``hive.file-status-cache-expire-time``
      - How long a cached directory listing is considered valid.
      - ``1m``
    * - ``hive.per-transaction-file-status-cache.max-retained-size``
      - Maximum retained size of all entries in per transaction file status cache.
        Retained size limit is shared across all running queries.
      - ``100MB``
    * - ``hive.rcfile.time-zone``
      - Adjusts binary encoded timestamp values to a specific time zone. For
        Hive 3.1+, this must be set to UTC.
      - JVM default
    * - ``hive.timestamp-precision``
      - Specifies the precision to use for Hive columns of type ``TIMESTAMP``.
        Possible values are ``MILLISECONDS``, ``MICROSECONDS`` and ``NANOSECONDS``.
        Values with higher precision than configured are rounded.
      - ``MILLISECONDS``
    * - ``hive.temporary-staging-directory-enabled``
      - Controls whether the temporary staging directory configured at
        ``hive.temporary-staging-directory-path`` is used for write
        operations. Temporary staging directory is never used for writes to
        non-sorted tables on S3, encrypted HDFS or external location. Writes to
        sorted tables will utilize this path for staging temporary files during
        sorting operation. When disabled, the target storage will be used for
        staging while writing sorted tables which can be inefficient when
        writing to object stores like S3.
      - ``true``
    * - ``hive.temporary-staging-directory-path``
      - Controls the location of temporary staging directory that is used for
        write operations. The ``${USER}`` placeholder can be used to use a
        different location for each user.
      - ``/tmp/presto-${USER}``
    * - ``hive.hive-views.enabled``
      - Enable translation for :ref:`Hive views <hive-views>`.
      - ``false``
    * - ``hive.hive-views.legacy-translation``
      - Use the legacy algorithm to translate :ref:`Hive views <hive-views>`.
        You can use the ``hive_views_legacy_translation`` catalog session
        property for temporary, catalog specific use.
      - ``false``
    * - ``hive.parallel-partitioned-bucketed-writes``
      - Improve parallelism of partitioned and bucketed table writes. When
        disabled, the number of writing threads is limited to number of buckets.
      - ``true``
    * - ``hive.fs.new-directory-permissions``
      - Controls the permissions set on new directories created for tables. It
        must be either 'skip' or an octal number, with a leading 0. If set to
        'skip', permissions of newly created directories will not be set by
        Trino.
      - ``0777``
    * - ``hive.fs.cache.max-size``
      - Maximum number of cached file system objects.
      - 1000
    * - ``hive.query-partition-filter-required``
      - Set to ``true`` to force a query to use a partition filter. You can use
        the ``query_partition_filter_required`` catalog session property for
        temporary, catalog specific use.
      - ``false``
    * - ``hive.table-statistics-enabled``
      - Enables :doc:`/optimizer/statistics`. The equivalent
        :doc:`catalog session property </sql/set-session>` is
        ``statistics_enabled`` for session specific use. Set to ``false`` to
        disable statistics. Disabling statistics means that
        :doc:`/optimizer/cost-based-optimizations` can not make smart decisions
        about the query plan.
      - ``true``
    * - ``hive.auto-purge``
      - Set the default value for the auto_purge table property for managed
        tables. See the :ref:`hive-table-properties` for more information on
        auto_purge.
      - ``false``
    * - ``hive.partition-projection-enabled``
      - Enables Athena partition projection support
      - ``false``
    * - ``hive.max-partition-drops-per-query``
      - Maximum number of partitions to drop in a single query.
      - 100,000
    * - ``hive.single-statement-writes``
      - Enables auto-commit for all writes. This can be used to disallow
        multi-statement write transactions.
      - ``false``
```

## Storage

The Hive connector supports the following storage options:

- {doc}`Amazon S3 <hive-s3>`
- {doc}`Azure Storage <hive-azure>`
- {doc}`Google Cloud Storage <hive-gcs-tutorial>`
- {doc}`IBM Cloud Object Storage <hive-cos>`

The Hive connector also supports {doc}`storage caching <hive-caching>`.

## Security

Please see the {doc}`/connector/hive-security` section for information on the
security options available for the Hive connector.

(hive-sql-support)=

## SQL support

The connector provides read access and write access to data and metadata in the
configured object storage system and metadata stores:

- {ref}`Globally available statements <sql-globally-available>`; see also
  {ref}`Globally available statements <hive-procedures>`

- {ref}`Read operations <sql-read-operations>`

- {ref}`sql-write-operations`:

  - {ref}`sql-data-management`; see also
    {ref}`Hive-specific data management <hive-data-management>`
  - {ref}`sql-schema-table-management`; see also
    {ref}`Hive-specific schema and table management <hive-schema-and-table-management>`
  - {ref}`sql-view-management`; see also
    {ref}`Hive-specific view management <hive-sql-view-management>`

- {ref}`sql-security-operations`: see also
  {ref}`SQL standard-based authorization for object storage <hive-sql-standard-based-authorization>`

- {ref}`sql-transactions`

Refer to {doc}`the migration guide </appendix/from-hive>` for practical advice
on migrating from Hive to Trino.

The following sections provide Hive-specific information regarding SQL support.

(hive-examples)=

### Basic usage examples

The examples shown here work on Google Cloud Storage by replacing `s3://` with
`gs://`.

Create a new Hive table named `page_views` in the `web` schema
that is stored using the ORC file format, partitioned by date and
country, and bucketed by user into `50` buckets. Note that Hive
requires the partition columns to be the last columns in the table:

```
CREATE TABLE example.web.page_views (
  view_time TIMESTAMP,
  user_id BIGINT,
  page_url VARCHAR,
  ds DATE,
  country VARCHAR
)
WITH (
  format = 'ORC',
  partitioned_by = ARRAY['ds', 'country'],
  bucketed_by = ARRAY['user_id'],
  bucket_count = 50
)
```

Create a new Hive schema named `web` that stores tables in an
S3 bucket named `my-bucket`:

```
CREATE SCHEMA example.web
WITH (location = 's3://my-bucket/')
```

Drop a schema:

```
DROP SCHEMA example.web
```

Drop a partition from the `page_views` table:

```
DELETE FROM example.web.page_views
WHERE ds = DATE '2016-08-09'
  AND country = 'US'
```

Query the `page_views` table:

```
SELECT * FROM example.web.page_views
```

List the partitions of the `page_views` table:

```
SELECT * FROM example.web."page_views$partitions"
```

Create an external Hive table named `request_logs` that points at
existing data in S3:

```
CREATE TABLE example.web.request_logs (
  request_time TIMESTAMP,
  url VARCHAR,
  ip VARCHAR,
  user_agent VARCHAR
)
WITH (
  format = 'TEXTFILE',
  external_location = 's3://my-bucket/data/logs/'
)
```

Collect statistics for the `request_logs` table:

```
ANALYZE example.web.request_logs;
```

Drop the external table `request_logs`. This only drops the metadata
for the table. The referenced data directory is not deleted:

```
DROP TABLE example.web.request_logs
```

- {doc}`/sql/create-table-as` can be used to create transactional tables in ORC format like this:

  ```
  CREATE TABLE <name>
  WITH (
      format='ORC',
      transactional=true
  )
  AS <query>
  ```

Add an empty partition to the `page_views` table:

```
CALL system.create_empty_partition(
    schema_name => 'web',
    table_name => 'page_views',
    partition_columns => ARRAY['ds', 'country'],
    partition_values => ARRAY['2016-08-09', 'US']);
```

Drop stats for a partition of the `page_views` table:

```
CALL system.drop_stats(
    schema_name => 'web',
    table_name => 'page_views',
    partition_values => ARRAY[ARRAY['2016-08-09', 'US']]);
```

(hive-procedures)=

### Procedures

Use the {doc}`/sql/call` statement to perform data manipulation or
administrative tasks. Procedures must include a qualified catalog name, if your
Hive catalog is called `web`:

```
CALL web.system.example_procedure()
```

The following procedures are available:

- `system.create_empty_partition(schema_name, table_name, partition_columns, partition_values)`

  Create an empty partition in the specified table.

- `system.sync_partition_metadata(schema_name, table_name, mode, case_sensitive)`

  Check and update partitions list in metastore. There are three modes available:

  - `ADD` : add any partitions that exist on the file system, but not in the metastore.
  - `DROP`: drop any partitions that exist in the metastore, but not on the file system.
  - `FULL`: perform both `ADD` and `DROP`.

  The `case_sensitive` argument is optional. The default value is `true` for compatibility
  with Hive's `MSCK REPAIR TABLE` behavior, which expects the partition column names in
  file system paths to use lowercase (e.g. `col_x=SomeValue`). Partitions on the file system
  not conforming to this convention are ignored, unless the argument is set to `false`.

- `system.drop_stats(schema_name, table_name, partition_values)`

  Drops statistics for a subset of partitions or the entire table. The partitions are specified as an
  array whose elements are arrays of partition values (similar to the `partition_values` argument in
  `create_empty_partition`). If `partition_values` argument is omitted, stats are dropped for the
  entire table.

(register-partition)=

- `system.register_partition(schema_name, table_name, partition_columns, partition_values, location)`

  Registers existing location as a new partition in the metastore for the specified table.

  When the `location` argument is omitted, the partition location is
  constructed using `partition_columns` and `partition_values`.

  Due to security reasons, the procedure is enabled only when `hive.allow-register-partition-procedure`
  is set to `true`.

(unregister-partition)=

- `system.unregister_partition(schema_name, table_name, partition_columns, partition_values)`

  Unregisters given, existing partition in the metastore for the specified table.
  The partition data is not deleted.

(hive-flush-metadata-cache)=

- `system.flush_metadata_cache()`

  Flush all Hive metadata caches.

- `system.flush_metadata_cache(schema_name => ..., table_name => ...)`

  Flush Hive metadata caches entries connected with selected table.
  Procedure requires named parameters to be passed

- `system.flush_metadata_cache(schema_name => ..., table_name => ..., partition_columns => ARRAY[...], partition_values => ARRAY[...])`

  Flush Hive metadata cache entries connected with selected partition.
  Procedure requires named parameters to be passed.

(hive-data-management)=

### Data management

Some {ref}`data management <sql-data-management>` statements may be affected by
the Hive catalog's authorization check policy. In the default `legacy` policy,
some statements are disabled by default. See {doc}`hive-security` for more
information.

The {ref}`sql-data-management` functionality includes support for `INSERT`,
`UPDATE`, `DELETE`, and `MERGE` statements, with the exact support
depending on the storage system, file format, and metastore.

When connecting to a Hive metastore version 3.x, the Hive connector supports
reading from and writing to insert-only and ACID tables, with full support for
partitioning and bucketing.

{doc}`/sql/delete` applied to non-transactional tables is only supported if the
table is partitioned and the `WHERE` clause matches entire partitions.
Transactional Hive tables with ORC format support "row-by-row" deletion, in
which the `WHERE` clause may match arbitrary sets of rows.

{doc}`/sql/update` is only supported for transactional Hive tables with format
ORC. `UPDATE` of partition or bucket columns is not supported.

{doc}`/sql/merge` is only supported for ACID tables.

ACID tables created with [Hive Streaming Ingest](https://cwiki.apache.org/confluence/display/Hive/Streaming+Data+Ingest)
are not supported.

(hive-schema-and-table-management)=

### Schema and table management

The Hive connector supports querying and manipulating Hive tables and schemas
(databases). While some uncommon operations must be performed using
Hive directly, most operations can be performed using Trino.

#### Schema evolution

Hive allows the partitions in a table to have a different schema than the
table. This occurs when the column types of a table are changed after
partitions already exist (that use the original column types). The Hive
connector supports this by allowing the same conversions as Hive:

- `VARCHAR` to and from `TINYINT`, `SMALLINT`, `INTEGER` and `BIGINT`
- `REAL` to `DOUBLE`
- Widening conversions for integers, such as `TINYINT` to `SMALLINT`

Any conversion failure results in null, which is the same behavior
as Hive. For example, converting the string `'foo'` to a number,
or converting the string `'1234'` to a `TINYINT` (which has a
maximum value of `127`).

(hive-avro-schema)=

#### Avro schema evolution

Trino supports querying and manipulating Hive tables with the Avro storage
format, which has the schema set based on an Avro schema file/literal. Trino is
also capable of creating the tables in Trino by infering the schema from a
valid Avro schema file located locally, or remotely in HDFS/Web server.

To specify that the Avro schema should be used for interpreting table data, use
the `avro_schema_url` table property.

The schema can be placed in the local file system or remotely in the following
locations:

- HDFS (e.g. `avro_schema_url = 'hdfs://user/avro/schema/avro_data.avsc'`)
- S3 (e.g. `avro_schema_url = 's3n:///schema_bucket/schema/avro_data.avsc'`)
- A web server (e.g. `avro_schema_url = 'http://example.org/schema/avro_data.avsc'`)

The URL, where the schema is located, must be accessible from the Hive metastore
and Trino coordinator/worker nodes.

Alternatively, you can use the table property `avro_schema_literal` to define
the Avro schema.

The table created in Trino using the `avro_schema_url` or
`avro_schema_literal` property behaves the same way as a Hive table with
`avro.schema.url` or `avro.schema.literal` set.

Example:

```
CREATE TABLE example.avro.avro_data (
   id BIGINT
 )
WITH (
   format = 'AVRO',
   avro_schema_url = '/usr/local/avro_data.avsc'
)
```

The columns listed in the DDL (`id` in the above example) is ignored if `avro_schema_url` is specified.
The table schema matches the schema in the Avro schema file. Before any read operation, the Avro schema is
accessed so the query result reflects any changes in schema. Thus Trino takes advantage of Avro's backward compatibility abilities.

If the schema of the table changes in the Avro schema file, the new schema can still be used to read old data.
Newly added/renamed fields *must* have a default value in the Avro schema file.

The schema evolution behavior is as follows:

- Column added in new schema:
  Data created with an older schema produces a *default* value when table is using the new schema.
- Column removed in new schema:
  Data created with an older schema no longer outputs the data from the column that was removed.
- Column is renamed in the new schema:
  This is equivalent to removing the column and adding a new one, and data created with an older schema
  produces a *default* value when table is using the new schema.
- Changing type of column in the new schema:
  If the type coercion is supported by Avro or the Hive connector, then the conversion happens.
  An error is thrown for incompatible types.

##### Limitations

The following operations are not supported when `avro_schema_url` is set:

- `CREATE TABLE AS` is not supported.
- Bucketing(`bucketed_by`) columns are not supported in `CREATE TABLE`.
- `ALTER TABLE` commands modifying columns are not supported.

(hive-alter-table-execute)=

#### ALTER TABLE EXECUTE

The connector supports the `optimize` command for use with
{ref}`ALTER TABLE EXECUTE <alter-table-execute>`.

The `optimize` command is used for rewriting the content
of the specified non-transactional table so that it is merged
into fewer but larger files.
In case that the table is partitioned, the data compaction
acts separately on each partition selected for optimization.
This operation improves read performance.

All files with a size below the optional `file_size_threshold`
parameter (default value for the threshold is `100MB`) are
merged:

```sql
ALTER TABLE test_table EXECUTE optimize
```

The following statement merges files in a table that are
under 10 megabytes in size:

```sql
ALTER TABLE test_table EXECUTE optimize(file_size_threshold => '10MB')
```

You can use a `WHERE` clause with the columns used to partition the table,
to filter which partitions are optimized:

```sql
ALTER TABLE test_partitioned_table EXECUTE optimize
WHERE partition_key = 1
```

The `optimize` command is disabled by default, and can be enabled for a
catalog with the `<catalog-name>.non_transactional_optimize_enabled`
session property:

```sql
SET SESSION <catalog_name>.non_transactional_optimize_enabled=true
```

:::{warning}
Because Hive tables are non-transactional, take note of the following possible
outcomes:

- If queries are run against tables that are currently being optimized,
  duplicate rows may be read.
- In rare cases where exceptions occur during the `optimize` operation,
  a manual cleanup of the table directory is needed. In this situation, refer
  to the Trino logs and query failure messages to see which files must be
  deleted.
:::

(hive-table-properties)=

#### Table properties

Table properties supply or set metadata for the underlying tables. This
is key for {doc}`/sql/create-table-as` statements. Table properties are passed
to the connector using a {doc}`WITH </sql/create-table-as>` clause:

```
CREATE TABLE tablename
WITH (format='CSV',
      csv_escape = '"')
```

```{eval-rst}
.. list-table:: Hive connector table properties
  :widths: 20, 60, 20
  :header-rows: 1

  * - Property name
    - Description
    - Default
  * - ``auto_purge``
    - Indicates to the configured metastore to perform a purge when a table or
      partition is deleted instead of a soft deletion using the trash.
    -
  * - ``avro_schema_url``
    - The URI pointing to :ref:`hive-avro-schema` for the table.
    -
  * - ``bucket_count``
    - The number of buckets to group data into. Only valid if used with
      ``bucketed_by``.
    - 0
  * - ``bucketed_by``
    - The bucketing column for the storage table. Only valid if used with
      ``bucket_count``.
    - ``[]``
  * - ``bucketing_version``
    - Specifies which Hive bucketing version to use. Valid values are ``1``
      or ``2``.
    -
  * - ``csv_escape``
    - The CSV escape character. Requires CSV format.
    -
  * - ``csv_quote``
    - The CSV quote character. Requires CSV format.
    -
  * - ``csv_separator``
    - The CSV separator character. Requires CSV format. You can use other
      separators such as ``|`` or use Unicode to configure invisible separators
      such tabs with ``U&'\0009'``.
    - ``,``
  * - ``external_location``
    - The URI for an external Hive table on S3, Azure Blob Storage, etc. See the
      :ref:`hive-examples` for more information.
    -
  * - ``format``
    - The table file format. Valid values include ``ORC``, ``PARQUET``,
      ``AVRO``, ``RCBINARY``, ``RCTEXT``, ``SEQUENCEFILE``, ``JSON``,
      ``TEXTFILE``, ``CSV``, and ``REGEX``. The catalog property
      ``hive.storage-format`` sets the default value and can change it to a
      different default.
    -
  * - ``null_format``
    - The serialization format for ``NULL`` value. Requires TextFile, RCText,
      or SequenceFile format.
    -
  * - ``orc_bloom_filter_columns``
    - Comma separated list of columns to use for ORC bloom filter. It improves
      the performance of queries using range predicates when reading ORC files.
      Requires ORC format.
    - ``[]``
  * - ``orc_bloom_filter_fpp``
    - The ORC bloom filters false positive probability. Requires ORC format.
    - 0.05
  * - ``partitioned_by``
    - The partitioning column for the storage table. The columns listed in the
      ``partitioned_by`` clause must be the last columns as defined in the DDL.
    - ``[]``
  * - ``skip_footer_line_count``
    - The number of footer lines to ignore when parsing the file for data.
      Requires TextFile or CSV format tables.
    -
  * - ``skip_header_line_count``
    - The number of header lines to ignore when parsing the file for data.
      Requires TextFile or CSV format tables.
    -
  * - ``sorted_by``
    - The column to sort by to determine bucketing for row. Only valid if
      ``bucketed_by`` and ``bucket_count`` are specified as well.
    - ``[]``
  * - ``textfile_field_separator``
    - Allows the use of custom field separators, such as '|', for TextFile
      formatted tables.
    -
  * - ``textfile_field_separator_escape``
    - Allows the use of a custom escape character for TextFile formatted tables.
    -
  * - ``transactional``
    - Set this property to ``true`` to create an ORC ACID transactional table.
      Requires ORC format. This property may be shown as true for insert-only
      tables created using older versions of Hive.
    -
  * - ``partition_projection_enabled``
    - Enables partition projection for selected table.
      Mapped from AWS Athena table property
      `projection.enabled <https://docs.aws.amazon.com/athena/latest/ug/partition-projection-setting-up.html>`_.
    -
  * - ``partition_projection_ignore``
    - Ignore any partition projection properties stored in the metastore for
      the selected table. This is a Trino-only property which allows you to
      work around compatibility issues on a specific table, and if enabled,
      Trino ignores all other configuration options related to partition
      projection.
    -
  * - ``partition_projection_location_template``
    - Projected partition location template, such as
      ``s3a://test/name=${name}/``. Mapped from the AWS Athena table property
      `storage.location.template <https://docs.aws.amazon.com/athena/latest/ug/partition-projection-setting-up.html#partition-projection-specifying-custom-s3-storage-locations>`_
    - ``${table_location}/${partition_name}``
  * - ``extra_properties``
    - Additional properties added to a Hive table. The properties are not used by Trino,
      and are available in the ``$properties`` metadata table.
      The properties are not included in the output of ``SHOW CREATE TABLE`` statements.
    -
```

(hive-special-tables)=

#### Metadata tables

The raw Hive table properties are available as a hidden table, containing a
separate column per table property, with a single row containing the property
values.

##### `$properties` table

The properties table name is composed with the table name and `$properties` appended.
It exposes the parameters of the table in the metastore.

You can inspect the property names and values with a simple query:

```
SELECT * FROM example.web."page_views$properties";
```

```text
       stats_generated_via_stats_task        | auto.purge |       presto_query_id       | presto_version | transactional
---------------------------------------------+------------+-----------------------------+----------------+---------------
 workaround for potential lack of HIVE-12730 | false      | 20230705_152456_00001_nfugi | 423            | false
```

##### `$partitions` table

The `$partitions` table provides a list of all partition values
of a partitioned table.

The following example query returns all partition values from the
`page_views` table in the `web` schema of the `example` catalog:

```
SELECT * FROM example.web."page_views$partitions";
```

```text
     day    | country
------------+---------
 2023-07-01 | POL
 2023-07-02 | POL
 2023-07-03 | POL
 2023-03-01 | USA
 2023-03-02 | USA
```

(hive-column-properties)=

#### Column properties

```{eval-rst}
.. list-table:: Hive connector column properties
  :widths: 20, 60, 20
  :header-rows: 1

  * - Property name
    - Description
    - Default
  * - ``partition_projection_type``
    - Defines the type of partition projection to use on this column.
      May be used only on partition columns. Available types:
      ``ENUM``, ``INTEGER``, ``DATE``, ``INJECTED``.
      Mapped from the AWS Athena table property
      `projection.${columnName}.type <https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html>`_.
    -
  * - ``partition_projection_values``
    - Used with ``partition_projection_type`` set to ``ENUM``. Contains a static
      list of values used to generate partitions.
      Mapped from the AWS Athena table property
      `projection.${columnName}.values <https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html>`_.
    -
  * - ``partition_projection_range``
    - Used with ``partition_projection_type`` set to ``INTEGER`` or ``DATE`` to
      define a range. It is a two-element array, describing the minimum and
      maximum range values used to generate partitions. Generation starts from
      the minimum, then increments by the defined
      ``partition_projection_interval`` to the maximum. For example, the format
      is ``['1', '4']`` for a ``partition_projection_type`` of ``INTEGER`` and
      ``['2001-01-01', '2001-01-07']`` or ``['NOW-3DAYS', 'NOW']`` for a
      ``partition_projection_type`` of ``DATE``. Mapped from the AWS Athena
      table property
      `projection.${columnName}.range <https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html>`_.
    -
  * - ``partition_projection_interval``
    - Used with ``partition_projection_type`` set to ``INTEGER`` or ``DATE``. It
      represents the interval used to generate partitions within
      the given range ``partition_projection_range``. Mapped from the AWS Athena
      table property
      `projection.${columnName}.interval <https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html>`_.
    -
  * - ``partition_projection_digits``
    - Used with ``partition_projection_type`` set to ``INTEGER``.
      The number of digits to be used with integer column projection.
      Mapped from the AWS Athena table property
      `projection.${columnName}.digits <https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html>`_.
    -
  * - ``partition_projection_format``
    - Used with ``partition_projection_type`` set to ``DATE``.
      The date column projection format, defined as a string such as ``yyyy MM``
      or ``MM-dd-yy HH:mm:ss`` for use with the
      `Java DateTimeFormatter class <https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html>`_.
      Mapped from the AWS Athena table property
      `projection.${columnName}.format <https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html>`_.
    -
  * - ``partition_projection_interval_unit``
    - Used with ``partition_projection_type=DATA``.
      The date column projection range interval unit
      given in ``partition_projection_interval``.
      Mapped from the AWS Athena table property
      `projection.${columnName}.interval.unit <https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html>`_.
    -
```

(hive-special-columns)=

#### Metadata columns

In addition to the defined columns, the Hive connector automatically exposes
metadata in a number of hidden columns in each table:

- `$bucket`: Bucket number for this row
- `$path`: Full file system path name of the file for this row
- `$file_modified_time`: Date and time of the last modification of the file for this row
- `$file_size`: Size of the file for this row
- `$partition`: Partition name for this row

You can use these columns in your SQL statements like any other column. They
can be selected directly, or used in conditional statements. For example, you
can inspect the file size, location and partition for each record:

```
SELECT *, "$path", "$file_size", "$partition"
FROM example.web.page_views;
```

Retrieve all records that belong to files stored in the partition
`ds=2016-08-09/country=US`:

```
SELECT *, "$path", "$file_size"
FROM example.web.page_views
WHERE "$partition" = 'ds=2016-08-09/country=US'
```

(hive-sql-view-management)=

### View management

Trino allows reading from Hive materialized views, and can be configured to
support reading Hive views.

#### Materialized views

The Hive connector supports reading from Hive materialized views.
In Trino, these views are presented as regular, read-only tables.

(hive-views)=

#### Hive views

Hive views are defined in HiveQL and stored in the Hive Metastore Service. They
are analyzed to allow read access to the data.

The Hive connector includes support for reading Hive views with three different
modes.

- Disabled
- Legacy
- Experimental

If using Hive views from Trino is required, you must compare results in Hive and
Trino for each view definition to ensure identical results. Use the experimental
mode whenever possible. Avoid using the legacy mode. Leave Hive views support
disabled, if you are not accessing any Hive views from Trino.

You can configure the behavior in your catalog properties file.

By default, Hive views are executed with the `RUN AS DEFINER` security mode.
Set the  `hive.hive-views.run-as-invoker` catalog configuration property to
`true` to use `RUN AS INVOKER` semantics.

**Disabled**

The default behavior is to ignore Hive views. This means that your business
logic and data encoded in the views is not available in Trino.

**Legacy**

A very simple implementation to execute Hive views, and therefore allow read
access to the data in Trino, can be enabled with
`hive.hive-views.enabled=true` and
`hive.hive-views.legacy-translation=true`.

For temporary usage of the legacy behavior for a specific catalog, you can set
the `hive_views_legacy_translation` {doc}`catalog session property
</sql/set-session>` to `true`.

This legacy behavior interprets any HiveQL query that defines a view as if it
is written in SQL. It does not do any translation, but instead relies on the
fact that HiveQL is very similar to SQL.

This works for very simple Hive views, but can lead to problems for more complex
queries. For example, if a HiveQL function has an identical signature but
different behaviors to the SQL version, the returned results may differ. In more
extreme cases the queries might fail, or not even be able to be parsed and
executed.

**Experimental**

The new behavior is better engineered and has the potential to become a lot
more powerful than the legacy implementation. It can analyze, process, and
rewrite Hive views and contained expressions and statements.

It supports the following Hive view functionality:

- `UNION [DISTINCT]` and `UNION ALL` against Hive views
- Nested `GROUP BY` clauses
- `current_user()`
- `LATERAL VIEW OUTER EXPLODE`
- `LATERAL VIEW [OUTER] EXPLODE` on array of struct
- `LATERAL VIEW json_tuple`

You can enable the experimental behavior with
`hive.hive-views.enabled=true`. Remove the
`hive.hive-views.legacy-translation` property or set it to `false` to make
sure legacy is not enabled.

Keep in mind that numerous features are not yet implemented when experimenting
with this feature. The following is an incomplete list of **missing**
functionality:

- HiveQL `current_date`, `current_timestamp`, and others
- Hive function calls including `translate()`, window functions, and others
- Common table expressions and simple case expressions
- Honor timestamp precision setting
- Support all Hive data types and correct mapping to Trino types
- Ability to process custom UDFs

(hive-fte-support)=

## Fault-tolerant execution support

The connector supports {doc}`/admin/fault-tolerant-execution` of query
processing. Read and write operations are both supported with any retry policy
on non-transactional tables.

Read operations are supported with any retry policy on transactional tables.
Write operations and `CREATE TABLE ... AS` operations are not supported with
any retry policy on transactional tables.

## Performance

The connector includes a number of performance improvements, detailed in the
following sections.

### Table statistics

The Hive connector supports collecting and managing {doc}`table statistics
</optimizer/statistics>` to improve query processing performance.

When writing data, the Hive connector always collects basic statistics
(`numFiles`, `numRows`, `rawDataSize`, `totalSize`)
and by default will also collect column level statistics:

```{eval-rst}
.. list-table:: Available table statistics
    :widths: 35, 65
    :header-rows: 1

    * - Column type
      - Collectible statistics
    * - ``TINYINT``
      - Number of nulls, number of distinct values, min/max values
    * - ``SMALLINT``
      - Number of nulls, number of distinct values, min/max values
    * - ``INTEGER``
      - Number of nulls, number of distinct values, min/max values
    * - ``BIGINT``
      - Number of nulls, number of distinct values, min/max values
    * - ``DOUBLE``
      - Number of nulls, number of distinct values, min/max values
    * - ``REAL``
      - Number of nulls, number of distinct values, min/max values
    * - ``DECIMAL``
      - Number of nulls, number of distinct values, min/max values
    * - ``DATE``
      - Number of nulls, number of distinct values, min/max values
    * - ``TIMESTAMP``
      - Number of nulls, number of distinct values, min/max values
    * - ``VARCHAR``
      - Number of nulls, number of distinct values
    * - ``CHAR``
      - Number of nulls, number of distinct values
    * - ``VARBINARY``
      - Number of nulls
    * - ``BOOLEAN``
      - Number of nulls, number of true/false values
```

(hive-analyze)=

#### Updating table and partition statistics

If your queries are complex and include joining large data sets,
running {doc}`/sql/analyze` on tables/partitions may improve query performance
by collecting statistical information about the data.

When analyzing a partitioned table, the partitions to analyze can be specified
via the optional `partitions` property, which is an array containing
the values of the partition keys in the order they are declared in the table schema:

```
ANALYZE table_name WITH (
    partitions = ARRAY[
        ARRAY['p1_value1', 'p1_value2'],
        ARRAY['p2_value1', 'p2_value2']])
```

This query will collect statistics for two partitions with keys
`p1_value1, p1_value2` and `p2_value1, p2_value2`.

On wide tables, collecting statistics for all columns can be expensive and can have a
detrimental effect on query planning. It is also typically unnecessary - statistics are
only useful on specific columns, like join keys, predicates, grouping keys. One can
specify a subset of columns to be analyzed via the optional `columns` property:

```
ANALYZE table_name WITH (
    partitions = ARRAY[ARRAY['p2_value1', 'p2_value2']],
    columns = ARRAY['col_1', 'col_2'])
```

This query collects statistics for columns `col_1` and `col_2` for the partition
with keys `p2_value1, p2_value2`.

Note that if statistics were previously collected for all columns, they must be dropped
before re-analyzing just a subset:

```
CALL system.drop_stats('schema_name', 'table_name')
```

You can also drop statistics for selected partitions only:

```
CALL system.drop_stats(
    schema_name => 'schema',
    table_name => 'table',
    partition_values => ARRAY[ARRAY['p2_value1', 'p2_value2']])
```

(hive-dynamic-filtering)=

### Dynamic filtering

The Hive connector supports the {doc}`dynamic filtering </admin/dynamic-filtering>` optimization.
Dynamic partition pruning is supported for partitioned tables stored in any file format
for broadcast as well as partitioned joins.
Dynamic bucket pruning is supported for bucketed tables stored in any file format for
broadcast joins only.

For tables stored in ORC or Parquet file format, dynamic filters are also pushed into
local table scan on worker nodes for broadcast joins. Dynamic filter predicates
pushed into the ORC and Parquet readers are used to perform stripe or row-group pruning
and save on disk I/O. Sorting the data within ORC or Parquet files by the columns used in
join criteria significantly improves the effectiveness of stripe or row-group pruning.
This is because grouping similar data within the same stripe or row-group
greatly improves the selectivity of the min/max indexes maintained at stripe or
row-group level.

#### Delaying execution for dynamic filters

It can often be beneficial to wait for the collection of dynamic filters before starting
a table scan. This extra wait time can potentially result in significant overall savings
in query and CPU time, if dynamic filtering is able to reduce the amount of scanned data.

For the Hive connector, a table scan can be delayed for a configured amount of
time until the collection of dynamic filters by using the configuration property
`hive.dynamic-filtering.wait-timeout` in the catalog file or the catalog
session property `<hive-catalog>.dynamic_filtering_wait_timeout`.

(hive-table-redirection)=

### Table redirection

```{include} table-redirection.fragment
```

The connector supports redirection from Hive tables to Iceberg
and Delta Lake tables with the following catalog configuration properties:

- `hive.iceberg-catalog-name` for redirecting the query to {doc}`/connector/iceberg`
- `hive.delta-lake-catalog-name` for redirecting the query to {doc}`/connector/delta-lake`

(hive-performance-tuning-configuration)=

### Performance tuning configuration properties

The following table describes performance tuning properties for the Hive
connector.

:::{warning}
Performance tuning configuration properties are considered expert-level
features. Altering these properties from their default values is likely to
cause instability and performance degradation.
:::

```{eval-rst}
.. list-table::
    :widths: 30, 50, 20
    :header-rows: 1

    * - Property name
      - Description
      - Default value
    * - ``hive.max-outstanding-splits``
      - The target number of buffered splits for each table scan in a query,
        before the scheduler tries to pause.
      - ``1000``
    * - ``hive.max-outstanding-splits-size``
      - The maximum size allowed for buffered splits for each table scan
        in a query, before the query fails.
      - ``256 MB``
    * - ``hive.max-splits-per-second``
      - The maximum number of splits generated per second per table scan. This
        can be used to reduce the load on the storage system. By default, there
        is no limit, which results in Trino maximizing the parallelization of
        data access.
      -
    * - ``hive.max-initial-splits``
      - For each table scan, the coordinator first assigns file sections of up
        to ``max-initial-split-size``. After ``max-initial-splits`` have been
        assigned, ``max-split-size`` is used for the remaining splits.
      - ``200``
    * - ``hive.max-initial-split-size``
      - The size of a single file section assigned to a worker until
        ``max-initial-splits`` have been assigned. Smaller splits results in
        more parallelism, which gives a boost to smaller queries.
      - ``32 MB``
    * - ``hive.max-split-size``
      - The largest size of a single file section assigned to a worker. Smaller
        splits result in more parallelism and thus can decrease latency, but
        also have more overhead and increase load on the system.
      - ``64 MB``
```

## Hive 3-related limitations

- For security reasons, the `sys` system catalog is not accessible.
- Hive's `timestamp with local zone` data type is mapped to
  `timestamp with time zone` with UTC timezone. It only supports reading
  values - writing to tables with columns of this type is not supported.
- Due to Hive issues [HIVE-21002](https://issues.apache.org/jira/browse/HIVE-21002)
  and [HIVE-22167](https://issues.apache.org/jira/browse/HIVE-22167), Trino does
  not correctly read `TIMESTAMP` values from Parquet, RCBinary, or Avro
  file formats created by Hive 3.1 or later. When reading from these file formats,
  Trino returns different results than Hive.
- Trino does not support gathering table statistics for Hive transactional tables.
  You must use Hive to gather table statistics with
  [ANALYZE statement](https://cwiki.apache.org/confluence/display/hive/statsdev#StatsDev-ExistingTables%E2%80%93ANALYZE)
  after table creation.
