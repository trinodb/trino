---
myst:
  substitutions:
    default_domain_compaction_threshold: '`5000`'
---

# Phoenix connector

```{raw} html
<img src="../_static/img/phoenix.png" class="connector-logo">
```

The Phoenix connector allows querying data stored in
[Apache HBase](https://hbase.apache.org/) using
[Apache Phoenix](https://phoenix.apache.org/).

## Requirements

To query HBase data through Phoenix, you need:

- Network access from the Trino coordinator and workers to the ZooKeeper
  servers. The default port is 2181.
- A compatible version of Phoenix: all 5.x versions starting from 5.2.0 are supported.
- The Trino [](jvm-config) must allow using the Java security manager:
  ```text
  # https://bugs.openjdk.org/browse/JDK-8327134
  -Djava.security.manager=allow
  ```

## Configuration

To configure the Phoenix connector, create a catalog properties file
`etc/catalog/example.properties` with the following contents,
replacing `host1,host2,host3` with a comma-separated list of the ZooKeeper
nodes used for discovery of the HBase cluster:

```text
connector.name=phoenix5
phoenix.connection-url=jdbc:phoenix:host1,host2,host3:2181:/hbase
phoenix.config.resources=/path/to/hbase-site.xml
```

The optional paths to Hadoop resource files, such as `hbase-site.xml` are used
to load custom Phoenix client connection properties.

The following Phoenix-specific configuration properties are available:

| Property name                      | Required | Description                                                                                                                                                                                                                                                                                                                                                                                      |
|------------------------------------| -------- |--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `phoenix.connection-url`           | Yes      | `jdbc:phoenix[:zk_quorum][:zk_port][:zk_hbase_path]`. The `zk_quorum` is a comma separated list of ZooKeeper servers. The `zk_port` is the ZooKeeper port. The `zk_hbase_path` is the HBase root znode path, that is configurable using `hbase-site.xml`.  By default the location is `/hbase`                                                                                                   |
| `phoenix.config.resources`         | No       | Comma-separated list of configuration files (e.g. `hbase-site.xml`) to use for connection properties.  These files must exist on the machines running Trino.                                                                                                                                                                                                                                     |
| `phoenix.max-scans-per-split`      | No       | Maximum number of HBase scans that will be performed in a single split. Default is 20. Lower values will lead to more splits in Trino. Can also be set via session propery `max_scans_per_split`. For details see: [https://phoenix.apache.org/update_statistics.html](https://phoenix.apache.org/update_statistics.html). (This setting has no effect when guideposts are disabled in Phoenix.) |
| `phoenix.server-scan-page-timeout` | No       | The time limit on the amount of work single RPC request can do before it times out. Type: [](prop-type-duration).                                                                                                                                                                                                                                                                                |

```{include} jdbc-common-configurations.fragment
```

```{include} query-comment-format.fragment
```

```{include} jdbc-domain-compaction-threshold.fragment
```

```{include} jdbc-case-insensitive-matching.fragment
```

## Querying Phoenix tables

The default empty schema in Phoenix maps to a schema named `default` in Trino.
You can see the available Phoenix schemas by running `SHOW SCHEMAS`:

```
SHOW SCHEMAS FROM example;
```

If you have a Phoenix schema named `web`, you can view the tables
in this schema by running `SHOW TABLES`:

```
SHOW TABLES FROM example.web;
```

You can see a list of the columns in the `clicks` table in the `web` schema
using either of the following:

```
DESCRIBE example.web.clicks;
SHOW COLUMNS FROM example.web.clicks;
```

Finally, you can access the `clicks` table in the `web` schema:

```
SELECT * FROM example.web.clicks;
```

If you used a different name for your catalog properties file, use
that catalog name instead of `example` in the above examples.

(phoenix-type-mapping)=
## Type mapping

Because Trino and Phoenix each support types that the other does not, this
connector {ref}`modifies some types <type-mapping-overview>` when reading or
writing data. Data types may not map the same way in both directions between
Trino and the data source. Refer to the following sections for type mapping in
each direction.

### Phoenix type to Trino type mapping

The connector maps Phoenix types to the corresponding Trino types following this
table:

:::{list-table} Phoenix type to Trino type mapping
:widths: 50, 50
:header-rows: 1

* - Phoenix database type
  - Trino type
* - `BOOLEAN`
  - `BOOLEAN`
* - `TINYINT`
  - `TINYINT`
* - `UNSIGNED_TINYINT`
  - `TINYINT`
* - `SMALLINT`
  - `SMALLINT`
* - `UNSIGNED_SMALLINT`
  - `SMALLINT`
* - `INTEGER`
  - `INTEGER`
* - `UNSIGNED_INT`
  - `INTEGER`
* - `BIGINT`
  - `BIGINT`
* - `UNSIGNED_LONG`
  - `BIGINT`
* - `FLOAT`
  - `REAL`
* - `UNSIGNED_FLOAT`
  - `REAL`
* - `DOUBLE`
  - `DOUBLE`
* - `UNSIGNED_DOUBLE`
  - `DOUBLE`
* - `DECIMAL(p,s)`
  - `DECIMAL(p,s)`
* - `CHAR(n)`
  - `CHAR(n)`
* - `VARCHAR(n)`
  - `VARCHAR(n)`
* - `BINARY`
  - `VARBINARY`
* - `VARBINARY`
  - `VARBINARY`
* - `DATE`
  - `DATE`
* - `UNSIGNED_DATE`
  - `DATE`
* - `ARRAY`
  - `ARRAY`
:::

No other types are supported.

### Trino type to Phoenix type mapping

The Phoenix fixed length `BINARY` data type is mapped to the Trino variable
length `VARBINARY` data type. There is no way to create a Phoenix table in
Trino that uses the `BINARY` data type, as Trino does not have an equivalent
type.

The connector maps Trino types to the corresponding Phoenix types following this
table:

:::{list-table} Trino type to Phoenix type mapping
:widths: 50, 50
:header-rows: 1

* - Trino database type
  - Phoenix type
* - `BOOLEAN`
  - `BOOLEAN`
* - `TINYINT`
  - `TINYINT`
* - `SMALLINT`
  - `SMALLINT`
* - `INTEGER`
  - `INTEGER`
* - `BIGINT`
  - `BIGINT`
* - `REAL`
  - `FLOAT`
* - `DOUBLE`
  - `DOUBLE`
* - `DECIMAL(p,s)`
  - `DECIMAL(p,s)`
* - `CHAR(n)`
  - `CHAR(n)`
* - `VARCHAR(n)`
  - `VARCHAR(n)`
* - `VARBINARY`
  - `VARBINARY`
* - `DATE`
  - `DATE`
* - `ARRAY`
  - `ARRAY`
:::

No other types are supported.

```{include} decimal-type-handling.fragment
```

```{include} jdbc-type-mapping.fragment
```

## Table properties - Phoenix

Table property usage example:

```
CREATE TABLE example_schema.scientists (
  recordkey VARCHAR,
  birthday DATE,
  name VARCHAR,
  age BIGINT
)
WITH (
  rowkeys = 'recordkey,birthday',
  salt_buckets = 10
);
```

The following are supported Phoenix table properties from [https://phoenix.apache.org/language/index.html#options](https://phoenix.apache.org/language/index.html#options)

| Property name           | Default value | Description                                                                                                           |
| ----------------------- | ------------- | --------------------------------------------------------------------------------------------------------------------- |
| `rowkeys`               | `ROWKEY`      | Comma-separated list of primary key columns.  See further description below                                           |
| `split_on`              | (none)        | List of keys to presplit the table on. See [Split Point](https://phoenix.apache.org/language/index.html#split_point). |
| `salt_buckets`          | (none)        | Number of salt buckets for this table.                                                                                |
| `disable_wal`           | false         | Whether to disable WAL writes in HBase for this table.                                                                |
| `immutable_rows`        | false         | Declares whether this table has rows which are write-once, append-only.                                               |
| `default_column_family` | `0`           | Default column family name to use for this table.                                                                     |

### `rowkeys`

This is a comma-separated list of columns to be used as the table's primary key. If not specified, a `BIGINT` primary key column named `ROWKEY` is generated
, as well as a sequence with the same name as the table suffixed with `_seq` (i.e. `<schema>.<table>_seq`)
, which is used to automatically populate the `ROWKEY` for each row during insertion.

## Table properties - HBase

The following are the supported HBase table properties that are passed through by Phoenix during table creation.
Use them in the same way as above: in the `WITH` clause of the `CREATE TABLE` statement.

| Property name         | Default value | Description                                                                                                            |
| --------------------- | ------------- | ---------------------------------------------------------------------------------------------------------------------- |
| `versions`            | `1`           | The maximum number of versions of each cell to keep.                                                                   |
| `min_versions`        | `0`           | The minimum number of cell versions to keep.                                                                           |
| `compression`         | `NONE`        | Compression algorithm to use.  Valid values are `NONE` (default), `SNAPPY`, `LZO`, `LZ4`, or `GZ`.                     |
| `data_block_encoding` | `FAST_DIFF`   | Block encoding algorithm to use. Valid values are: `NONE`, `PREFIX`, `DIFF`, `FAST_DIFF` (default), or `ROW_INDEX_V1`. |
| `ttl`                 | `FOREVER`     | Time To Live for each cell.                                                                                            |
| `bloomfilter`         | `NONE`        | Bloomfilter to use. Valid values are `NONE` (default), `ROW`, or `ROWCOL`.                                             |

(phoenix-sql-support)=
## SQL support

The connector provides read and write access to data and metadata in Phoenix. In
addition to the [globally available](sql-globally-available) and [read
operation](sql-read-operations) statements, the connector supports the following
features:

- [](/sql/insert), see also [](phoenix-insert)
- [](/sql/update)
- [](/sql/delete), see also [](phoenix-delete)
- [](/sql/merge), see also [](phoenix-merge)
- [](/sql/create-table)
- [](/sql/create-table-as)
- [](/sql/drop-table)
- [](/sql/create-schema)
- [](/sql/drop-schema)
- [](phoenix-procedures)

(phoenix-insert)=
```{include} non-transactional-insert.fragment
```

(phoenix-delete)=
```{include} sql-delete-limitation.fragment
```

(phoenix-merge)=
```{include} non-transactional-merge.fragment
```

(phoenix-procedures)=
### Procedures

```{include} procedures-execute.fragment
```
