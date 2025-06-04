# Hudi connector

```{raw} html
<img src="../_static/img/hudi.png" class="connector-logo">
```

The Hudi connector enables querying [Hudi](https://hudi.apache.org/docs/overview/) tables.

## Requirements

To use the Hudi connector, you need:

- Hudi version 0.12.3 or higher.
- Network access from the Trino coordinator and workers to the Hudi storage.
- Access to a Hive metastore service (HMS).
- Network access from the Trino coordinator to the HMS.
- Data files stored in the [Parquet file format](parquet-format-configuration)
  on a [supported file system](hudi-file-system-configuration).

## General configuration

To configure the Hudi connector, create a catalog properties file
`etc/catalog/example.properties` that references the `hudi` connector.

You must configure a [metastore for table metadata](/object-storage/metastores).

You must select and configure one of the [supported file
systems](hudi-file-system-configuration).

```properties
connector.name=hudi
hive.metastore.uri=thrift://example.net:9083
fs.x.enabled=true
```

Replace the `fs.x.enabled` configuration property with the desired file system.

There are {ref}`HMS configuration properties <general-metastore-properties>`
available for use with the Hudi connector. The connector recognizes Hudi tables
synced to the metastore by the [Hudi sync tool](https://hudi.apache.org/docs/syncing_metastore).

Additionally, following configuration properties can be set depending on the use-case:

:::{list-table} Hudi configuration properties
:widths: 30, 55, 15
:header-rows: 1

* - Property name
  - Description
  - Default
* - `hudi.columns-to-hide`
  - List of column names that are hidden from the query output. It can be used
    to hide Hudi meta fields. By default, no fields are hidden.
  -
* - `hudi.parquet.use-column-names`
  - Access Parquet columns using names from the file. If disabled, then columns
    are accessed using the index. Only applicable to Parquet file format.
  - `true`
* - `hudi.split-generator-parallelism`
  - Number of threads to generate splits from partitions.
  - `4`
* - `hudi.split-loader-parallelism`
  - Number of threads to run background split loader. A single background split
    loader is needed per query.
  - `4`
* - `hudi.size-based-split-weights-enabled`
  - Unlike uniform splitting, size-based splitting ensures that each batch of
    splits has enough data to process. By default, it is enabled to improve
    performance.
  - `true`
* - `hudi.standard-split-weight-size`
  - The split size corresponding to the standard weight (1.0) when size-based
    split weights are enabled.
  - `128MB`
* - `hudi.minimum-assigned-split-weight`
  - Minimum weight that a split can be assigned when size-based split weights
    are enabled.
  - `0.05`
* - `hudi.max-splits-per-second`
  - Rate at which splits are queued for processing. The queue is throttled if
    this rate limit is breached.
  - `Integer.MAX_VALUE`
* - `hudi.max-outstanding-splits`
  - Maximum outstanding splits in a batch enqueued for processing.
  - `1000`
* - `hudi.per-transaction-metastore-cache-maximum-size`
  - Maximum number of metastore data objects per transaction in the Hive
    metastore cache.
  - `2000`
* - `hudi.query-partition-filter-required`
  - Set to `true` to force a query to use a partition column in the filter condition.
    The equivalent catalog session property is `query_partition_filter_required`.
    Enabling this property causes query failures if the partition column used
    in the filter condition doesn't effectively reduce the number of data files read.
    Example: Complex filter expressions such as `id = 1 OR part_key = '100'`
    or `CAST(part_key AS INTEGER) % 2 = 0` are not recognized as partition filters,
    and queries using such expressions fail if the property is set to `true`.
  - `false`
* - `hudi.ignore-absent-partitions`
  - Ignore partitions when the file system location does not exist rather than
    failing the query. This skips data that may be expected to be part of the
    table.
  - `false`

:::

(hudi-file-system-configuration)=
## File system access configuration

The connector supports accessing the following file systems:

* [](/object-storage/file-system-azure)
* [](/object-storage/file-system-gcs)
* [](/object-storage/file-system-s3)
* [](/object-storage/file-system-hdfs)

You must enable and configure the specific file system access. [Legacy
support](file-system-legacy) is not recommended and will be removed.

## SQL support

The connector provides read access to data in the Hudi table that has been synced to
Hive metastore. The {ref}`globally available <sql-globally-available>`
and {ref}`read operation <sql-read-operations>` statements are supported.

### Basic usage examples

In the following example queries, `stock_ticks_cow` is the Hudi copy-on-write
table referred to in the Hudi [quickstart guide](https://hudi.apache.org/docs/docker_demo/).

```sql
USE example.example_schema;

SELECT symbol, max(ts)
FROM stock_ticks_cow
GROUP BY symbol
HAVING symbol = 'GOOG';
```

```text
  symbol   |        _col1         |
-----------+----------------------+
 GOOG      | 2018-08-31 10:59:00  |
(1 rows)
```

```sql
SELECT dt, symbol
FROM stock_ticks_cow
WHERE symbol = 'GOOG';
```

```text
    dt      | symbol |
------------+--------+
 2018-08-31 |  GOOG  |
(1 rows)
```

```sql
SELECT dt, count(*)
FROM stock_ticks_cow
GROUP BY dt;
```

```text
    dt      | _col1 |
------------+--------+
 2018-08-31 |  99  |
(1 rows)
```

### Schema and table management

Hudi supports [two types of tables](https://hudi.apache.org/docs/table_types)
depending on how the data is indexed and laid out on the file system. The following
table displays a support matrix of tables types and query types for the connector:

:::{list-table} Hudi configuration properties
:widths: 45, 55
:header-rows: 1

* - Table type
  - Supported query type
* - Copy on write
  - Snapshot queries
* - Merge on read
  - Read-optimized queries
:::

(hudi-metadata-tables)=
#### Metadata tables

The connector exposes a metadata table for each Hudi table.
The metadata table contains information about the internal structure
of the Hudi table. You can query each metadata table by appending the
metadata table name to the table name:

```
SELECT * FROM "test_table$timeline"
```

##### `$timeline` table

The `$timeline` table provides a detailed view of meta-data instants
in the Hudi table. Instants are specific points in time.

You can retrieve the information about the timeline of the Hudi table
`test_table` by using the following query:

```
SELECT * FROM "test_table$timeline"
```

```text
 timestamp          | action  | state
--------------------+---------+-----------
8667764846443717831 | commit  | COMPLETED
7860805980949777961 | commit  | COMPLETED
```

The output of the query has the following columns:

:::{list-table} Timeline columns
:widths: 20, 30, 50
:header-rows: 1

* - Name
  - Type
  - Description
* - `timestamp`
  - `VARCHAR`
  - Instant time is typically a timestamp when the actions performed.
* - `action`
  - `VARCHAR`
  - [Type of action](https://hudi.apache.org/docs/concepts/#timeline) performed
    on the table.
* - `state`
  - `VARCHAR`
  - Current state of the instant.
:::
