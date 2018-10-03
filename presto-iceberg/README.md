This plugin allows Presto to interact with [Iceberg](https://iceberg.apache.org/) tables.

## Status

Currently, this plugin supports create, CTAS, drop, rename, and reading from Iceberg tables.
It also supports adding, dropping, and renaming columns.

## Configuration

Iceberg supports the same metastore configuration properties as the Hive connector.
At a minimum, `hive.metastore.uri` must be configured:

```
connector.name=iceberg
hive.metastore.uri=thrift://localhost:9083
```

## Unpartitioned Tables

``` sql
CREATE TABLE iceberg.testdb.sample (
    i int, 
    s varchar
)
```

## Partitioned Tables

Iceberg supports partitioning by specifying transforms over the table columns.
A partition is created for each unique tuple value produced by the transforms.
Identity transforms are simply the column name. Other transforms:

* `year(ts)`
* `month(ts)`
* `day(ts)`
* `hour(ts)`
* `bucket(x, 512)`
* `truncate(s, 16)`

``` sql
CREATE TABLE iceberg.testdb.sample_partitioned (
    b boolean,
    dateint integer,
    l bigint,
    f real,
    d double,
    de decimal(12,2),
    dt date,
    ts timestamp,
    s varchar,
    bi varbinary
 )
WITH (partitioning = ARRAY['dateint', 's'])
```

## Migrating existing tables

The connector can read from or write to Hive tables that have been migrated to Iceberg.
Currently, there is no Presto support to migrate Hive tables to Presto, so you will
need to use either the Icerberg API or Spark.

## System Tables and Columns

The connector supports `table$partitions` as a substitute for Hive's `SHOW PARTITIONS`.
The differences are that it returns some partition metrics for each partition value
and you can also use it for unpartitioned tables.

Iceberg supports `$snapshot_id` and `$snapshot_timestamp_ms` as hidden columns.
These columns allow users to query an old version of the table. Think of this
as a time travel feature which lets you query your table's snapshot at a given time.

## TODO

* Update the README to reflect the current status, and convert it to proper connector documentation
  before announcing the connector as ready for use.
* Fix table listing to skip non-Iceberg tables. This will need a new metastore method to list tables
  filtered on a property name, similar to how view listing works in `ThriftHiveMetastore`.
* Predicate pushdown is currently broken, which means delete is also broken. The code from the
  original `getTableLayouts()` implementation needs to be updated for `applyFilter()`.
* All of the `HdfsContext` calls that use `/tmp` need to be fixed.
* `HiveConfig` needs to be removed. We might need to split out separate config classes in the
  Hive connector for the components that are reused in Iceberg.
* We should try to remove `HiveColumnHandle`. This will require replacing or abstracting
  `HivePageSource`, which is currently used to handle schema evolution and prefilled
  column values (identity partitions).
* Writing of decimals and timestamps is broken, since their representation in Parquet seems
  to be different for Iceberg and Hive. Reads are probably also broken, but this isn't tested
  yet since writes don't work.
* UUID likely does not work and is not tested.
* Implement time type.
* System tables (history, snapshots, manifests, partitions) probably do not work and are not tested.
  These likely need to be implemented as a Presto `SystemTable` using the API in the Iceberg library.
* Needs complete tests for all data types and all partitioning transforms.
* Needs integration tests (probably as product tests) for interoperability with Spark in both directions
  (write Spark -> read Presto, write Presto -> read Spark).
* Needs correctness tests for partition pruning.
  (also validate the pushdown is happening by checking the query plans?)
* Return table statistics so CBO can leverage them.
* Iceberg table properties.
* Add tests for `CREATE TABLE LIKE`.
* Add test for creating `NOT NULL` columns.
* Add tests for table comments.
* Add tests for column comments.
* Add tests for non-Iceberg tables
  * listing tables in a schema
  * listing columns in a schema
    * filtered for an Iceberg table
    * filtered for a non-Iceberg table
    * no filters
  * describing a table
  * selecting from a table
* Add procedures for *migrate table* and *rollback table to snapshot*.
