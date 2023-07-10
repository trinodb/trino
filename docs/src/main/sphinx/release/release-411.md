# Release 411 (29 Mar 2023)

## General

* Add spilled data size to query statistics. ({issue}`16442`)
* Add {func}`sinh` function. ({issue}`16494`)
* Add {func}`quantile_at_value` function. ({issue}`16736`)
* Add support for a `GRACE PERIOD` clause in the `CREATE MATERIALIZED VIEW`
  task. For backwards compatibility, the existing materialized views are
  interpreted as having a `GRACE PERIOD` of zero, however, new materialized
  views have an unlimited grace period by default. This is a backwards
  incompatible change, and the previous behavior can be restored with the
  `legacy.materialized-view-grace-period` configuration property or the
  `legacy_materialized_view_grace_period` session property. ({issue}`15842`)
* Fix potential incorrect query stats when tasks are waiting on running drivers
  to fully terminate. ({issue}`15478`)
* Add support for specifying the number of nodes that will write data during
  `INSERT`, `CREATE TABLE ... AS SELECT`, or `EXECUTE` queries with the
  `query.max-writer-tasks-count` configuration property. ({issue}`16238`)
* Improve performance of queries that contain predicates involving the `year`
  function. ({issue}`14078`)
* Improve performance of queries that contain a `sum` aggregation. ({issue}`16624`)
* Improve performance of `filter` function on arrays. ({issue}`16681`)
* Reduce coordinator memory usage. ({issue}`16668`, {issue}`16669`)
* Reduce redundant data exchanges for queries with multiple aggregations. ({issue}`16328`)
* Fix incorrect query results when using `keyvalue()` methods in the
  [JSON path](json-path-language). ({issue}`16482`)
* Fix potential incorrect results in queries involving joins and a
  non-deterministic value. ({issue}`16512`)
* Fix potential query failure when exchange compression is enabled. ({issue}`16541`)
* Fix query failure when calling a function with a large number of parameters. ({issue}`15979`)

## BigQuery connector

* Fix failure of aggregation queries when executed against a materialized view,
  external table, or snapshot table. ({issue}`15546`)

## Delta Lake connector

* Add support for inserting into tables that have
  [simple invariants](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#column-invariants). ({issue}`16136`)
* Add [generated column expressions](https://docs.delta.io/latest/delta-batch.html#use-generated-columns)
  to the `Extra` column in the results of `DESCRIBE` and `SHOW COLUMNS`. ({issue}`16631`)
* Expand the `flush_metadata_cache` table procedure to also flush the internal
  caches of table snapshots and active data files. ({issue}`16466`)
* Collect statistics for newly-created columns. ({issue}`16109`)
* Remove the `$data` system table. ({issue}`16650`)
* Fix query failure when evaluating a `WHERE` clause on a partition column. ({issue}`16388`)

## Druid connector

* Fix failure when the query passed to the `query` table function contains a
  column alias. ({issue}`16225`)

## Elasticsearch connector

* Remove the deprecated pass-through query, which has been replaced with the
  `raw_query` table function. ({issue}`13050`)

## Hive connector

* Add a native OpenX JSON file format reader and writer. These can be disabled
  with the `openx_json_native_reader_enabled` and
  `openx_json_native_writer_enabled` session properties or the
  `openx-json.native-reader.enabled` and `openx-json.native-writer.enabled`
  configuration properties. ({issue}`16073`)
* Add support for implicit coercions between `char` types of different lengths. ({issue}`16402`)
* Improve performance of queries with joins where both sides of a join have keys
  with the same table bucketing definition. ({issue}`16381`)
* Improve query planning performance for queries scanning tables with a large
  number of columns. ({issue}`16203`)
* Improve scan performance for `COUNT(*)` queries on row-oriented formats. ({issue}`16595`)
* Ensure the value of the `hive.metastore-stats-cache-ttl` configuration
  property always is greater than or equal to the value specified in the
  `hive.metastore-cache-ttl` configuration property. ({issue}`16625`)
* Skip listing Glue metastore tables with invalid column types. ({issue}`16677`)
* Fix query failure when a file that is using a text file format with a
  single header row that is large enough to be split into multiple files. ({issue}`16492`)
* Fix potential query failure when Kerberos is enabled and the query execution
  takes longer than a Kerberos ticket's lifetime. ({issue}`16680`)

## Hudi connector

* Add a `$timeline` system table which can be queried to inspect the Hudi table
  timeline. ({issue}`16149`)

## Iceberg connector

* Add a `migrate` procedure that converts a Hive table to an Iceberg table. ({issue}`13196`)
* Add support for materialized views with a freshness grace period. ({issue}`15842`)
* Add a `$refs` system table which can be queried to inspect snapshot
  references. ({issue}`15649`)
* Add support for creation of materialized views partitioned with a temporal
  partitioning function on a `timestamp with time zone` column. ({issue}`16637`)
* Improve performance of queries run after data was written by Trino. ({issue}`15441`)
* Remove the `$data` system table. ({issue}`16650`)
* Fix failure when the `$files` system table contains non-null values in the
  `key_metadata`, `split_offsets`, and `equality_ids` columns. ({issue}`16473`)
* Fix failure when partitioned column names contain uppercase characters. ({issue}`16622`)

## Ignite connector

* Add support for predicate pushdown with a `LIKE` clause. ({issue}`16396`)
* Add support for pushdown of joins. ({issue}`16428`)
* Add support for {doc}`/sql/delete`. ({issue}`16720`)

## MariaDB connector

* Fix failure when the query passed to the `query` table function contains a
  column alias. ({issue}`16225`)

## MongoDB connector

* Fix incorrect results when the query passed to the MongoDB `query` table
  function contains helper functions such as `ISODate`. ({issue}`16626`)

## MySQL connector

* Fix failure when the query passed to the `query` table function contains a
  column alias. ({issue}`16225`)

## Oracle connector

* Improve performance of queries when the network latency between Trino and
  Oracle is high, or when selecting a small number of columns. ({issue}`16644`)

## PostgreSQL connector

* Improve performance of queries when the network latency between Trino and
  PostgreSQL is high, or when selecting a small number of columns. ({issue}`16644`)

## Redshift connector

* Improve performance of queries when the network latency between Trino and
  Redshift is high, or when selecting a small number of columns. ({issue}`16644`)

## SingleStore connector

* Fix failure when the query passed to the `query` table function contains a
  column alias. ({issue}`16225`)

## SQL Server connector

* Add support for executing stored procedures using the `procedure` table
  function. ({issue}`16696`)
