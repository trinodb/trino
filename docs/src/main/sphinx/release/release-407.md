# Release 407 (16 Feb 2023)

## General

* Add support for correlated queries involving a `VALUES` clause with a single
  row. ({issue}`15989`)
* Reduce memory usage for large schemas. This behavior can be enabled with the
  `query.remote-task.enable-adaptive-request-size` configuration property and
  configured with the `query.remote-task.max-request-size`,
  `query.remote-task.request-size-headroom`, and
  `query.remote-task.guaranteed-splits-per-task` configuration properties or
  their respective session properties. ({issue}`15721`)
* Improve concurrency when small, concurrent queries are run on a large cluster
  by automatically determining how many nodes to use for distributed joins
  and aggregations. This can be managed with the `query.max-hash-partition-count`
  (renamed from `query.hash-partition-count`) and `query.min-hash-partition-count`
  configuration properties. ({issue}`15489`)
* Improve query memory tracking. ({issue}`15983`)
* Improve memory usage accounting for queries with dynamic filters. ({issue}`16110`)
* Improve query performance when a predicate evaluates to a null value. ({issue}`15744`)
* Improve performance of queries with joins on the output of global
  aggregations. ({issue}`15858`)
* Improve performance of selective queries, queries that read a small number of
  columns, and queries that process tables with large Parquet row groups or ORC
  stripes. ({issue}`15579`)
* Improve performance of queries with window functions. ({issue}`15994`)
* Return an exit code of `100` when Trino crashes during startup. ({issue}`16113`)
* Fix precision loss when converting `time` values with a precision higher than
  three and `time with time zone` values with lower precision. ({issue}`15861`)
* Fix potential incorrect results due to a query reporting normal completion
  instead of failing. ({issue}`15917`)
* Fix connection errors caused by a reusable connection being closed. ({issue}`16121`)
* Fix incorrect results for queries involving an equality predicate in a `WHERE`
  clause that is equal to a term of a `SELECT` clause in one of the branches of
  a `JOIN`. ({issue}`16101`)

## Cassandra connector

* Add `query` table function for full query pass-through to the connector. ({issue}`15973`)

## Delta Lake connector

* Add support for the `unregister_table` procedure. ({issue}`15784`)
* Add support for inserting into tables that have `CHECK` constraints. ({issue}`15396`)
* Add support for writing to the [change data feed](https://docs.delta.io/2.0.0/delta-change-data-feed.html).
  This can be enabled with the `delta.enableChangeDataFeed` table property. ({issue}`15453`)
* Add a `$history` system table which can be queried to inspect Delta Lake table
  history. ({issue}`15683`)
* Improve performance of reading decimal types from Parquet files. ({issue}`15713`)
* Improve performance of reading numeric types from Parquet files. ({issue}`15850`)
* Improve performance of reading string types from Parquet files. ({issue}`15897`, {issue}`15923`)
* Improve performance of reading timestamp and boolean types from Parquet files. ({issue}`15954`)
* Improve query performance on tables created by Trino with `CREATE TABLE AS`. ({issue}`15878`)
* Remove support for the legacy Parquet writer. ({issue}`15436`)
* Fix query failure when reading Parquet files written by Apache Impala. ({issue}`15942`)
* Fix listing relations failure when a Glue table has no table type set. ({issue}`15909`)

## Hive connector

* Reduce query latency. ({issue}`15811`)
* Improve performance of reading decimal types from Parquet files. ({issue}`15713`)
* Improve performance of reading numeric types from Parquet files. ({issue}`15850`)
* Improve performance of reading string types from Parquet files. ({issue}`15897`, {issue}`15923`)
* Improve performance of reading timestamp and boolean types from Parquet files. ({issue}`15954`)
* Improve performance of predicate pushdown to partitioned columns in tables
  with a high number of partitions. ({issue}`16113`)
* Reduce server errors in high-load scenarios. This can be enabled with the
  `hive.s3.connect-ttl` configuration property. ({issue}`16005`)
* Allow setting the `hive.max-partitions-per-scan` configuration property to a
  value lower than the value set in `hive.max-partitions-for-eager-load`. ({issue}`16111`)
* Fix query failure when reading Parquet files written by Apache Impala. ({issue}`15942`)
* Fix listing relations failure when a Glue table has no table type set. ({issue}`15909`)

## Hudi connector

* Improve performance of reading decimal types from Parquet files. ({issue}`15713`)
* Improve performance of reading numeric types from Parquet files. ({issue}`15850`)
* Improve performance of reading string types from Parquet files. ({issue}`15897`, {issue}`15923`)
* Improve performance of reading timestamp and boolean types from Parquet files. ({issue}`15954`)
* Fix query failure when reading Parquet files written by Apache Impala. ({issue}`15942`)

## Iceberg connector

* Add support for the `unregister_table` procedure. ({issue}`15784`)
* Add support for `register_table` procedures in the JDBC catalog. ({issue}`15853`)
* Add support for specifying a user and password when connecting to the JDBC
  catalog via the `iceberg.jdbc-catalog.connection-user` and
  `iceberg.jdbc-catalog.connection-password` configuration properties. ({issue}`16040`)
* Add support for compacting manifests asynchronously, which can be enabled by
  setting the `iceberg.merge_manifests_on_write` session property to `false`. ({issue}`14822`)
* Improve performance of `DROP TABLE`. ({issue}`15981`)
* Improve performance of reading [position delete files](https://iceberg.apache.org/spec/#position-delete-files)
  with ORC data ({issue}`15969`).
* Improve performance of reading decimal columns from Parquet files. ({issue}`15713`)
* Improve performance of reading numeric types from Parquet files. ({issue}`15850`)
* Improve performance of reading string types from Parquet files. ({issue}`15897`, {issue}`15923`)
* Improve performance of reading timestamp and boolean types from Parquet files. ({issue}`15954`)
* Prevent creating a table when the specified schema does not exist. ({issue}`15779`)
* Fix query failure when reading Parquet files written by Apache Impala. ({issue}`15942`)
* Fix listing relations failure when a Glue table has no table type set. ({issue}`15909`)
* Fix failure when encountering access denied exceptions while listing
  materialized views in the Glue metastore. ({issue}`15893`)

## Kudu connector

* Fix authentication failure when Kerberos tickets expire. ({issue}`14372`)

## Memory connector

* Fix potential failure when reading table column metadata with concurrent
  `CREATE TABLE` or `DROP TABLE` operations. ({issue}`16062`)

## MongoDB connector

* Add support for changing column types. ({issue}`15515`)

## MySQL connector

* Fix potential failure when `zeroDateTimeBehavior` is set to `convertToNull`. ({issue}`16027`)
