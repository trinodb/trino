# Release 346 (10 Nov 2020)

## General

* Add support for `RANGE BETWEEN <value> PRECEDING AND <value> FOLLOWING` window frames. ({issue}`609`)
* Add support for window frames based on `GROUPS`. ({issue}`5713`)
* Add support for {func}`extract` with `TIMEZONE_HOUR` and `TIMEZONE_MINUTE` for `time with time zone` values. ({issue}`5668`)
* Add SQL syntax for `GRANT` and `REVOKE` on schema. This is not yet used by any connector. ({issue}`4396`)
* Add `ALTER TABLE ... SET AUTHORIZATION` syntax to allow changing the table owner. ({issue}`5717`)
* Make `EXPLAIN` more readable for queries containing `timestamp` or `timestamp with time zone`  constants. ({issue}`5683`)
* Improve performance for queries with inequality conditions. ({issue}`2674`)
* Improve performance of queries with uncorrelated `IN` clauses. ({issue}`5582`)
* Use consistent NaN behavior for {func}`least`, {func}`greatest`,
  {func}`array_min`, {func}`array_max`, {func}`min`, {func}`max`,
  {func}`min_by`, and {func}`max_by`.
  NaN is only returned when it is the only value (except for null
  which are ignored for aggregation functions). ({issue}`5851`)
* Restore previous null handling for {func}`least` and {func}`greatest`. ({issue}`5787`)
* Restore previous null handling for {func}`array_min` and {func}`array_max`. ({issue}`5787`)
* Remove configuration properties `arrayagg.implementation`,
  `multimapagg.implementation`, and `histogram.implementation`. ({issue}`4581`)
* Fix incorrect handling of negative offsets for the `time with time zone` type. ({issue}`5696`)
* Fix incorrect result when casting `time(p)` to `timestamp(p)` for precisions higher than 6. ({issue}`5736`)
* Fix incorrect query results when comparing a `timestamp` column with a `timestamp with time zone` constant. ({issue}`5685`)
* Fix improper table alias visibility for queries that select all fields. ({issue}`5660`)
* Fix failure when query parameter appears in a lambda expression. ({issue}`5640`)
* Fix failure for queries containing `DISTINCT *` and fully-qualified column names in the `ORDER BY` clause. ({issue}`5647`)
* Fix planning failure for certain queries involving `INNER JOIN`, `GROUP BY` and correlated subqueries. ({issue}`5846`)
* Fix recording of query completion event when query is aborted early. ({issue}`5815`)
* Fix exported JMX name for `QueryManager`. ({issue}`5702`)
* Fix failure when {func}`approx_distinct` is used with high precision `timestamp(p)`/`timestamp(p) with time zone`/`time(p) with time zone`
  data types. ({issue}`5392`)

## Web UI

* Fix "Capture Snapshot" button on the Worker page. ({issue}`5759`)

## JDBC driver

* Support number accessor methods like `ResultSet.getLong()` or `ResultSet.getDouble()`
  on `decimal` values, as well as `char` or `varchar` values that can be unambiguously interpreted as numbers. ({issue}`5509`)
* Add `SSLVerification` JDBC connection parameter that allows configuring SSL verification. ({issue}`5610`)
* Remove legacy `useSessionTimeZone` JDBC connection parameter. ({issue}`4521`)
* Implement `ResultSet.getRow()`. ({issue}`5769`)

## Server RPM

* Remove leftover empty directories after RPM uninstall. ({issue}`5782`)

## BigQuery connector

* Fix issue when query could return invalid results if some column references were pruned out during query optimization. ({issue}`5618`)

## Cassandra connector

* Improve performance of `INSERT` queries with batch statement. The batch size can be configured via the `cassandra.batch-size`
  configuration property. ({issue}`5047`)

## Elasticsearch connector

* Fix failure when index mappings do not contain a `properties` section. ({issue}`5807`)

## Hive connector

* Add support for `ALTER TABLE ... SET AUTHORIZATION` SQL syntax to change the table owner. ({issue}`5717`)
* Add support for writing timestamps with microsecond or nanosecond precision, in addition to milliseconds. ({issue}`5283`)
* Export JMX statistics for Glue metastore client request metrics. ({issue}`5693`)
* Collect column statistics during `ANALYZE` and when data is inserted to table for columns of `timestamp(p)`
  when precision is greater than 3. ({issue}`5392`)
* Improve query performance by adding support for dynamic bucket pruning. ({issue}`5634`)
* Remove deprecated `parquet.fail-on-corrupted-statistics` (previously known as `hive.parquet.fail-on-corrupted-statistics`).
  A new configuration property, `parquet.ignore-statistics`, can be used to deal with Parquet files with incorrect metadata.  ({issue}`3077`)
* Do not write min/max statistics for `timestamp` columns. ({issue}`5858`)
* If multiple metastore URIs are defined via `hive.metastore.uri`, prefer connecting to one which was seen operational most recently.
  This prevents query failures when one or more metastores are misbehaving. ({issue}`5795`)
* Fix Hive view access when catalog name is other than `hive`. ({issue}`5785`)
* Fix failure when the declared length of a `varchar(n)` column in the partition schema differs from the table schema. ({issue}`5484`)
* Fix Glue metastore pushdown for complex expressions. ({issue}`5698`)

## Iceberg connector

* Add support for materialized views. ({issue}`4832`)
* Remove deprecated `parquet.fail-on-corrupted-statistics` (previously known as `hive.parquet.fail-on-corrupted-statistics`).
  A new configuration property, `parquet.ignore-statistics`, can be used to deal with Parquet files with incorrect metadata.  ({issue}`3077`)

## Kafka connector

* Fix incorrect column comment. ({issue}`5751`)

## Kudu connector

* Improve performance of queries having only `LIMIT` clause. ({issue}`3691`)

## MySQL connector

* Improve performance for queries containing a predicate on a `varbinary` column. ({issue}`5672`)

## Oracle connector

* Add support for setting column comments. ({issue}`5399`)
* Allow enabling remarks reporting via `oracle.remarks-reporting.enabled` configuration property. ({issue}`5720`)

## PostgreSQL connector

* Improve performance of queries comparing a `timestamp` column with a `timestamp with time zone` constants
  for `timestamp with time zone` precision higher than 3. ({issue}`5543`)

## Other connectors

* Improve performance of queries with `DISTINCT` or `LIMIT`, or with `GROUP BY` and no aggregate functions and `LIMIT`,
  when the computation can be pushed down to the underlying database for the PostgreSQL, MySQL, Oracle, Redshift and
  SQL Server connectors. ({issue}`5522`)

## SPI

* Fix propagation of connector session properties to `ConnectorNodePartitioningProvider`. ({issue}`5690`)
* Add user groups to query events. ({issue}`5643`)
* Add planning time to query completed event. ({issue}`5643`)
