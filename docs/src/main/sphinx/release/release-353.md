# Release 353 (5 Mar 2021)

## General

* Add {doc}`/connector/clickhouse`. ({issue}`4500`)
* Extend support for correlated subqueries including `UNNEST`. ({issue}`6326`, {issue}`6925`, {issue}`6951`)
* Add {func}`to_geojson_geometry` and {func}`from_geojson_geometry` functions. ({issue}`6355`)
* Add support for values of any integral type (`tinyint`, `smallint`, `integer`, `bigint`, `decimal(p, 0)`)
  in window frame bound specification. ({issue}`6897`)
* Improve query planning time for queries containing `IN` predicates with many elements. ({issue}`7015`)
* Fix potential incorrect results when columns from `WITH` clause are exposed with aliases. ({issue}`6839`)
* Fix potential incorrect results for queries containing multiple `<` predicates. ({issue}`6896`)
* Always show `SECURITY` clause in `SHOW CREATE VIEW`. ({issue}`6913`)
* Fix reporting of column references for aliased tables in `QueryCompletionEvent`. ({issue}`6972`)
* Fix potential compiler failure when constructing an array with more than 128 elements. ({issue}`7014`)
* Fail `SHOW COLUMNS` when column metadata cannot be retrieved. ({issue}`6958`)
* Fix rendering of function references in `EXPLAIN` output. ({issue}`6703`)
* Fix planning failure when `WITH` clause contains hidden columns. ({issue}`6838`)
* Prevent client hangs when OAuth2 authentication fails. ({issue}`6659`)

## Server RPM

* Allow configuring process environment variables through `/etc/trino/env.sh`. ({issue}`6635`)

## BigQuery connector

* Add support for `CREATE TABLE` and `DROP TABLE` statements. ({issue}`3767`)
* Allow for case-insensitive identifiers matching via `bigquery.case-insensitive-name-matching` config property. ({issue}`6748`)

## Hive connector

* Add support for `current_user()` in Hive defined views. ({issue}`6720`)
* Add support for reading and writing column statistics from Glue metastore. ({issue}`6178`)
* Improve parallelism of bucketed tables inserts. Inserts into bucketed tables can now be parallelized
  within task using `task.writer-count` feature config. ({issue}`6924`, {issue}`6866`)
* Fix a failure when `INSERT` writes to a partition created by an earlier `INSERT` statement. ({issue}`6853`)
* Fix handling of folders created using the AWS S3 Console. ({issue}`6992`)
* Fix query failures on ``information_schema.views`` table when there are failures 
  translating hive view definitions. ({issue}`6370`)

## Iceberg connector

* Fix handling of folders created using the AWS S3 Console. ({issue}`6992`)
* Fix query failure when reading nested columns with field names that may 
  contain upper case characters. ({issue}`7180`)

## Kafka connector

* Fix failure when querying Schema Registry tables. ({issue}`6902`)
* Fix querying of Schema Registry tables with References in their schema. ({issue}`6907`)
* Fix listing of schema registry tables having ambiguous subject name in lower case. ({issue}`7048`)

## MySQL connector

* Fix failure when reading a `timestamp` or `datetime` value with more than 3 decimal digits 
  in the fractional seconds part. ({issue}`6852`)
* Fix incorrect predicate pushdown for `char` and `varchar` column with operators 
  like `<>`, `<`, `<=`, `>` and `>=` due different case sensitivity between Trino 
  and MySQL. ({issue}`6746`, {issue}`6671`)

## MemSQL connector

* Fix failure when reading a `timestamp` or `datetime` value with more than 3 decimal digits 
  of the second fraction. ({issue}`6852`)
* Fix incorrect predicate pushdown for `char` and `varchar` column with operators 
  like `<>`, `<`, `<=`, `>` and `>=` due different case sensitivity between Trino 
  and MemSQL. ({issue}`6746`, {issue}`6671`)

## Phoenix connector

* Add support for Phoenix 5.1. This can be used by setting `connector.name=phoenix5` in catalog
  configuration properties. ({issue}`6865`)
* Fix failure when query contains a `LIMIT` exceeding 2147483647. ({issue}`7169`)

## PostgreSQL connector

* Improve performance of queries with `ORDER BY ... LIMIT` clause, when the computation 
  can be pushed down to the underlying database. This can be enabled by setting `topn-pushdown.enabled`.
  Enabling this feature can currently result in incorrect query results when sorting
  on `char` or `varchar` columns. ({issue}`6847`)
* Fix incorrect predicate pushdown for `char` and `varchar` column with operators 
  like `<>`, `<`, `<=`, `>` and `>=` due different case collation between Trino 
  and PostgreSQL. ({issue}`3645`)

## Redshift connector

* Fix failure when reading a `timestamp` value with more than 3 decimal digits of
  the second fraction. ({issue}`6893`)

## SQL Server connector

* Abort queries on the SQL Server side when the Trino query is finished. ({issue}`6637`)
* Fix incorrect predicate pushdown for `char` and `varchar` column with operators 
  like `<>`, `<`, `<=`, `>` and `>=` due different case sensitivity between Trino 
  and SQL Server. ({issue}`6753`)

## Other connectors

* Reduce number of opened JDBC connections during planning for ClickHouse, Druid, MemSQL, MySQL,
  Oracle, Phoenix, Redshift, and SQL Server connectors. ({issue}`7069`)
* Add experimental support for join pushdown in PostgreSQL, MySQL, MemSQL, Oracle, and SQL Server connectors.
  It can be enabled with the `experimental.join-pushdown.enabled=true` catalog configuration property. ({issue}`6874`)

## SPI

* Fix lazy blocks to call listeners that are registered after the top level block is already loaded.
  Previously, such registered listeners were not called when the nested blocks were later loaded. ({issue}`6783`)
* Fix case where LazyBlock.getFullyLoadedBlock() would not load nested blocks 
  when the top level block was already loaded. ({issue}`6783`)
* Do not include coordinator node in the result of `ConnectorAwareNodeManager.getWorkerNodes()` 
  when `node-scheduler.include-coordinator` is false. ({issue}`7007`)
* The function name passed to `ConnectorMetadata.applyAggregation()` 
  is now the canonical function name. Previously, if query used function alias, the alias 
  name was passed. ({issue}`6189`)
* Add support for redirecting table scans to multiple tables that are unioned together. ({issue}`6679`)
* Change return type of `Range.intersect(Range)`. The method now 
  returns `Optional.empty()` instead of throwing when ranges do not overlap. ({issue}`6976`)
* Change signature of `ConnectorMetadata.applyJoin()` to have an additional `JoinStatistics` argument. ({issue}`7000`)
* Deprecate `io.trino.spi.predicate.Marker`.
