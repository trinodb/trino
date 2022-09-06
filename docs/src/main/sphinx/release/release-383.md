# Release 383 (1 Jun 2022)

```{warning}
This release has a regression that may cause queries to fail.
```

## General

* Introduce `json_exists`, `json_query`, and `json_value` [JSON functions](/functions/json). ({issue}`9081`)
* Add AWS IAM role support for exchange spooling on S3. ({issue}`12444`)
* Improve query performance by reducing worker-to-worker communication overhead. ({issue}`11289`)
* Improve performance and reduce memory usage of queries that contain aggregations. ({issue}`12336`)
* Improve performance of correlated queries involving distinct aggregations. ({issue}`12564`)

## Web UI

* Clarify format of cumulative user memory on query details page. ({issue}`12596`)

## Accumulo connector

* Fail creating a new table if a table comment is specified. Previously, the
  comment was ignored. ({issue}`12452`)

## BigQuery connector

* Fail creating a new table if a table comment or a column comment is specified.
  Previously, the comment was ignored. ({issue}`12452`, {issue}`12574`)

## Cassandra connector

* Fail creating a new table if a table comment or a column comment is specified.
  Previously, the comment was ignored. ({issue}`12452`, {issue}`12574`)

## ClickHouse connector

* Fix incorrect results for certain aggregation queries when aggregations are
  pushed down to the underlying database. ({issue}`12598`)

## Delta Lake connector

* Add support for table comments during table creation.. ({issue}`12452`)
* Fix incorrect `table already exists` error caused by a client timeout when
  creating a new table. ({issue}`12300`)
* Fail creating a new table if a column comment is specified. Previously, the
  comment was ignored. ({issue}`12574`)

## Iceberg connector

* Add support for v2 tables for the `optimize` table procedure. ({issue}`12351`)
* Rename `hive.target-max-file-size` to `iceberg.target-max-file-size` and
  `hive.delete-schema-locations-fallback` to `iceberg.delete-schema-locations-fallback`. ({issue}`12330`)

## Kudu connector

* Fail creating a new table if a table comment or a column comment is specified.
  Previously, the comment was ignored. ({issue}`12452`, {issue}`12574`)

## MariaDB connector

* Fix incorrect results for certain queries involving aggregations that are
  pushed down to the underlying database. ({issue}`12598`)
* Fail creating a new table if a column comment is specified. Previously, the
  comment was ignored. ({issue}`12574`)

## Memory connector

* Fail creating a new table if a table comment or a column comment is specified.
  Previously, the comment was ignored. ({issue}`12452`, {issue}`12574`)

## MySQL connector

* Fix incorrect results for certain aggregation queries when aggregations are
  pushed down to the underlying database. ({issue}`12598`)
* Fail creating a new table if a column comment is specified. Previously, the
  comment was ignored. ({issue}`12574`)

## Oracle connector

* Fail creating a new table if a table comment or a column comment is specified.
  Previously, the comment was ignored. ({issue}`12452`, {issue}`12574`)
* Fix incorrect results for certain aggregation queries when aggregations are
  pushed down to the underlying database. ({issue}`12598`)

## Phoenix connector

* Fail creating a new table if a table comment or a column comment is specified.
  Previously, the comment was ignored. ({issue}`12452`, {issue}`12574`)

## PostgreSQL connector

* Fail creating a new table if a table comment or a column comment is specified.
  Previously, the comment was ignored. ({issue}`12452`, {issue}`12574`)
* Fix incorrect results for certain aggregation queries when aggregations are
  pushed down to the underlying database. ({issue}`12598`)

## Raptor connector

* Fail creating a new table if a table comment or a column comment is specified.
  Previously, the comment was ignored. ({issue}`12452`, {issue}`12574`)

## Redshift connector

* Fail creating a new table if a table comment or a column comment is specified.
  Previously, the comment was ignored. ({issue}`12452`, {issue}`12574`)

## SingleStore (MemSQL) connector

* Fail creating a new table if a table comment or a column comment is specified.
  Previously, the comment was ignored. ({issue}`12452`, {issue}`12574`)
* Fix incorrect results for certain aggregation queries when aggregations are
  pushed down to the underlying database. ({issue}`12598`)

## SQL Server connector

* Fail creating a new table if a table comment or a column comment is specified.
  Previously, the comment was ignored. ({issue}`12452`, {issue}`12574`)
* Fix incorrect results for certain aggregation queries when aggregations are
  pushed down to the underlying database. ({issue}`12598`)

## SPI

* Allow limiting access to functions based on whether they are scalar,
  aggregation, window, or table functions. ({issue}`12544`)
