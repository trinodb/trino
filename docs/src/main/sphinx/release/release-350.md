# Release 350 (28 Dec 2020)

## General

* Add HTTP client JMX metrics. ({issue}`6453`)
* Improve query performance by reducing worker to worker communication overhead. ({issue}`6283`, {issue}`6349`)
* Improve performance of queries that contain `IS NOT DISTINCT FROM` join predicates. ({issue}`6404`)
* Fix failure when restricted columns have column masks. ({issue}`6017`)
* Fix failure when `try` expressions reference columns that contain `@` or `:` in their names. ({issue}`6380`)
* Fix memory management config handling to use `query.max-total-memory-per-node`
  rather than only using `query.max-memory-per-node` for both values. ({issue}`6349`)

## Web UI

* Fix truncation of query text in cluster overview page. ({issue}`6216`)

## JDBC driver

* Accept `java.time.OffsetTime` in `PreparedStatement.setObject(int, Object)`. ({issue}`6352`)
* Extend `PreparedStatement.setObject(int, Object, int)` to allow setting `time with time zone` and `timestamp with time zone`
  values with precision higher than nanoseconds. This can be done via providing a `String` value representing a valid SQL literal. ({issue}`6352`)

## BigQuery connector

* Fix incorrect results for `count(*)` queries with views. ({issue}`5635`)

## Cassandra connector

* Support `DELETE` statement with primary key or partition key. ({issue}`4059`)

## Elasticsearch connector

* Improve query analysis performance when Elasticsearch contains many index mappings. ({issue}`6368`)

## Kafka connector

* Support Kafka Schema Registry for Avro topics. ({issue}`6137`)

## SQL Server connector

* Add `data_compression` table property to control the target compression in SQL Server.
  The allowed values are `NONE`, `ROW` or `PAGE`. ({issue}`4693`)

## Other connectors

This change applies to the MySQL, Oracle, PostgreSQL, Redshift, and SQL Server connectors.

* Send shorter and potentially more performant queries to remote database when a Presto query has a `NOT IN`
  predicate eligible for pushdown into the connector. ({issue}`6075`)

## SPI

* Rename `LongTimeWithTimeZone.getPicoSeconds()` to `LongTimeWithTimeZone.getPicoseconds()`. ({issue}`6354`)
