# Release 366 (14 Dec 2021)

## General

* Add support for automatic query retries. This feature can be turned on by setting the `retry-policy` 
  config property or the `retry_policy` session property to `retry`. ({issue}`9361`)
* Add CREATE privilege kind to SQL grammar. Note that this permission is not used by any
  existing security systems, but is available for plugins. ({issue}`10206`)
* Add support for `DENY` statement in the engine. Note that this statement is not supported by any
  existing security systems, but is available for plugins. ({issue}`10205`)
* Reduce lock contention during query execution. ({issue}`10246`, {issue}`10239`)
* Improve query performance through optimizations to in-memory representations. ({issue}`10225`)
* Reduce query latency for contended clusters or complex queries with multiple stages. ({issue}`10249`)
* Fix incorrect results or failure when casting numeric values to `varchar(n)` type. ({issue}`552`)
* Remove support for spilling aggregations containing `ORDER BY` or `DISTINCT` clauses and associated
  configuration properties `spill-distincting-aggregations-enabled`, `spill-ordering-aggregations-enabled`.
  ({issue}`10183`)

## Elasticsearch connector

* Read extended metadata from the `_meta.trino` index mapping attribute. `_meta.presto` is still
  supported for backward compatibility. ({issue}`8383`)

## Hive connector

* Add support for redirects from Hive to Iceberg. This can be configured with `hive.iceberg-catalog-name`
  catalog configuration property. ({issue}`10173`)
* Improve performance of uploading data into tables that use S3 filesystem. ({issue}`10180`)

## Iceberg connector

* Fix incorrect query results for tables partitioned on columns of type `binary`. ({issue}`9755`)

## MemSQL connector

* Fix incorrect result when a `date` value is older than or equal to `1582-10-14`. ({issue}`10054`)

## MySQL connector

* Fix incorrect result when a `date` value is older than or equal to `1582-10-14`. ({issue}`10054`)

## Phoenix connector

* Avoid running into out of memory errors with certain types of queries. ({issue}`10143`)

## Prometheus connector

* Support configuring a read timeout via the `prometheus.read-timeout` config property. ({issue}`10101`)

## PostgreSQL connector

* Fix incorrect result when a `date` value is older than or equal to `1582-10-14`. ({issue}`10054`)
