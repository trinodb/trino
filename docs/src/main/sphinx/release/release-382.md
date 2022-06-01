# Release 382 (25 May 2022)

## General

* Add support for fault-tolerant execution with [exchange spooling on Google Cloud Storage](fte-exchange-gcs). ({issue}`12360`)
* Drop support for exchange spooling on S3 with for the legacy schemes `s3n://` and `s3a://`. ({issue}`12360`)
* Improve join performance when one side of the join is small. ({issue}`12257`)
* Fix potential query failures due to `EXCEEDED_TASK_DESCRIPTOR_STORAGE_CAPACITY`
  errors with task-based fault-tolerant execution. ({issue}`12478`)

## BigQuery connector

* Add support for [using BigQuery's cached query results](https://cloud.google.com/bigquery/docs/cached-results).
  This can be enabled using the `bigquery.query-results-cache.enabled` configuration property. ({issue}`12408`)
* Support reading wildcard tables. ({issue}`4124`)

## Delta Lake connector

* Improve performance of queries that include filters on columns of `timestamp with time zone` type. ({issue}`12007`)
* Add support for adding columns with `ALTER TABLE`. ({issue}`12371`)

## Hive connector

* Add support for disabling partition caching in the Hive metastore with the
  `hive.metastore-cache.cache-partitions` catalog configuration property. ({issue}`12343`)
* Fix potential query failure when metastore caching is enabled. ({issue}`12513`)
* Fix query failure when a transactional table contains a column named
  `operation`, `originalTransaction`, `bucket`, `rowId`, `row`, or
  `currentTransaction`. ({issue}`12401`)
* Fix `sync_partition_metadata` procedure failure when table has a large number of partitions. ({issue}`12525`)

## Iceberg connector

* Support updating Iceberg table partitioning using `ALTER TABLE ... SET PROPERTIES`. ({issue}`12174`)
* Improves the performance of queries using equality and `IN` predicates when
  reading ORC data that contains Bloom filters. ({issue}`11732`)
* Rename the `delete_orphan_files` table procedure to `remove_orphan_files`. ({issue}`12468`)
* Improve query performance of reads after `DELETE` removes all rows from a file. ({issue}`12197`)

## MySQL connector

* Improve `INSERT` performance. ({issue}`12411`)

## Oracle connector

* Improve `INSERT` performance when data includes `NULL` values. ({issue}`12400`)

## PostgreSQL connector

* Improve `INSERT` performance. ({issue}`12417`)

## Prometheus connector

* Add support for Basic authentication. ({issue}`12302`)

## SPI

* Change `ConnectorTableFunction` into an interface and add
  `AbstractConnectorTableFunction` class as the base implementation of table
  functions. ({issue}`12531`)
