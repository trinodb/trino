# Release 455 (29 Aug 2024)

## General

* Add query starting time in `QueryStatistics` in all [](admin-event-listeners).
  ({issue}`23113`)
* Add JMX metrics for the bean
  `trino.execution.executor.timesharing:name=TimeSharingTaskExecutor` replacing
  metrics previously found in `trino.execution.executor:name=TaskExecutor`.
  ({issue}`22914`)
* Add support S3 file system encryption with fault-tolerant execution mode. ({issue}`22529`)
* Fix memory tracking issue for aggregations that could cause worker crashes
  with out-of-memory errors. ({issue}`23098`)

## Delta Lake connector

* Allow configuring endpoint for the native Azure filesystem. ({issue}`23071`)
* Improve stability for concurrent Glue connections. ({issue}`23039`)

## ClickHouse connector

* Add support for creating tables with the `MergeTree` engine without the
  `order_by` table property. ({issue}`23048`)

## Hive connector

* Allow configuring endpoint for the native Azure filesystem. ({issue}`23071`)
* Improve stability for concurrent Glue connections. ({issue}`23039`)
* Fix query failures when Parquet files contain column names that only differ in
  case. ({issue}`23050`)

## Hudi connector

* Allow configuring endpoint for the native Azure filesystem. ({issue}`23071`)

## Iceberg connector

* Allow configuring endpoint for the native Azure filesystem. ({issue}`23071`)
* Improve stability for concurrent Glue connections. ({issue}`23039`)
* Fix `$files` table not showing delete files with the Iceberg v2 format. ({issue}`16233`)

## OpenSearch connector

* Improve performance of queries that reference nested fields from OpenSearch
  documents. ({issue}`22646`)

## PostgreSQL

* Fix potential failure for pushdown of `euclidean_distance`, `cosine_distance`
  and `dot_product` functions. ({issue}`23152`)

## Prometheus connector

* Add support for the catalog session properties `query_chunk_size_duration` and
  `max_query_range_duration`. ({issue}`22319`)

## Redshift connector

* Release resources in Redshift promptly when a query is cancelled. ({issue}`22774`)
