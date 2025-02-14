# Release 468 (17 Dec 2024)

## General

* Add support for [](/udf/python). ({issue}`24378`)
* Add cluster overview to the [](/admin/preview-web-interface). ({issue}`23600`)
* Add new node states `DRAINING` and `DRAINED` to make it possible to reactivate
  a draining worker node. ({issue}`24444 `)

## BigQuery connector

* Improve performance when reading external
  [BigLake](https://cloud.google.com/bigquery/docs/biglake-intro) tables. ({issue}`21016`)

## Delta Lake connector

* {{breaking}} Reduce coordinator memory usage for the Delta table metadata
  cache and enable configuration `delta.metadata.cache-max-retained-size` to
  control memory usage. Remove the configuration property
  `delta.metadata.cache-size` and increase the default for
  `delta.metadata.cache-ttl` to `30m`. ({issue}`24432`)

## Hive connector

* Enable mismatched bucket execution optimization by default. This can be
  disabled with `hive.optimize-mismatched-bucket-count` configuration property
  and the `optimize_mismatched_bucket_count` session property. ({issue}`23432`)
* Improve performance by deactivating bucket execution when not useful in query
  processing. ({issue}`23432`)

## Iceberg connector

* Improve performance when running a join or aggregation on a bucketed table
  with bucketed execution. This can be deactivated with the
  `iceberg.bucket-execution` configuration property and the
  `bucket_execution_enabled` session property. ({issue}`23432`)
* Deprecate the `iceberg.materialized-views.storage-schema` configuration
  property. ({issue}`24398`)  
* {{breaking}} Rename the `partitions` column in the `$manifests` metadata table
  to `partition_summaries`. ({issue}`24103`)
* Avoid excessive resource usage on coordinator when reading Iceberg system
  tables. ({issue}`24396`)

## PostgreSQL connector

* Add support for non-transactional [MERGE statements](/sql/merge). ({issue}`23034`)

## SPI

* Add partitioning push down, which a connector can use to activate optional
  partitioning or choose between multiple partitioning strategies. ({issue}`23432`)
