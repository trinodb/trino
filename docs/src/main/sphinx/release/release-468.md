# Release 468 (11 Dec 2024)

## General

* Add experimental support for Python user-defined functions. ({issue}`24378`)

## Hive connector

* Deactivate bucket execution when not useful in query processing. ({issue}`23432`)
* Enable mismatched bucket execution optimization by default. This can be
  disabled with `hive.optimize-mismatched-bucket-count` configuration property,
  and the `optimize_mismatched_bucket_count` session property. ({issue}`23432`)

## Iceberg connector

* Add bucketed execution to improve performance when running a join or
  aggregation on a bucketed table. This can be deactivated with
  `iceberg.bucket-execution` configuration property, and the
  `bucket_execution_enabled` session property. ({issue}`23432`)
* Deprecate the `iceberg.materialized-views.storage-schema` configuration
  property. ({issue}`24398`)  
* {{breaking}} Rename the `partitions` column in the `$manifests` metadata table
  to `partition_summaries`. ({issue}`24103`)

## PostgreSQL connector

* Add non-transactional support for [MERGE statements](/sql/merge). ({issue}`23034`)

## SPI

* Add partitioning push down, which a connector can use to activate optional
  partitioning, or choose between multiple partitioning strategies. ({issue}`23432`)
