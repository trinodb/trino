# Release 464 (30 Oct 2024)

## General

* {{breaking}} Require JDK 23 to run Trino, including updated [](jvm-config). ({issue}`21316`)
* Add the [](/connector/faker) for easy generation of data. ({issue}`23691`)
* Add the [](/connector/vertica). ({issue}`23948`)
* Rename the
  `fault-tolerant-execution-eager-speculative-tasks-node_memory-overcommit`
  configuration property to
  `fault-tolerant-execution-eager-speculative-tasks-node-memory-overcommit`.
  ({issue}`23876`)  

## Accumulo connector

* {{breaking}} Remove the Accumulo connector. ({issue}`23792`)  

## BigQuery connector

* Fix incorrect results when reading array columns and
  `bigquery.arrow-serialization.enabled` is set to true. ({issue}`23982`)

## Delta Lake connector

* Fix failure of S3 file listing of buckets that enforce [requester
  pays](https://docs.aws.amazon.com/AmazonS3/latest/userguide/RequesterPaysBuckets.html).
  ({issue}`23906`)

## Hive connector

* Use the `hive.metastore.partition-batch-size.max` catalog configuration
  property value in the `sync_partition_metadata` procedure. Change the default
  batch size from 1000 to 100. ({issue}`23895`)
* Fix failure of S3 file listing of buckets that enforce [requester
  pays](https://docs.aws.amazon.com/AmazonS3/latest/userguide/RequesterPaysBuckets.html).
  ({issue}`23906`)

## Hudi connector

* Fix failure of S3 file listing of buckets that enforce [requester
  pays](https://docs.aws.amazon.com/AmazonS3/latest/userguide/RequesterPaysBuckets.html).
  ({issue}`23906`)

## Iceberg connector

* Improve performance of `OPTIMIZE` on large partitioned tables. ({issue}`10785`)
* Rename the `iceberg.expire_snapshots.min-retention` configuration property to
  `iceberg.expire-snapshots.min-retention`. ({issue}`23876`)
* Rename the `iceberg.remove_orphan_files.min-retention` configuration property
  to `iceberg.remove-orphan-files.min-retention`. ({issue}`23876`)
* Fix failure of S3 file listing of buckets that enforce [requester
  pays](https://docs.aws.amazon.com/AmazonS3/latest/userguide/RequesterPaysBuckets.html).
  ({issue}`23906`)
* Fix incorrect column constraints when using the `migrate` procedure on tables
  that contain `NULL` values. ({issue}`23928`)

## Phoenix connector

* {{breaking}} Require JVM configuration to allow the Java security manager. ({issue}`24207`)