# Release 475 (16 Apr 2025)

## General

* Publish additional metrics for input tables in event listener. ({issue}`25475`)
* Expose dynamic filter statistics in the `QueryCompletedEvent`. ({issue}`25575`)
* Improve scalability of inline data encoding in the parallel client protocol. ({issue}`25439`)
* Improve performance of queries involving the `exclude_columns` table function. ({issue}`25117`)
* Disallow dropping the `system` catalog. ({issue}`24745`)
* Fix occasional query failures when adaptive planning is enabled. ({issue}`25411`)
* Fix incorrect results when using window functions with `DISTINCT`. ({issue}`25434`)
* Fix query failures with `EXCEEDED_LOCAL_MEMORY_LIMIT` errors due to incorrect memory accounting. ({issue}`25600`)
* Properly handle inline session properties for `EXPLAIN` queries. ({issue}`25496`)

## Security

* Fix impersonation access control when access is granted via the role. ({issue}`25166`)

## BigQuery connector

* Add support limiting the max parallelism via the `bigquery.max-parallelism` config property. ({issue}`25422`)
* Fix queries getting stuck when reading large tables. ({issue}`25423`)

## Delta Lake connector

* Improve performance when filtering on `$path`, `$file_modified_time` and `$file_size` columns. ({issue}`25369`)
* Improve performance of scans on delta lake tables with v2 checkpoints. ({issue}`25469`)

## Hive connector

* Add support for showing column comments on Hive views. ({issue}`23845`)
* Add support for multiple predicates on partition projection columns with [injected types](https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html#partition-projection-injected-type). ({issue}`17641`)
* Fix potential failures or incorrect results when querying partitioned tables using the OpenX JSON SerDe. ({issue}`25444`)
* Ensure Hive metastore locks are released if a failure occurs during lock acquisition. ({issue}`25380`)

## Iceberg connector

* Add `system.iceberg_tables` system table to allow listing only Iceberg tables. ({issue}`25136`)
* Add support for IAM role authentication REST catalog. ({issue}`25002`)
* Fix potential failure when queries modify a table concurrently. ({issue}`25445`)
* Add support for returning column statistics for new columns in `$partitions` system table. ({issue}`25532`)
* Optimize manifest files by the top-level partition using the `optimize_manifests` procedure. ({issue}`25378`)
* Clean up old snapshots when refreshing a materialized view. ({issue}`25343`)
* Set Glue catalog ID when `hive.metastore.glue.catalogid` is configured. ({issue}`25511`)
* Fix failure when executing `migrate` on tables partitioned on columns with special characters. ({issue}`25228`)

## Memory connector

* Fix incorrect memory usage accounting for truncated tables. ({issue}`25564`)

## MySQL connector

* Add support for creating tables with a primary key. ({issue}`24930`)

## PostgreSQL connector

* Support MERGE when `retry_policy` is set to `TASK`. ({issue}`24467`)

## SPI

* Remove LazyBlock. ({issue}`25255`)
