# Release 475 (23 Apr 2025)

## General

* Add support for the `CORRESPONDING` clause in set operations. ({issue}`25260`)
* Add support for the `AUTO` grouping set that includes all non-aggregated columns 
  in the `SELECT` clause. ({issue}`18390`)
* Publish additional metrics for input tables in event listener. ({issue}`25475`)
* Expose dynamic filter statistics in the `QueryCompletedEvent`. ({issue}`25575`)
* Improve scalability of inline data encoding in the spooling client protocol. ({issue}`25439`)
* Improve performance of queries involving the `exclude_columns` table function. ({issue}`25117`)
* Disallow dropping the `system` catalog. ({issue}`24745`)
* Fix occasional query failures when [adaptive planning](/optimizer/adaptive-plan-optimizations) is enabled. ({issue}`25411`)
* Fix incorrect results when using window functions with `DISTINCT`. ({issue}`25434`)
* Fix query failures with `EXCEEDED_LOCAL_MEMORY_LIMIT` errors due to incorrect memory accounting. ({issue}`25600`)
* Properly handle inline session properties for `EXPLAIN` queries. ({issue}`25496`)

## Security

* Fix incorrect access denial for access control with impersonation when access is granted via the role. ({issue}`25166`)

## JDBC driver

* Avoid query cancellation when the client is fetching results. ({issue}`25267`)

## CLI

* Avoid query cancellation when the client is fetching results. ({issue}`25267`)

## Clickhouse connector

* Add support for Clickhouse's `bool` type. ({issue}`25130`)

## BigQuery connector

* Add support for limiting the max parallelism with the `bigquery.max-parallelism` configuration property. ({issue}`25422`)
* Fix queries getting stuck when reading large tables. ({issue}`25423`)

## Delta Lake connector

* Allow cross-region data retrieval when using the S3 native filesystem. ({issue}`25200`)
* Add support for all storage classes when using the S3 native filesystem for writes. ({issue}`25435`)
* Improve performance when filtering on `$path`, `$file_modified_time` or `$file_size` columns. ({issue}`25369`)
* Improve performance of scans on Delta Lake tables with v2 checkpoints. ({issue}`25469`)

## Hive connector

* Allow cross-region data retrieval when using the S3 native filesystem. ({issue}`25200`)
* Add support for all storage classes when using the S3 native filesystem for writes. ({issue}`25435`)
* Add support for showing column comments on Hive views. ({issue}`23845`)
* Add support for multiple predicates on partition projection columns with [injected types](https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html#partition-projection-injected-type). ({issue}`17641`)
* Fix potential failures or incorrect results when querying partitioned tables using the OpenX JSON SerDe. ({issue}`25444`)
* Ensure Hive metastore locks are released if a failure occurs during lock acquisition. ({issue}`25380`)
* Rename `hive.s3.storage-class-filter` to `hive.s3-glacier-filter` to better reflect its purpose. ({issue}`25633`)
* Fix incorrect results when reading timestamp values with leading or trailing spaces using the Regex and 
  OpenX JSON table deserializers. ({issue}`25442`)
* Fix potential performance regression when reading ORC data. ({issue}`25617`)

## Iceberg connector

* Allow cross-region data retrieval when using the S3 native filesystem. ({issue}`25200`)
* Add support for all storage classes when using the S3 native filesystem for writes. ({issue}`25435`)
* Add `system.iceberg_tables` system table to allow listing only Iceberg tables. ({issue}`25136`)
* Add support for IAM role authentication with the REST catalog. ({issue}`25002`)
* Fix potential failure when queries modify a table concurrently. ({issue}`25445`)
* Add support for returning column statistics for new columns in `$partitions` system table. ({issue}`25532`)
* Improve the `optimize_manifests` procedure to produce better organized manifests. ({issue}`25378`)
* Clean up old snapshots when refreshing a materialized view. ({issue}`25343`)
* Set Glue catalog ID when `hive.metastore.glue.catalogid` is configured. ({issue}`25511`)
* Fix failure when executing `migrate` on tables partitioned on columns with special characters. ({issue}`25106`)
* Fix `OPTIMIZE` failures due to commit conflicts with certain `DELETE` queries. ({issue}`25584`)
* Fix failure when analyzing a table without any snapshots. ({issue}`25563`)

## Memory connector

* Fix incorrect memory usage accounting for truncated tables. ({issue}`25564`)

## MySQL connector

* Add support for creating tables with a primary key. ({issue}`24930`)

## PostgreSQL connector

* Add support for MERGE when `retry_policy` is set to `TASK`. ({issue}`24467`)
* Add support for `array(uuid)` type. ({issue}`25557`)

## SQL Server connector

* Fix incorrect results for queries involving `LIKE` on columns with case-insensitive collations. ({issue}`25488`)

## SPI

* Remove the `LazyBlock` class. ({issue}`25255`)
