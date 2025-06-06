# Release 476 (5 Jun 2025)

## General

* Add support for comparing values of `geometry` type. ({issue}`25225`)
* {{breaking}} Require JDK 24 to run Trino. ({issue}`23498`)
* Allow configuring `query.max-memory-per-node` and `memory.heap-headroom-per-node` 
  relative to maximum heap size. ({issue}`25843`)
* Add feature to deactivate the automated database schema migration for the database 
  backend for resource groups with the property `resource-groups.db-migrations-enabled`. ({issue)`25451`)
* Make soft memory limit optional in resource groups. ({issue}`25916`)
* Remove the [](/develop/example-http) from the tar.gz archive and the Docker container.  ({issue}`25128`)
* Fix rare bug when server can hang under load. ({issue}`25816`)
* Fix regression introduce in Trino 474 that prevented graceful shutdown from working. ({issue}`25690`)
* Fix potential query failure when the `fault_tolerant_execution_runtime_adaptive_partitioning_enabled` 
  session property is set to `true`. ({issue}`25870`)
* Fix failure for queries involving casts with `row` types. ({issue}`25864`)
* Fix query failures when dynamic catalog names contain mixed case letters. ({issue}`25701`)
* Improve retry logic for S3 operations in file system exchange. ({issue}`25908`)
* Fix query failures when the session catalog or schema names provided by clients contain 
  capital letters. ({issue}`25903`)

## BigQuery connector

* {{breaking}} Require the `--sun-misc-unsafe-memory-access=allow` JVM configuration 
  option to run Trino with the connector. ({issue}`25669`)

## ClickHouse connector

* Improve performance of selective joins for federated queries. ({issue}`25123`)

## Delta Lake connector

* Add support for the `FOR TIMESTAMP AS OF` clause. ({issue}`21024`)
* Add support for user-assigned managed identity authentication for AzureFS. ({issue}`23447`)
* Add signer type support to the native S3 filesystem. ({issue}`25820`)
* Improve compatibility with S3-compliant storage systems. ({issue}`25791`)
* Improve query planning performance. ({issue}`24570`)
* Improve performance when reading tables. ({issue}`25826`)
* Reduce S3 throttling failures. ({issue}`25781`)
* Fix failure when reading `variant` type column after executing `optimize` 
  procedure. ({issue}`25666`)
* Fix query failures when attempting to read `date` columns stored as integer
  values in Parquet files. ({issue}`25667`)
* Fix failure when querying views without [StorageDescriptor](https://docs.aws.amazon.com/glue/latest/webapi/API_StorageDescriptor.html)
  on Glue. ({issue}`25894`)
* Fix skipping statistics computation on all columns when only some column types
  don't support statistics. ({issue}`24487`)

## Druid connector

* Improve performance of selective joins for federated queries. ({issue}`25123`)

## DuckDB connector

* Improve performance of selective joins for federated queries. ({issue}`25123`)

## Exasol connector

* Improve performance of selective joins for federated queries. ({issue}`25123`)

## Google Sheets connector

* Add support for authentication using delegated user credentials with the 
  `gsheets.delegated-user-email` configuration property. ({issue}`25746`)
 
## Hive connector

* Add support for excluding certain tables from the directory listing cache 
  with the `hive.file-status-cache.excluded-tables` configuration property. ({issue}`25715`)
* Allow selecting the AWS signing protocol to use when authenticating S3 requests. ({issue}`25820`)
* Improve compatibility with S3-compliant storage systems. ({issue}`25791`)
* Add support for user-assigned managed identity authentication for AzureFS. ({issue}`23447`)
* Improve robustness of the OpenX JSON reader when parsing timestamp values. Previously,
  only timestamps with a space separator between date and time were supported. ({issue}`25792`)
* Improve metadata reading performance by optimizing directory listing cache behavior. ({issue}`25749`)
* Fix query failures with `HIVE_CANNOT_OPEN_SPLIT` error when reading ORC files with a large row count. ({issue}`25634`)
* Reduce S3 throttling failures. ({issue}`25781`)
* Fix query failures when attempting to read `date` columns stored as integer
  values in Parquet files. ({issue}`25667`)
* Fix failure when querying views without [StorageDescriptor](https://docs.aws.amazon.com/glue/latest/webapi/API_StorageDescriptor.html) 
  on Glue. ({issue}`25894`)

## Hudi connector

* Add support for user-assigned managed identity authentication for AzureFS. ({issue}`23447`)
* Fix query failures when attempting to read `date` columns stored as integer
  values in Parquet files. ({issue}`25667`)

## Iceberg connector

* Add support for user-assigned managed identity authentication for AzureFS. ({issue}`23447`)
* Add signer type support to the native S3 filesystem. ({issue}`25820`)
* Add the `added_delete_files_count`, `existing_delete_files_count`, `deleted_delete_files_count`, and
  `reference_snapshot_id` columns to `$all_manifests` metadata tables. ({issue}`25867`)
* Improve compatibility with S3-compliant storage systems. ({issue}`25791`)
* Show detailed metrics from splits generation in output of `EXPLAIN ANALYZE VERBOSE`. ({issue}`25770`)
* Add the `max_partitions_per_writer` catalog session property, which corresponds to the 
  `iceberg.max-partitions-per-writer` configuration property. ({issue}`25662`)
* Improve query planning performance when reading from materialized views. ({issue}`24734`)
* Prevent rare failure when `iceberg.bucket-execution` is enabled. ({issue}`25125`)
* Fix query failures with `HIVE_CANNOT_OPEN_SPLIT` error when reading ORC files with a large row count. ({issue}`25634`)
* Reduce S3 throttling failures. ({issue}`25781`)
* Fix query timeout errors due to concurrent writes on tables with large number of 
  manifest files. ({issue}`24751`)
* Fix query failures when attempting to read `date` columns stored as integer
  values in Parquet files. ({issue}`25667`)
* Fix failure when querying views without [StorageDescriptor](https://docs.aws.amazon.com/glue/latest/webapi/API_StorageDescriptor.html)
  on Glue. ({issue}`25894`)

## Ignite connector

* Improve performance of selective joins for federated queries. ({issue}`25123`)

## Kafka event Listener

* {{breaking}} Remove the `kafka-event-listener.client-config-overrides` configuration
  property. To configure the Kafka client for the event listener, specify the configuration
  in a separate file and set `kafka-event-listener.config.resources` to the path to the file. ({issue}`25553`)

## MariaDB connector

* Improve performance of selective joins for federated queries. ({issue}`25123`)

## Memory connector

* Fix failures when deleting rows from a table. ({issue}`25670`)

## MySQL connector

* Improve performance of selective joins for federated queries. ({issue}`25123`)

## Oracle connector

* Improve performance of selective joins for federated queries. ({issue}`25123`)
* Improve performance of listing table columns. ({issue}`25231`)

## PostgreSQL connector

* Improve performance of selective joins for federated queries. ({issue}`25123`)

## Redshift connector

* Improve performance of selective joins for federated queries. ({issue}`25123`)

## SingleStore connector

* Improve performance of selective joins for federated queries. ({issue}`25123`)

## Snowflake connector

* Improve performance of selective joins for federated queries. ({issue}`25123`)
* {{breaking}} Require the `--sun-misc-unsafe-memory-access=allow` JVM configuration
  option to run Trino with the connector. ({issue}`25669`)

## SQL Server connector

* Improve performance of selective joins for federated queries. ({issue}`25123`)

## Vertica connector

* Improve performance of selective joins for federated queries. ({issue}`25123`)

## SPI

* Return an estimate of the full data size of the block with `getSizeInBytes()`. ({issue}`25256`)
