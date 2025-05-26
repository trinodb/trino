# Release 476 (27 May 2025)

## General

* Add support for comparing values of `geometry` type. ({issue}`25225`)
* Allow configuring `query.max-memory-per-node` and `memory.heap-headroom-per-node` relative to maximum heap size. ({issue}`25843`)
* Add feature to deactivate the automated database schema migration for the database backend for resource groups with the property `resource-groups.db-migrations-enabled`. ({issue)`25451`)
* Remove the [](/develop/example-http) from the tar.gz archive and the Docker container.  ({issue}`25128`)
* Fix rare bug when server can hang under load. ({issue}`25816`)
* Require JDK 24 to run Trino. ({issue}`23498`)
* Fix Regression: Graceful shutdown doesn't work. ({issue}`25690`)
* Fix potential query failure when `fault_tolerant_execution_runtime_adaptive_partitioning_enabled` is set to `true`. ({issue}`25870`)

## Security

## Web UI

## JDBC driver

## Docker image

## CLI

## BigQuery connector

* {breaking} Require `--sun-misc-unsafe-memory-access=allow` while running Trino server. ({issue}`25669`)

## Blackhole connector

## Cassandra connector

## ClickHouse connector

* Improve performance of selective joins for federated queries. ({issue}`25123`)

## Delta Lake connector

* Add support for `FOR TIMESTAMP AS OF` clause. ({issue}`21024`)
* Allow configuring User-ManagedIdentity in AzureFS. ({issue}`23447`)
* Add signer type to native S3 filesystem. ({issue}`25820`)
* Avoid skipping statistics computation for all columns when some column types don't support statistics. ({issue}`24487`)
* Improve compatibility with S3-compliant storage systems ({issue}`25791`)
* Improve query planning performance on delta tables. ({issue}`24570`)
* Improve performance when reading tables. ({issue}`25826`)
* Reduce S3 throttling failures by increasing streaming upload part size and reducing number of requests. ({issue}`25781`)
* Fix failure when reading `variant` type column after executing `optimize` procedure. ({issue}`25666`)
* Fix query failures when attempting to read parquet data with integer logical annotation as a `date` column. ({issue}`25667`)

## Druid connector

* Improve performance of selective joins for federated queries. ({issue}`25123`)

## DuckDB connector

* Improve performance of selective joins for federated queries. ({issue}`25123`)

## Elasticsearch connector

## Exasol connector

* Improve performance of selective joins for federated queries. ({issue}`25123`)

## Faker connector

## Google Sheets connector

* Add support for authentication using delegated user credentials using the `gsheets.delegated-user-email` config property. ({issue}`25746`)

## Hive connector

* Add support for excluding certain tables from the directory listing cache, controlled by a new configuration option `hive.file-status-cache.excluded-tables`.  ({issue}`25715`)
* Add signer type to native S3 filesystem. ({issue}`25820`)
* Improve compatibility with S3-compliant storage systems ({issue}`25791`)
* Allow configuring User-ManagedIdentity in AzureFS. ({issue}`23447`)
* Improve the directory listing cache invalidation behavior to avoid invalidating the entire cache when dropping a partitioned table. ({issue}`25749`)
* Fix query failure when reading ORC files with a large row count. ({issue}`25634`)
* Reduce S3 throttling failures by increasing streaming upload part size and reducing number of requests. ({issue}`25781`)
* Fix query failures when attempting to read parquet data with integer logical annotation as a `date` column. ({issue}`25667`)

## Hudi connector

* Allow configuring User-ManagedIdentity in AzureFS. ({issue}`23447`)
* Fix query failures when attempting to read parquet data with integer logical annotation as a `date` column. ({issue}`25667`)

## Iceberg connector

* Allow configuring User-ManagedIdentity in AzureFS. ({issue}`23447`)
* Add signer type to native S3 filesystem. ({issue}`25820`)
* Improve compatibility with S3-compliant storage systems ({issue}`25791`)
* Show detailed metrics from splits generation in output of EXPLAIN ANALYZE VERBOSE. ({issue}`25770`)
* Add `max_partitions_per_writer` session property, which corresponds to the `iceberg.max-partitions-per-writer` configuration property. ({issue}`25662`)
* Improve query planning performance when reading from materialized views. ({issue}`24734`)
* Fix rare failure when `iceberg.bucket-execution` is enabled. ({issue}`25125`)
* Fix query failure when reading ORC files with a large row count. ({issue}`25634`)
* Reduce S3 throttling failures by increasing streaming upload part size and reducing number of requests. ({issue}`25781`)
* Fix query timeout errors due to concurrent writes on tables with large number of manifest files. ({issue}`24751`)
* Fix query failures when attempting to read parquet data with integer logical annotation as a `date` column. ({issue}`25667`)

## Ignite connector

* Improve performance of selective joins for federated queries. ({issue}`25123`)

## JMX connector

## Kafka connector

## Kafka Event Listener

* `kafka-event-listener.client-config-overrides` is removed. To configure the Kafka client for the event listener specify the configuration in a separate file and set `kafka-event-listener.config.resources` to the path to the file. ({issue}`25553`)

## Loki connector

## MariaDB connector

* Improve performance of selective joins for federated queries. ({issue}`25123`)

## Memory connector

* Fix failures when deleting rows from memory connector ({issue}`25670`)

## MongoDB connector

## MySQL connector

* Improve performance of selective joins for federated queries. ({issue}`25123`)

## OpenSearch connector

## Oracle connector

* Improve performance of selective joins for federated queries. ({issue}`25123`)
* Improve performance of listing table columns. ({issue}`25231`)

## Pinot connector

## PostgreSQL connector

* Improve performance of selective joins for federated queries. ({issue}`25123`)

## Prometheus connector

## Redis connector

## Redshift connector

* Improve performance of selective joins for federated queries. ({issue}`25123`)

## SingleStore connector

* Improve performance of selective joins for federated queries. ({issue}`25123`)

## Snowflake connector

* Improve performance of selective joins for federated queries. ({issue}`25123`)
* {{breaking}} Require `--sun-misc-unsafe-memory-access=allow` while running Trino server. ({issue}`25669`)

## SQL Server connector

* Improve performance of selective joins for federated queries. ({issue}`25123`)

## TPC-H connector

## TPC-DS connector

## Vertica connector

* Improve performance of selective joins for federated queries. ({issue}`25123`)

## SPI

* Block `getSizeInBytes` now returns an estimate of the full data size of the block. ({issue}`25256`)
