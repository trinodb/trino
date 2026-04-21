# Release 469 (27 Jan 2025)

## General

* Add support for the `FIRST`, `AFTER`, and `LAST` clauses to `ALTER TABLE ...
  ADD COLUMN`. ({issue}`20091`)
* Add the {func}`ST_GeomFromKML` function. ({issue}`24297`)
* Allow configuring the spooling client protocol behaviour with session
  properties. ({issue}`24655`, {issue}`24757`)
* Improve stability of the cluster under load. ({issue}`24572`)
* Prevent planning failures resulting from join pushdown for modified tables. ({issue}`24447`)
* Fix parsing of negative hexadecimal, octal, and binary numeric literals. ({issue}`24601`)
* Fix failures with recursive delete operations on S3Express preventing usage
  for fault-tolerant execution. ({issue}`24763`)

## Web UI

* Add support for filtering queries by client tags. ({issue}`24494`)

## JDBC driver

* Add `planningTimeMillis`, `analysisTimeMillis`, `finishingTimeMillis`,
  `physicalInputBytes`, `physicalWrittenBytes`, `internalNetworkInputBytes` and
  `physicalInputTimeMillis` to `io.trino.jdbc.QueryStats`. ({issue}`24571`,
  {issue}`24604`)
* Improve the `Connection.isValid(int)` method so it validates the connection
  and credentials, and add the `validateConnection` connection property.
  ({issue}`24127`, {issue}`22684`)
* Prevent failures when using the spooling protocol with a cluster using its own
  certificate chain. ({issue}`24595`)
* Fix deserialization failures with `SetDigest`, `BingTile`, and `Color` types. ({issue}`24612`)

## CLI

* Prevent failures when using the spooling protocol with a cluster using its own
  certificate chain. ({issue}`24595`)
* Fix deserialization of `SetDigest`, `BingTile`, and `Color` types. ({issue}`24612`)

## BigQuery connector

* Allow configuration of the channel pool for gRPC communication with BigQuery. ({issue}`24638`)

## ClickHouse connector

* {{breaking}} Raise minimum required versions to ClickHouse 24.3 and Altinity
  22.3. ({issue}`24515`)
* Fix failure when updating values to `NULL`. ({issue}`24204`)

## Delta Lake connector

* Add support for SSE-C in S3 security mapping. ({issue}`24566`)
* Allow configuring the endpoint for the Google Storage file system with the
  `gcs.endpoint` property. ({issue}`24626`)
* Improve performance of reading from new Delta Lake table data by compressing
  files with `ZSTD` by default, instead of the previous `SNAPPY`.
  ({issue}`17426`)
* Improve performance of queries on tables with large transaction log JSON
  files. ({issue}`24491`)
* Improve performance of reading from Parquet files with a large number of row
  groups. ({issue}`24618`)
* Improve performance for the `OPTIMIZE` statement by enabling concurrent
  execution. ({issue}`16985`)
* Improve performance of reading from large files on S3. ({issue}`24521`)
* Correct catalog information in JMX metrics when using file system caching with
  multiple catalogs. ({issue}`24510`)
* Fix table read failures when using the Alluxio file system. ({issue}`23815`)
* Fix incorrect results when updating tables with deletion vectors enabled. ({issue}`24648`)
* Fix incorrect results when reading from tables with deletion vectors enabled. ({issue}`22972`)

## Elasticsearch connector

* Improve performance of queries that reference nested fields from Elasticsearch
  documents. ({issue}`23069`)

## Faker connector

* Add support for views. ({issue}`24242`)
* Support generating sequences. ({issue}`24590`)
* {{breaking}} Replace specifying constraints using `WHERE` clauses with the
  `min`, `max`, and `options` column properties. ({issue}`24147`)

## Hive connector

* Add support for SSE-C in S3 security mapping. ({issue}`24566`)
* Allow configuring the endpoint for the Google Storage file system with the
  `gcs.endpoint` property. ({issue}`24626`)
* Split AWS SDK client retry count metrics into separate client-level, logical
  retries and lower-level HTTP client retries. ({issue}`24606`)
* Improve performance of reading from Parquet files with a large number of row
  groups. ({issue}`24618`)
* Improve performance of reading from large files on S3. ({issue}`24521`)
* Correct catalog information in JMX metrics when using file system caching with
  multiple catalogs. ({issue}`24510`)
* Fix table read failures when using the Alluxio file system. ({issue}`23815`)
* Prevent writing of invalid data for NaN, Infinity, -Infinity values to JSON
  files. ({issue}`24558`)

## Hudi connector

* Add support for SSE-C in S3 security mapping. ({issue}`24566`)
* Allow configuring the endpoint for the Google Storage file system with the
  `gcs.endpoint` property. ({issue}`24626`)
* Improve performance of reading from Parquet files with a large number of row
  groups. ({issue}`24618`)
* Improve performance of reading from large files on S3. ({issue}`24521`)

## Iceberg connector

* Add support for the `FIRST`, `AFTER`, and `LAST` clauses to `ALTER TABLE ...
  ADD COLUMN`. ({issue}`20091`)
* Add support for SSE-C in S3 security mapping. ({issue}`24566`)
* Allow configuring the endpoint for the Google Storage file system with the
  `gcs.endpoint` property. ({issue}`24626`)
* Add `$entries` metadata table. ({issue}`24172`)
* Add `$all_entries` metadata table. ({issue}`24543`)
* Allow configuring the `parquet_bloom_filter_columns` table property. ({issue}`24573`)
* Allow configuring the `orc_bloom_filter_columns` table property. ({issue}`24584`)
* Add the `rollback_to_snapshot` table procedure. The existing
  `system.rollback_to_snapshot` procedure is deprecated. ({issue}`24580`)
* Improve performance when listing columns. ({issue}`23909`)
* Improve performance of reading from Parquet files with a large number of row
  groups. ({issue}`24618`)
* Improve performance of reading from large files on S3. ({issue}`24521`)
* Remove the oldest tracked version metadata files when
  `write.metadata.delete-after-commit.enabled` is set to `true`. ({issue}`19582`)
* Correct catalog information in JMX metrics when using file system caching with
  multiple catalogs. ({issue}`24510`)
* Fix table read failures when using the Alluxio file system. ({issue}`23815`)
* Prevent return of incomplete results by the `table_changes` table function. ({issue}`24709`) 
* Prevent failures on queries accessing tables with multiple nested partition
  columns. ({issue}`24628`)

## Ignite connector

* Add support for `MERGE` statements. ({issue}`24443`)
* Fix failure when updating values to `NULL`. ({issue}`24204`)

## Kudu connector

* Add support for unpartitioned tables. ({issue}`24661`)

## MariaDB connector

* Add support for the `FIRST`, `AFTER`, and `LAST` clauses to `ALTER TABLE ...
  ADD COLUMN`. ({issue}`24735`)
* Fix failure when updating values to `NULL`. ({issue}`24204`)

## MySQL connector

* Add support for the `FIRST`, `AFTER`, and `LAST` clauses to `ALTER TABLE ...
  ADD COLUMN`. ({issue}`24735`)
* Fix failure when updating values to `NULL`. ({issue}`24204`)

## Oracle connector

* Fix failure when updating values to `NULL`. ({issue}`24204`)

## Phoenix connector

* Allow configuring scan page timeout with the
  `phoenix.server-scan-page-timeout` configuration property. ({issue}`24689`)
* Fix failure when updating values to `NULL`. ({issue}`24204`)

## PostgreSQL connector

* Fix failure when updating values to `NULL`. ({issue}`24204`)

## Redshift connector

* Improve performance of reading from Redshift tables. ({issue}`24117`)
* Fix failure when updating values to `NULL`. ({issue}`24204`)

## SingleStore connector

* Fix failure when updating values to `NULL`. ({issue}`24204`)

## Snowflake connector

* Fix failure when updating values to `NULL`. ({issue}`24204`)

## SQL Server connector

* Fix failure when updating values to `NULL`. ({issue}`24204`)

## Vertica connector

* Fix failure when updating values to `NULL`. ({issue}`24204`)

## SPI

* Remove support for connector-level event listeners and the related
  `Connector.getEventListeners()` method. ({issue}`24609`)
