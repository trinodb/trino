# Release 386 (15 Jun 2022)

## General

* Improve out-of-the-box performance of queries when `task` retry policy is
  enabled. ({issue}`12646`)
* Improve query latency when `task` retry policy is enabled. ({issue}`12615`)

## JDBC driver

* Add configuration property
  `assumeLiteralUnderscoreInMetadataCallsForNonConformingClients` for situations
  where applications do not properly escape schema or table names in calls to
  `DatabaseMetaData`. ({issue}`12672`)

## Accumulo connector

* Disallow creating a view in a non-existent schema. ({issue}`12475`)

## Delta Lake connector

* Improve query performance on tables with many small files. ({issue}`12755`)
* Disallow reading tables if `delta.columnMapping.mode` table property is
  specified. ({issue}`12621`)
* Set a target maximum file size during table writes. The default is 1 GB and
  can be configured with the `target_max_file_size` session property or the
  `target-max-file-size` configuration property. ({issue}`12820`)

## Hive connector

* Fix incompatibility with Apache Hive when writing decimal values with
  precision of 18 or less with the experimental Parquet writer. ({issue}`12658`)
* Fix potential query failure when using schema evolution with union-typed
  columns. ({issue}`12520`)
* Fix potential query failure when reading `timestamp(6) with time zone` values. ({issue}`12804`)

## Iceberg connector

* Disallow creating a table with a pre-existing destination location. ({issue}`12573`)
* Fix `NoClassDefFoundError` query failure when using Google Cloud Storage. ({issue}`12674`)
* Fix `ClassNotFoundException: Class io.trino.plugin.hive.s3.TrinoS3FileSystem`
  error when querying `information_schema.columns`. ({issue}`12676`)
* Avoid creating a table snapshot when a write statement does not change the
  table state. ({issue}`12319`)
* Fix incorrect query results when filtering on `$path` synthetic column and on
  at least one other column. ({issue}`12790`)
* Fix potential query failure when reading `timestamp(6) with time zone` values. ({issue}`12804`)
* Fix query failure when using the `[VERSION | TIMESTAMP] AS OF` clause on a
  table with redirection. ({issue}`12542`)
* Fix query failure when reading a `timestamp(p) with time zone` value before
  1970 from a Parquet file. ({issue}`12852`)

## Kafka connector

* Fix failure when decoding a `float` value to `real` type. ({issue}`12784`)

## Phoenix connector

* Remove support for Phoenix 4. ({issue}`12772`)

## SPI

* Add new version of `getStatisticsCollectionMetadata()` to `ConnectorMetadata`
  which returns `ConnectorAnalyzeMetadata`. Deprecate the existing method and
  `getTableHandleForStatisticsCollection()`. ({issue}`12388`)
* Remove deprecated `ConnectorMetadata.getTableStatistics` method. ({issue}`12489`)
