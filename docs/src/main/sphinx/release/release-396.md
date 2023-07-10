# Release 396 (15 Sep 2022)

## General

* Improve performance of queries that process string data. ({issue}`12798`)
* Fix failure when querying views that use table functions.  ({issue}`13944`)

## BigQuery connector

* Add support for [column and table comments](/sql/comment). ({issue}`13882`)

## ClickHouse connector

* Improve performance when pushing down large lists of predicates by raising
  the default threshold before the predicate is compacted. ({issue}`14029`)
* Fix mapping to the ClickHouse `Date` and `DateTime` types to include the full
  range of possible values. ({issue}`11116`)
* Fix failure when specifying a table or column comment that contains special
  characters. ({issue}`14058`)

## Delta Lake connector

* Add support for writing to tables using [version 3 of the
  writer](https://docs.delta.io/latest/versioning.html#features-by-protocol-version).
  This does not yet include support for `CHECK` constraints. ({issue}`14068`)
* Add support for reading tables with the table property
  `delta.columnMapping.mode` set to `id`. ({issue}`13629`)
* Improve performance when writing
  [structural data types](structural-data-types) to Parquet files. ({issue}`13714`)
* Attempt to undo the operation when writing a checkpoint file fails. ({issue}`14108`)
* Fix performance regression when reading Parquet data. ({issue}`14094`)

## Hive connector

* Add verification for files written by the optimized Parquet writer. This can
  be configured with the [`parquet.optimized-writer.validation-percentage`
  configuration property](hive-parquet-configuration) or the
  `parquet_optimized_writer_validation_percentage` session property. ({issue}`13246`)
* Improve optimized Parquet writer performance for
  [structural data types](structural-data-types). ({issue}`13714`)
* Fix performance regression in reading Parquet files. ({issue}`14094`)

## Iceberg connector

* Improve performance when writing
  [structural data types](structural-data-types) to Parquet files. ({issue}`13714`)
* Improve performance of queries that contain predicates involving `date_trunc`
  on `date`, `timestamp` or `timestamp with time zone` partition columns. ({issue}`14011`)
* Fix incorrect results from using the `[VERSION | TIMESTAMP] AS OF` clause when
  the snapshot's schema differs from the current schema of the table. ({issue}`14064`)
* Prevent `No bucket node map` failures when inserting data. ({issue}`13960`)
* Fix performance regression when reading Parquet data introduced in
  [Trino version 394](release-394.md). ({issue}`14094`)

## MariaDB connector

* Fix failure when using special characters in a table or column comment when
  creating a table. ({issue}`14058`)

## MySQL connector

* Fix failure when using special characters in a table or column comment when
  creating a table. ({issue}`14058`)

## Oracle connector

* Fix failure when setting a column comment with special characters. ({issue}`14058`)

## Phoenix connector

* Improve performance when pushing down large lists of predicates by raising
  the default threshold before the predicate is compacted. ({issue}`14029`)

## PostgreSQL connector

* Fix failure when setting a column comment with special characters. ({issue}`14058`)

## Redshift connector

* Fix failure when setting a column comment with special characters. ({issue}`14058`)

## SPI

* Add the `SystemAccessControl.checkCanGrantExecuteFunctionPrivilege` overload,
  which needs to be implemented to allow views that use table functions. ({issue}`13944`)
* Add the `ConnectorMetadata.applyJoin` overload. It provides the connector
  with a join condition that is as complete as possible to represent using
  `ConnectorExpression`. Deprecate the previous version of 
  `ConnectorMetadata.applyJoin`. ({issue}`13943`)
