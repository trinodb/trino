# Release 413 (12 Apr 2023)

## General

* Improve performance of queries involving window operations or
  [row pattern recognition](/sql/pattern-recognition-in-window) on small
  partitions. ({issue}`16748`)
* Improve performance of queries with the {func}`row_number` and {func}`rank`
  window functions. ({issue}`16753`)
* Fix potential failure when cancelling a query. ({issue}`16960`)

## Delta Lake connector

* Add support for nested `timestamp with time zone` values in
  [structural data types](structural-data-types). ({issue}`16826`)
* Disallow using `_change_type`, `_commit_version`, and `_commit_timestamp` as
  column names when creating a table or adding a column with
  [change data feed](https://docs.delta.io/2.0.0/delta-change-data-feed.html). ({issue}`16913`)
* Disallow enabling change data feed when the table contains
  `_change_type`, `_commit_version` and `_commit_timestamp` columns. ({issue}`16913`)
* Fix incorrect results when reading `INT32` values without a decimal logical
  annotation in Parquet files. ({issue}`16938`)

## Hive connector

* Fix incorrect results when reading `INT32` values without a decimal logical
  annotation in Parquet files. ({issue}`16938`)
* Fix incorrect results when the file path contains hidden characters. ({issue}`16386`)

## Hudi connector

* Fix incorrect results when reading `INT32` values without a decimal logical
  annotation in Parquet files. ({issue}`16938`)

## Iceberg connector

* Fix incorrect results when reading `INT32` values without a decimal logical
  annotation in Parquet files. ({issue}`16938`)
* Fix failure when creating a schema with a username containing uppercase
  characters in the Iceberg Glue catalog. ({issue}`16116`)

## Oracle connector

* Add support for [table comments](/sql/comment) and creating tables with
  comments. ({issue}`16898`)

## Phoenix connector

* Add support for {doc}`/sql/merge`. ({issue}`16661`)

## SPI

* Deprecate the `getSchemaProperties()` and `getSchemaOwner()` methods in
  `ConnectorMetadata` in favor of versions that accept a `String` for the schema
  name rather than `CatalogSchemaName`. ({issue}`16862`)
