# Release 420 (22 Jun 2023)

## General

* Add support for the {func}`any_value` aggregation function. ({issue}`17777`)
* Add support for underscores in numeric literals. ({issue}`17776`)
* Add support for hexadecimal, binary, and octal numeric literals. ({issue}`17776`)
* Deprecate the `dynamic-filtering.small-broadcast.*` and
  `dynamic-filtering.large-broadcast.*` configuration properties in favor of
  `dynamic-filtering.small.*` and `dynamic-filtering.large.*`. ({issue}`17831`)

## Security

* Add support for configuring authorization rules for
  `ALTER ... SET AUTHORIZATION...` statements in file-based access control. ({issue}`16691`)
* Remove the deprecated `legacy.allow-set-view-authorization` configuration
  property. ({issue}`16691`)

## BigQuery connector

* Fix direct download of access tokens, and correctly use the proxy when it
  is enabled with the `bigquery.rpc-proxy.enabled` configuration property. ({issue}`17783`)

## Delta Lake connector

* Add support for [comments](/sql/comment) on view columns. ({issue}`17773`)
* Add support for recalculating all statistics with an `ANALYZE` statement. ({issue}`15968`)
* Disallow using the root directory of a bucket (`scheme://authority`) as a
  table location without a trailing slash in the location name. ({issue}`17921`)
* Fix Parquet writer incompatibility with Apache Spark and Databricks Runtime. ({issue}`17978`)

## Druid connector

* Add support for tables with uppercase characters in their names. ({issue}`7197`)

## Hive connector

* Add a native Avro file format reader. This can be disabled with the
  `avro.native-reader.enabled` configuration property or the
  `avro_native_reader_enabled` session property. ({issue}`17221`)
* Require admin role privileges to perform `ALTER ... SET AUTHORIZATION...`
  statements when the `hive-security` configuration property is set to
  `sql-standard`. ({issue}`16691`)
* Improve query performance on partitioned Hive tables when table statistics are 
  not available. ({issue}`17677`)
* Disallow using the root directory of a bucket (`scheme://authority`) as a
  table location without a trailing slash in the location name. ({issue}`17921`)
* Fix Parquet writer incompatibility with Apache Spark and Databricks Runtime. ({issue}`17978`)
* Fix reading from a Hive table when its location is the root directory of an S3
  bucket. ({issue}`17848`)

## Hudi connector

* Disallow using the root directory of a bucket (`scheme://authority`) as a
  table location without a trailing slash in the location name. ({issue}`17921`)
* Fix Parquet writer incompatibility with Apache Spark and Databricks Runtime. ({issue}`17978`)
* Fix failure when fetching table metadata for views. ({issue}`17901`)

## Iceberg connector

* Disallow using the root directory of a bucket (`scheme://authority`) as a
  table location without a trailing slash in the location name. ({issue}`17921`)
* Fix Parquet writer incompatibility with Apache Spark and Databricks Runtime. ({issue}`17978`)
* Fix scheduling failure when dynamic filtering is enabled. ({issue}`17871`)

## Kafka connector

* Fix server startup failure when a Kafka catalog is present. ({issue}`17299`)

## MongoDB connector

* Add support for `ALTER TABLE ... RENAME COLUMN`. ({issue}`17874`)
* Fix incorrect results when the order of the
  [dbref type](https://www.mongodb.com/docs/manual/reference/database-references/#dbrefs)
  fields is different from `databaseName`, `collectionName`, and `id`. ({issue}`17883`)

## SPI

* Move table function infrastructure to the `io.trino.spi.function.table`
  package. ({issue}`17774`)
