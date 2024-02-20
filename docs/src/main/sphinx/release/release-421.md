# Release 421 (6 Jul 2023)

## General

* Add support for check constraints in an `UPDATE` statement. ({issue}`17195`)
* Improve performance for queries involving a `year` function within an `IN`
  predicate. ({issue}`18092`)
* Fix failure when cancelling a query with a window function. ({issue}`18061`)
* Fix failure for queries involving the `concat_ws` function on arrays with more
  than 254 values. ({issue}`17816`)
* Fix query failure or incorrect results when coercing a
  [structural data type](structural-data-types) that contains a timestamp. ({issue}`17900`)

## JDBC driver

* Add support for using an alternative hostname with the `hostnameInCertificate`
  property when SSL verification is set to `FULL`. ({issue}`17939`)

## Delta Lake connector

* Add support for check constraints and column invariants in `UPDATE`
  statements. ({issue}`17195`)
* Add support for creating tables with the `column` mapping mode. ({issue}`12638`)
* Add support for using the `OPTIMIZE` procedure on column mapping tables. ({issue}`17527`)
* Add support for `DROP COLUMN`. ({issue}`15792`)

## Google Sheets connector

* Add support for {doc}`/sql/insert` statements. ({issue}`3866`)

## Hive connector

* Add Hive partition projection column properties to the output of
  `SHOW CREATE TABLE`. ({issue}`18076`)
* Fix incorrect query results when using S3 Select with `IS NULL` or
  `IS NOT NULL` predicates. ({issue}`17563`)
* Fix incorrect query results when using S3 Select and a table's `null_format`
  field is set. ({issue}`17563`)

## Iceberg connector

* Add support for migrating a bucketed Hive table into a non-bucketed Iceberg
  table. ({issue}`18103`)

## Kafka connector

* Add support for reading Protobuf messages containing the `Any` Protobuf type.
  This is disabled by default and can be enabled by setting the 
  `kafka.protobuf-any-support-enabled` configuration property to `true`. ({issue}`17394`)

## MongoDB connector

* Improve query performance on tables with `row` columns when only a subset of
  fields is needed for the query. ({issue}`17710`)

## Redshift connector

* Add support for [table comments](/sql/comment). ({issue}`16900`)

## SPI

* Add the `BLOCK_AND_POSITION_NOT_NULL` argument convention. ({issue}`18035`)
* Add the `BLOCK_BUILDER` return convention that writes function results
  directly to a `BlockBuilder`. ({issue}`18094`)
* Add the `READ_VALUE` operator that can read a value from any argument
  convention to any return convention.  ({issue}`18094`)
* Remove write methods from the BlockBuilder interface. ({issue}`17342`)
* Change array, map, and row build to use a single `writeEntry`. ({issue}`17342`)
