# Release 433 (10 Nov 2023)

## General

* Improve planning time and resulting plan efficiency for queries involving
  `UNION ALL` with `LIMIT`. ({issue}`19471`)
* Fix long query planning times for queries with multiple window functions. ({issue}`18491`)
* Fix resource groups not noticing updates to the `softMemoryLimit` if it is
  changed from a percent-based value to an absolute value. ({issue}`19626`)
* Fix potential query failure for queries involving arrays, `GROUP BY`,
  or `DISTINCT`. ({issue}`19596`)

## BigQuery connector

* Fix incorrect results for queries involving projections and the `query` table
  function. ({issue}`19570`)

## Delta Lake connector

* Fix query failure when reading ORC files with a `DECIMAL` column that
  contains only null values. ({issue}`19636`)
* Fix possible JVM crash when reading short decimal columns in Parquet files
  created by Impala. ({issue}`19697`)

## Hive connector

* Add support for reading tables where a column's type has been changed from
  `boolean` to `varchar`. ({issue}`19571`)
* Add support for reading tables where a column's type has been changed from
  `varchar` to `double`. ({issue}`19517`)
* Add support for reading tables where a column's type has been changed from
  `tinyint`, `smallint`, `integer`, or `bigint` to `double`. ({issue}`19520`)
* Add support for altering table comments in the Glue catalog. ({issue}`19073`)
* Fix query failure when reading ORC files with a `DECIMAL` column that
  contains only null values. ({issue}`19636`)
* Fix possible JVM crash when reading short decimal columns in Parquet files
  created by Impala. ({issue}`19697`)

## Hudi connector

* Fix query failure when reading ORC files with a `DECIMAL` column that
  contains only null values. ({issue}`19636`)
* Fix possible JVM crash when reading short decimal columns in Parquet files
  created by Impala. ({issue}`19697`)

## Iceberg connector

* Fix incorrect query results when querying Parquet files with dynamic filtering
  on `UUID` columns. ({issue}`19670`)
* Fix query failure when reading ORC files with a `DECIMAL` column that
  contains only null values. ({issue}`19636`)
* Fix possible JVM crash when reading short decimal columns in Parquet files
  created by Impala. ({issue}`19697`)
* Prevent creation of separate entries for storage tables of materialized views.
  ({issue}`18853`)

## SPI

* Add JMX metrics for event listeners through
  `trino.eventlistener:name=EventListenerManager`. ({issue}`19623`)
