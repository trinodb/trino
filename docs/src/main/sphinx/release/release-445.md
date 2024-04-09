# Release 445 (17 Apr 2024)

## General

* Add support for large constant arrays. ({issue}`21566`)
* Add the `query.dispatcher-query-pool-size` configuration property to prevent
  the coordinator from hanging when too many queries are being executed at once. ({issue}`20817`)
* Improve performance of queries selecting only catalog, schema, and name from
  the `system.metadata.materialized_views` table. ({issue}`21448`)
* {{breaking}} Remove the deprecated `legacy.materialized-view-grace-period`
  configuration property. ({issue}`21474`)
* Increase the number of columns supported by `MERGE` queries before failing
  with a `MethodTooLargeException` error. ({issue}`21299`)
* Fix potential query hang when there is an error processing data. ({issue}`21397`)
* Fix possible worker crashes when running aggregation queries due to
  out-of-memory error. ({issue}`21425`)
* Fix incorrect results when performing aggregations over null values. ({issue}`21457`)
* Fix failure for queries containing expressions involving types that do
  not support the `=` operator (e.g., `HyperLogLog`, `Geometry`, etc.). ({issue}`21508`)
* Fix incorrect results for distinct count aggregations over a constant value. ({issue}`18562`)
* Fix sporadic query failure when filesystem caching is enabled. ({issue}`21342`)
* Fix unexpected failure for join queries containing predicates that might raise
  an error for some inputs. ({issue}`21521`)

## BigQuery connector

* Add support for reading materialized views. ({issue}`21487`)
* Add support for using filters when materializing BigQuery views. ({issue}`21488`)

## Delta Lake connector

* Add support for [time travel](delta-time-travel) queries. ({issue}`21052`)
* Add support for the `REPLACE` modifier as part of a `CREATE TABLE` statement. ({issue}`13180`) ({issue}`19991`)

## Hive connector

* Add support for creating views with custom properties. ({issue}`21401`)
* Add support for writing Bloom filters in Parquet files. ({issue}`20662`)
* {{breaking}} Remove the deprecated `PARTITION_COLUMN` and `PARTITION_VALUE`
  arguments from the `flush_metadata_cache` procedure in favor of
  `PARTITION_COLUMNS` and `PARTITION_VALUES`. ({issue}`21410`)

## Iceberg connector

* Deprecate the `iceberg.materialized-views.hide-storage-table` configuration
  property. ({issue}`21485`)

## MongoDB connector

* Add support for [dynamic filtering](/admin/dynamic-filtering). ({issue}`21355`)

## MySQL connector

* Improve performance of queries with `timestamp(n)` values. ({issue}`21244`)

## PostgreSQL connector

* Improve performance of queries with `timestamp(n)` values. ({issue}`21244`)

## Redis connector

* Upgrade minimum required Redis version to 5.0.14 or later. ({issue}`21455`)

## Snowflake connector

* Add support for pushing down execution of the `variance`, `var_pop`,
  `var_samp`,`covar_pop`, `covar_samp`, `corr`, `regr_intercept`, and
  `regr_slope` functions to the underlying database. ({issue}`21384`)
