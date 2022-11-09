# Release 403 (15 Nov 2022)

## General

* Include the amount of data read from external sources in the output of
  `EXPLAIN ANALYZE`. ({issue}`14907`)
* Improve performance of worker-to-worker data transfer encryption when
  fault-tolerant execution is enabled. ({issue}`14941`)
* Improve performance of aggregations when input data does not contain nulls. ({issue}`14567`)
* Fix potential failure when clients do not support variable precision temporal
  types. ({issue}`14950`)
* Fix query deadlock in multi-join queries where broadcast join size is
  underestimated. ({issue}`14948`)
* Fix incorrect results when `min(x, n)` or `max(x, n)` is used as a window
  function. ({issue}`14886`)
* Fix failure for certain queries involving joins over partitioned tables. ({issue}`14317`)
* Fix incorrect order of parameters in `DESCRIBE INPUT` when they appear in a
  `WITH` clause. ({issue}`14738`)
* Fix failure for queries involving `BETWEEN` predicates over `varchar` columns
  that contain temporal data. ({issue}`14954`)

## Security

* Allow access token passthrough when using OAuth 2.0 authentication with
  refresh tokens enabled. ({issue}`14949`)

## BigQuery connector

* Improve performance of `SHOW SCHEMAS` by adding a metadata cache. This can be
  configured with the `bigquery.metadata.cache-ttl` catalog property, which is
  disabled by default. ({issue}`14729`)
* Fix failure when a [row access policy](https://cloud.google.com/bigquery/docs/row-level-security-intro)
  returns an empty result. ({issue}`14760`)

## ClickHouse connector

* Add mapping for the ClickHouse `DateTime(timezone)` type to the Trino
  `timestamp(0) with time zone` type for read-only operations. ({issue}`13541`)

## Delta Lake connector

* Fix statistics for `DATE` columns. ({issue}`15005`)

## Hive connector

* Avoid showing the unsupported `AUTHORIZATION ROLE` property in the result of
  `SHOW CREATE SCHEMA` when the access control doesn't support roles. ({issue}`8817`)

## Iceberg connector

* Improve performance and storage requirements when running the
  `expire_snapshots` table procedure on S3-compatible storage. ({issue}`14434`)
* Allow registering existing table files in the metastore with the new
  [`register_table` procedure](iceberg-register-table). ({issue}`13552`)

## MongoDB connector

* Add support for {doc}`/sql/delete`. ({issue}`14864`)
* Fix incorrect results when predicates over `varchar` and `char` columns are
  pushed into the connector and MongoDB collections have a collation specified. ({issue}`14900`)

## SQL Server connector

* Fix incorrect results when non-transactional `INSERT` is disabled and bulk
  `INSERT` is enabled. ({issue}`14856`)

## SPI

* Enhance `ConnectorTableLayout` to allow the connector to specify that multiple
  writers per partition are allowed. ({issue}`14956`)
* Remove deprecated methods from `ConnectorPageSinkProvider`. ({issue}`14959`)
