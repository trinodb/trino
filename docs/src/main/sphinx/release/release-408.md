# Release 408 (23 Feb 2023)

## General

* Add physical input read time to query statistics and the output of `EXPLAIN
  ANALYZE`. ({issue}`16190`)
* Fix query failure for queries involving joins or aggregations with a
  [structural type](structural-data-types) that contains `NULL` elements. ({issue}`16140`)

## Security

* Deprecate using groups with OAuth 2.0 authentication, and rename the
  `http-server.authentication.oauth2.groups-field` configuration property to
  `deprecated.http-server.authentication.oauth2.groups-field`. ({issue}`15669`)

## CLI

* Add `AUTO` output format which switches from `ALIGNED` to `VERTICAL` if
  the output doesn't fit the current terminal. ({issue}`12208`)
* Add `--pager` and `--history-file` options to match the existing `TRINO_PAGER`
  and `TRINO_HISTORY_FILE` environmental variables. Also allow setting these
  options in a configuration file. ({issue}`16151`)

## BigQuery connector

* Add support for writing `decimal` types to BigQuery. ({issue}`16145`)

## Delta Lake connector

* Rename the connector to `delta_lake`. The old name `delta-lake` is now
  deprecated and will be removed in a future release. ({issue}`13931`)
* Add support for creating tables with the Trino `change_data_feed_enabled`
  table property. ({issue}`16129`)
* Improve query performance on tables that Trino has written to with `INSERT`. ({issue}`16026`)
* Improve performance of reading [structural types](structural-data-types) from
  Parquet files. This optimization can be disabled with the
  `parquet_optimized_nested_reader_enabled` catalog session property or the
  `parquet.optimized-nested-reader.enabled` catalog configuration property. ({issue}`16177`)
* Retry dropping Delta tables registered in the Glue catalog to avoid failures
  due to concurrent modifications. ({issue}`13199`)
* Allow updating the `reader_version` and `writer_version` table properties. ({issue}`15932`)
* Fix inaccurate change data feed entries for `MERGE` queries. ({issue}`16127`)
* Fix performance regression when writing to partitioned tables if table
  statistics are absent. ({issue}`16152`)

## Hive connector

* Remove support for the deprecated `hive-hadoop2` connector name, requiring the
  `connector.name` property to be set to `hive`. ({issue}`16166`)
* Retry dropping Delta tables registered in the Glue catalog to avoid failures
  due to concurrent modifications. ({issue}`13199`)
* Fix performance regression when writing to partitioned tables if table
  statistics are absent. ({issue}`16152`)

## Iceberg connector

* Reduce memory usage when reading `$files` system tables. ({issue}`15991`)
* Require the `iceberg.jdbc-catalog.driver-class` configuration property to be
  set to prevent a "driver not found" error after initialization. ({issue}`16196`)
* Fix performance regression when writing to partitioned tables if table
  statistics are absent. ({issue}`16152`)

## Ignite connector

* Add [Ignite connector](/connector/ignite). ({issue}`8098`)

## SingleStore connector

* Remove support for the deprecated `memsql` connector name, requiring the
  `connector.name` property to be set to `singlestore`. ({issue}`16180`)

## SQL Server connector

* Add support for pushing down `=`, `<>` and `IN` predicates over text columns
  if the column uses a case-sensitive collation within SQL Server. ({issue}`15714`)

## Thrift connector

* Rename the connector to `trino_thrift`. The old name `trino-thrift` is now
  deprecated and will be removed in a future release. ({issue}`13931`)
