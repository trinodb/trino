# Release 426 (5 Sep 2023)

## General

* Add support for `SET SESSION AUTHORIZATION` and `RESET SESSION AUTHORIZATION`. ({issue}`16067`)
* Add support for automatic type coercion when creating tables. ({issue}`13994`)
* Improve performance of aggregations over decimal values. ({issue}`18868`)
* Fix event listener incorrectly reporting output columns for `UPDATE`
  statements with subqueries. ({issue}`18815`)
* Fix failure when performing an outer join involving geospatial functions in
  the join clause. ({issue}`18860`)
* Fix failure when querying partitioned tables with a `WHERE` clause that
  contains lambda expressions. ({issue}`18865`)
* Fix failure for `GROUP BY` queries over `map` and `array` types. ({issue}`18863`)

## Security

* Fix authentication failure with OAuth 2.0 when authentication tokens are
  larger than 4 KB. ({issue}`18836`)

## Delta Lake connector

* Add support for the `TRUNCATE TABLE` statement. ({issue}`18786`)
* Add support for the `CASCADE` option in `DROP SCHEMA` statements. ({issue}`18333`)
* Add support for
  [Databricks 13.3 LTS](https://docs.databricks.com/en/release-notes/runtime/13.3lts.html). ({issue}`18888`)
* Fix writing an incorrect transaction log for partitioned tables with an `id`
  or `name` column mapping mode. ({issue}`18661`)

## Hive connector

* Add the `hive.metastore.thrift.batch-fetch.enabled` configuration property,
  which can be set to `false` to disable batch metadata fetching from the Hive
  metastore. ({issue}`18111`)
* Fix `ANALYZE` failure when row count stats are missing. ({issue}`18798`)
* Fix the `hive.target-max-file-size` configuration property being ignored
  when writing to sorted tables. ({issue}`18653`)
* Fix query failure when reading large SequenceFile, RCFile, or Avro files. ({issue}`18837`)

## Iceberg connector

* Fix the `iceberg.target-max-file-size` configuration property being ignored
  when writing to sorted tables. ({issue}`18653`)

## SPI

* Remove the deprecated
  `ConnectorMetadata#dropSchema(ConnectorSession session, String schemaName)`
  method. ({issue}`18839`)
