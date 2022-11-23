# Release 388 (29 Jun 2022)

## General

* Add support for `EXPLAIN (TYPE LOGICAL, FORMAT JSON)`. ({issue}`12694`)
* Add `use_exact_partitioning` session property to re-partition data when the
  upstream stage's partitioning does not exactly match what the downstream stage
  expects. ({issue}`12495`)
* Improve read performance for `row` data types. ({issue}`12926`)
* Remove the grouped execution mechanism, including the
  `grouped-execution-enabled`, `dynamic-schedule-for-grouped-execution`,
  and `concurrent-lifespans-per-task` configuration properties and the
  `grouped_execution`, `dynamic_schedule_for_grouped_execution`, and
  `concurrent_lifespans_per_task` session properties. ({issue}`12916`)

## Security

* Add [refresh token](https://oauth.net/2/refresh-tokens/) support in OAuth 2.0. ({issue}`12664`)

## Delta Lake connector

* Add support for setting table and column comments with the `COMMENT`
  statement. ({issue}`12971`)
* Support reading tables with the property `delta.columnMapping.mode=name`. ({issue}`12675`)
* Allow renaming tables with an explicitly set location. ({issue}`11400`)

## Elasticsearch connector

* Remove support for Elasticsearch versions below 6.6.0. ({issue}`11263`)

## Hive connector

* Improve performance of listing files and generating splits when recursive
  directory listings are enabled and tables are stored in S3. ({issue}`12443`)
* Fix incompatibility that prevents Apache Hive 3 and older from reading 
  timestamp columns in files produced by Trino's optimized Parquet
  writer. ({issue}`12857 `)
* Prevent reading from a table that was modified within the same Trino 
  transaction. Previously, this returned incorrect query results. ({issue}`11769`)

## Iceberg connector

* Add support for reading `tinyint` columns from ORC files. ({issue}`8919`)
* Add the ability to configure the schema for materialized view storage tables. ({issue}`12591`)
* Remove old deletion-tracking files when running `optimize`. ({issue}`12617`)
* Fix failure when invoking the `rollback_to_snapshot` procedure. ({issue}`12887`)
* Fix query failure when reading the `$partitions` table after table
  partitioning changed. ({issue}`12874`)

