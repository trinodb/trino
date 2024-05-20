# Release 446 (1 May 2024)

## General

* Improve performance of `INSERT` statements into partitioned tables when the
  `retry_policy` configuration property is set to `TASK`. ({issue}`21661 `)
* Improve performance of queries with complex grouping operations.  ({issue}`21726`)
* Reduce delay before killing queries when the cluster runs out of memory. ({issue}`21719`)
* Prevent assigning null values to non-null columns as part of a `MERGE`
  statement. ({issue}`21619`)
* Fix `CREATE CATALOG` statements including quotes in catalog names. ({issue}`21399`)
* Fix potential query failure when a column name ends with a `:`. ({issue}`21676`)
* Fix potential query failure when a [SQL routine](/routines) contains a label
  reference in a `LEAVE`, `ITERATE`, `REPEAT`, or `WHILE` statement. ({issue}`21682`)
* Fix query failure when [SQL routines](/routines) use the `NULLIF` or `BETWEEN`
  functions. ({issue}`19820`)
* Fix potential query failure due to worker nodes running out of memory in
  concurrent scenarios. ({issue}`21706`)

## BigQuery connector

* Improve performance when listing table comments. ({issue}`21581`)
* {{breaking}} Enable `bigquery.arrow-serialization.enabled` by default. This
  requires `--add-opens=java.base/java.nio=ALL-UNNAMED` in
  `jvm-config`. ({issue}`21580`)

## Delta Lake connector

* Fix failure when reading from Azure file storage and the schema, table, or
  column name contains non-alphanumeric characters. ({issue}`21586`)
* Fix incorrect results when reading a partitioned table with a
  [deletion vector](https://docs.delta.io/latest/delta-deletion-vectors.html). ({issue}`21737`)

## Hive connector

* Add support for reading S3 objects restored from Glacier storage. ({issue}`21164`)
* Fix failure when reading from Azure file storage and the schema, table, or
  column name contains non-alphanumeric characters. ({issue}`21586`)
* Fix failure when listing Hive views with unsupported syntax. ({issue}`21748`)

## Iceberg connector

* Add support for the [Snowflake catalog](iceberg-snowflake-catalog). ({issue}`19362`)
* Automatically use `varchar` as a type during table creation when `char` is
  specified. ({issue}`19336`, {issue}`21515`)
* Deprecate the `schema` and `table` arguments for the `table_changes` function
  in favor of `schema_name` and `table_name`, respectively. ({issue}`21698`)
* Fix failure when executing the `migrate` procedure with partitioned Hive
  tables on Glue. ({issue}`21391`)
* Fix failure when reading from Azure file storage and the schema, table, or
  column name contains non-alphanumeric characters. ({issue}`21586`)

## Pinot connector

* Fix query failure when a predicate contains a `'`. ({issue}`21681`)

## Snowflake connector

* Add support for the `unsupported-type-handling` and
  `jdbc-types-mapped-to-varchar` type mapping configuration properties. ({issue}`21528`)

## SPI

* Remove support for `@RemoveInput` as an annotation for aggregation functions.
  A `WindowAggregation` can be declared in `@AggregationFunction` instead, which
   supports input removal. ({issue}`21349`)
* Extend `QueryCompletionEvent` with various aggregated, per-stage, per-task
  distribution statistics. New information is available in
  `QueryCompletedEvent.statistics.taskStatistics`. ({issue}`21694`)
