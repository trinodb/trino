# Release 418 (17 May 2023)

## General

* Add support for [EXECUTE IMMEDIATE](/sql/execute-immediate). ({issue}`17341`)
* Fix failure when invoking `current_timestamp`. ({issue}`17455`)

## BigQuery connector

* Add support for adding labels to BigQuery jobs started by Trino as part of
  query processing. The name and value of the label can be configured via the
  `bigquery.job.label-name` and `bigquery.job.label-format` catalog
  configuration properties, respectively. ({issue}`16187`)

## Delta Lake connector

* Add support for `INSERT`, `UPDATE`, `DELETE`, and `MERGE` statements for
  tables with an `id` column mapping. ({issue}`16600`)
* Add the `table_changes` table function. ({issue}`16205`)
* Improve performance of joins on partition columns. ({issue}`14493`)

## Hive connector

* Improve performance of querying `information_schema.tables` when using the
  Hive metastore. ({issue}`17127`)
* Improve performance of joins on partition columns. ({issue}`14493`)
* Improve performance of writing Parquet files by enabling the optimized Parquet 
  writer by default. ({issue}`17393`)
* Remove the `temporary_staging_directory_enabled` and
  `temporary_staging_directory_path` session properties. ({issue}`17390`)
* Fix failure when querying text files in S3 if the native reader is enabled. ({issue}`16546`)

## Hudi connector

* Improve performance of joins on partition columns. ({issue}`14493`)

## Iceberg connector

* Improve planning time for `SELECT` queries. ({issue}`17347`)
* Improve performance of joins on partition columns. ({issue}`14493`)
* Fix incorrect results when querying the `$history` table if the REST catalog
  is used. ({issue}`17470`)

## Kafka connector

* Fix query failure when a Kafka key or message cannot be de-serialized, and
  instead correctly set the `_key_corrupt` and `_message_corrupt` columns. ({issue}`17479`)

## Kinesis connector

* Fix query failure when a Kinesis message cannot be de-serialized, and
  instead correctly set the `_message_valid` column. ({issue}`17479`)

## Oracle connector

* Add support for writes when [fault-tolerant
  execution](/admin/fault-tolerant-execution) is enabled. ({issue}`17200`)

## Redis connector

* Fix query failure when a Redis key or value cannot be de-serialized, and
  instead correctly set the `_key_corrupt` and `_value_corrupt` columns. ({issue}`17479`)
