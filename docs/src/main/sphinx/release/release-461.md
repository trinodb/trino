# Release 461 (10 Oct 2024)

## General

* Rename the configuration property `max-tasks-waiting-for-execution-per-stage`
  to `max-tasks-waiting-for-execution-per-query` and the session property
  `max_tasks_waiting_for_node_per_stage` to
  `max_tasks_waiting_for_node_per_query` to match implemented semantics. ({issue}`23585`)
* Fix failure when joining tables with large numbers of columns. ({issue}`23720`)
* Fix failure for `MERGE` queries on tables with large numbers of columns. ({issue}`15848`)

## Security

* Add support for BCrypt versions 2A, 2B, and 2X usage in password database files
  used with file-based authentication. ({issue}`23648`)

## Web UI

* Add buttons on the query list to access query details. ({issue}`22831`)
* Add syntax highlighting to query display on query list. ({issue}`22831`)

## BigQuery connector

* Fix failure when `bigquery.case-insensitive-name-matching` is enabled and
  `bigquery.case-insensitive-name-matching.cache-ttl` is `0m`. ({issue}`23698`)

## Delta Lake connector

* Enforce access control for new tables in the `register_table` procedure. ({issue}`23728`)

## Hive connector

* Add support for reading Hive tables that use `CombineTextInputFormat`. ({issue}`21842`)
* Improve performance of queries with selective joins. ({issue}`23687`)

## Iceberg connector

* Add support for the `add_files` and `add_files_from_table` procedures. ({issue}`11744`)
* Support `timestamp` type columns with the `migrate` procedure. ({issue}`17006`)
* Enforce access control for new tables in the `register_table` procedure. ({issue}`23728`)

## Redshift connector

* Improve performance of queries with range filters on integers. ({issue}`23417`)
