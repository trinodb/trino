# Release 373 (9 Mar 2022)

## General

* Add {doc}`/connector/delta-lake`. ({issue}`11296`, {issue}`10897`)
* Improve query performance by reducing overhead of cluster internal
  communication. ({issue}`11146`)
* Handle `varchar` to `timestamp` conversion errors in {func}`try`. ({issue}`11259`)
* Add redirection awareness for `DROP COLUMN` task. ({issue}`11304`)
* Add redirection awareness for `RENAME COLUMN` task. ({issue}`11226`)
* Disallow table redirections in `SHOW GRANTS` statement. ({issue}`11270`)
* Allow low memory killer to abort individual tasks when `retry-mode` is set to
  `TASK`. This requires `query.low-memory-killer.policy` set to
  `total-reservation-on-blocked-nodes`. ({issue}`11129`)
* Fix incorrect results when distinct or ordered aggregation are used and
  spilling is enabled. ({issue}`11353`)

## Web UI

* Add CPU time, scheduled time, and cumulative memory statistics regarding
  failed tasks in a query. ({issue}`10754`)

## BigQuery connector

* Allow configuring view expiration time via the `bigquery.view-expire-duration`
  config property. ({issue}`11272`)

## Elasticsearch connector

* Improve performance of queries involving `LIKE` by pushing predicate
  computation to the Elasticsearch cluster. ({issue}`7994`, {issue}`11308`)

## Hive connector

* Support access to S3 via a HTTP proxy. ({issue}`11255`)
* Improve query performance by better estimating partitioned tables statistics. ({issue}`11333`)
* Prevent failure for queries with the final number of partitions
  below `HIVE_EXCEEDED_PARTITION_LIMIT`. ({issue}`10215`)
* Fix issue where duplicate rows could be inserted into a partition when
  `insert_existing_partitions_behavior` was set to `OVERWRITE` and
  `retry-policy` was `TASK`. ({issue}`11196`)
* Fix failure when querying Hive views containing column aliases that differ in
  case only. ({issue}`11159`)

## Iceberg connector

* Support access to S3 via a HTTP proxy. ({issue}`11255`)
* Delete table data when dropping table. ({issue}`11062`)
* Fix `SHOW TABLES` failure when a materialized view is removed during query
  execution. ({issue}`10976`)
* Fix query failure when reading from `information_schema.tables` or
  `information_schema.columns` and a materialized view is removed during
  query execution. ({issue}`10976`)

## Oracle connector

* Fix query failure when performing concurrent write operations. ({issue}`11318`)

## Phoenix connector

* Prevent writing incorrect results when arrays contain `null` values. ({issue}`11351`)

## SQL Server connector

* Fix incorrect results when querying SQL Server `tinyint` columns by mapping
  them to Trino `smallint`. ({issue}`11209`)

## SPI

* Add CPU time, scheduled time, and cumulative memory statistics regarding
  failed tasks in a query to query-completion events. ({issue}`10734`)

