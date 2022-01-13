# Release 367 (22 Dec 2021)

## General

* Capture lineage details for columns from `WITH` clauses and subqueries. ({issue}`10272`)
* Improve `CREATE VIEW` error message when table or materialized view already exists. ({issue}`10186`)
* Disallow query retries when connectors cannot perform them safely. ({issue}`10064`)
* Improve performance when query retries are enabled by adding support for dynamic filtering. ({issue}`10274`)
* Fix failure in `min_by` and `max_by` aggregation execution. ({issue}`10347`)
* Fix planning failure for queries that access fields of `row` types by index (`ROW(...)[n]`) or that 
  select all row fields (`ROW(..).*`). ({issue}`10321`)
* Fix bug where certain queries which use broadcast joins could hang and never complete. ({issue}`10344`)
* Fix failure when row or array in `VALUES` clause contains nulls. ({issue}`10141`)

## Security

* Hide inaccessible columns from `SELECT *` statement when  
  the `hide-inaccessible-columns` configuration property is set to true. ({issue}`9991`)
* Disable `SET AUTHORIZATION` when `VIEW` runs as `DEFINER`. ({issue}`10351`)

## Web UI

* Improve user experience by introducing a new landing page for logout flow when 
  Oauth2 authentication is used. ({issue}`10299`)

## Hive connector

* Add procedure `system.flush_metadata_cache()` to flush metadata caches. ({issue}`10251`)
* Prevent data loss during `DROP SCHEMA` when schema location contains files but not tables. ({issue}`10146`)
* Ensure no duplicate rows are created if query which writes data to Hive table is retried. ({issue}`10252`, {issue}`10064`)

## Iceberg connector

* Prevent data loss during `DROP SCHEMA` when schema location contains files but not tables. ({issue}`9767`)

## SPI

* Fix `ClassNotFoundException` when using aggregation with a custom state type. ({issue}`10341`)
