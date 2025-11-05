# Release 472 (5 Mar 2025)

## General

* Color the server console output for improved readability. ({issue}`25090`)
* {{breaking}} Rename HTTP client property prefixes from `workerInfo` and
  `memoryManager` to `worker-info` and `memory-manager`. ({issue}`25099`)
* Fix failure for queries with large numbers of expressions in the `SELECT` clause. ({issue}`25040`)
* Improve performance of certain queries involving `ORDER BY ... LIMIT` with subqueries. ({issue}`25138`)
* Fix incorrect results when passing an array that contains nulls to
  `cosine_distance` and `cosine_similarity`. ({issue}`25195`)
* Prevent improper use of `WITH SESSION` with non-`SELECT` queries. ({issue}`25112`)

## JDBC driver

* Provide a `javax.sql.DataSource` implementation. ({issue}`24985`)
* Fix roles being cleared after invoking `SET SESSION AUTHORIZATION` or 
  `RESET SESSION AUTHORIZATION`. ({issue}`25191`)

## Docker image

* Improve performance when using Snappy compression. ({issue}`25143`)
* Fix initialization failure for the DuckDB connector. ({issue}`25143`)

## BigQuery connector

* Improve performance of listing tables when
  `bigquery.case-insensitive-name-matching` is enabled. ({issue}`25222`)

## Delta Lake connector

* Improve support for highly concurrent table modifications. ({issue}`25141`)

## Faker connector

* Add support for the `row` type and generate empty values for `array`, `map`,
  and `json` types. ({issue}`25120`)

## Iceberg connector

* Add the `$partition` hidden column. ({issue}`24301`)
* Fix incorrect results when reading Iceberg tables after deletes were
  performed. ({issue}`25151`)

## Loki connector

* Fix connection failures with Loki version higher than 3.2.0. ({issue}`25156`)

## PostgreSQL connector

* Improve performance for queries involving cast of
  [integer types](integer-data-types). ({issue}`24950`)

## SPI

* Remove the deprecated `ConnectorMetadata.addColumn(ConnectorSession session,
  ConnectorTableHandle tableHandle, ColumnMetadata column)` method. Use the
  `ConnectorMetadata.addColumn(ConnectorSession session, ConnectorTableHandle
  tableHandle, ColumnMetadata column, ColumnPosition position)` instead.
  ({issue}`25163`)
