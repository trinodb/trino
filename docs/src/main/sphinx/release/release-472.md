# Release 472 (5 Mar 2025)

## General

* Color the console output for improved log readability. ({issue}`25090`)
* Suggest configuration for unused properties in error message. ({issue}`25090`)
* Suggest valid session property names in error message. ({issue}`25133`)
* {{breaking}} Rename HTTP client property prefixes from `workerInfo` and
  `memoryManager` to `worker-info` and `memory-manager`. ({issue}`25099`)
* Limit `WITH SESSION` scope to `SELECT` queries. ({issue}`25112`)
* Add support for a large number of projections. ({issue}`25040`)
* Improve performance of certain queries involving topN over a projection. ({issue}`#25138`)
* Fix null handling in `cosine_distance` and `cosine_similarity` to return null
  when array contains null. ({issue}`25195`)

## JDBC driver

* Implement `DataSource` for JDBC driver. ({issue}`24985`)
* Prevent clearing of roles after resetting authorization. ({issue}`25191`)

## Docker image

* Fix failures of DuckDB connector use. ({issue}`25143`)
* Enable native compression use for Snappy. ({issue}`25143`)

## Delta Lake connector

* Improve support for highly concurrent table modifications. ({issue}`25141`)

## Faker connector

* Add support for the `ROW` type and generate empty values for `ARRAY`, `MAP`,
  and `JSON` types. ({issue}`25120`)

## Hive connector

* Enable Athena partition projection with
  `hive.partition-projection-enabled` set to `true` by default. ({issue}`25079`)

## Iceberg connector

* Add the `$partition` hidden column. ({issue}`24301`)
* Fix incorrect results for reads on Iceberg tables with deletes. ({issue}`25151`)

## Loki connector

* Fix connection failures with Loki version higher than 3.2.0. ({issue}`25156`)

## PostgreSQL connector

* Support integral cast projection pushdown. ({issue}`24950`)

## SPI

* Remove the deprecated `ConnectorMetadata.addColumn(ConnectorSession session,
  ConnectorTableHandle tableHandle, ColumnMetadata column)` method. Use the
  `ConnectorMetadata.addColumn(ConnectorSession session, ConnectorTableHandle
  tableHandle, ColumnMetadata column, ColumnPosition position)` instead.
  ({issue}`25163`)
