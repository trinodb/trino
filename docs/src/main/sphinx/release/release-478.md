# Release 478 (29 Oct 2025)

## General

* Include lineage information for columns used in `UNNEST` expressions. ({issue}`16946`)
* Add support for limiting which retry policies a user can select. This can be configured using
  the `retry-policy.allowed` option. ({issue}`26628`)
* Add support for loading plugins from multiple directories. ({issue}`26855`)
* Allow dropping catalogs that failed to load correctly. ({issue}`26918`)
* Improve performance of queries with an `ORDER BY` clause using `varchar` or `varbinary` types. ({issue}`26725`)
* Improve performance of `MERGE` statements involving a `NOT MATCHED` case. ({issue}`26759`)
* Improve performance of queries involving `JOIN` when the join spills to disk. ({issue}`26076`)
* Fix potential incorrect results when query uses `row` type. ({issue}`26806`)
* Include catalogs that failed to load in the `metadata.catalogs` table. ({issue}`26918`)
* Fix `EXPLAIN ANALYZE` planning so that it executes with the same plan as would be used to execute the query
  being analyzed. ({issue}`26938`)
* Fix incorrect results when using logical navigation function `FIRST` in row pattern recognition. ({issue}`26981`)

## Security

* Propagate `queryId` to the [Open Policy Agent](/security/opa-access-control)
  authorizer. ({issue}`26851`)

## Docker image

* Run Trino on JDK 25.0.0 (build 36). ({issue}`26693`)

## Delta Lake connector

* Fix failure when reading a `map(..., json)` column when the map item value is `NULL`. ({issue}`26700`)
* Deprecate the `gcs.use-access-token` configuration property. Use `gcs.auth-type` instead. ({issue}`26681`)

## Google Sheets connector

* Fix potential query failure when the `gsheets.delegated-user-email` configuration property
  is used. ({issue}`26501`)

## Hive connector

* Add support for reading encrypted Parquet files. ({issue}`24517`, {issue}`9383`)
* Deprecate the `gcs.use-access-token` configuration property. Use `gcs.auth-type` instead. ({issue}`26681`)
* Improve performance of queries using complex predicates on `$path` column. ({issue}`27000`)
* Fix writing ORC files to ensure that dates and timestamps before `1582-10-15` are read correctly by Apache Hive. ({issue}`26507`)
* Fix `flush_metadata_cache` procedure failure when metastore impersonation is enabled. ({issue}`27059`)

## Hudi connector

* Deprecate the `gcs.use-access-token` configuration property. Use `gcs.auth-type` instead. ({issue}`26681`)

## Iceberg connector

* Improve performance when writing sorted tables and `iceberg.sorted-writing.local-staging-path`
  is set. ({issue}`24376`)
* Improve performance of `ALTER TABLE EXECUTE OPTIMIZE` on tables with bucket transform partitioning. ({issue}`27104`)
* Return execution metrics while running the `remove_orphan_files` command. ({issue}`26661`)
* Deprecate the `gcs.use-access-token` configuration property. Use `gcs.auth-type` instead. ({issue}`26681`)
* Collect distinct values count on all columns when replacing tables. ({issue}`26983`)
* Fix failure due to column count mismatch when executing the `add_files_from_table`
  procedure. ({issue}`26774`)
* Fix failure when executing `optimize_manifests` on tables without a snapshot. ({issue}`26970`)
* Fix incorrect results when reading Avro files migrated from Hive. ({issue}`26863`)
* Fix failure when executing `SHOW CREATE SCHEMA` on a schema with unsupported properties
  with REST, Glue or Nessie catalog. ({issue}`24744`)
* Fix failure when running `EXPLAIN` or `EXPLAIN ANALYZE` on `OPTIMIZE` command. ({issue}`26598`)

## Kafka connector

* Fix failure when filtering partitions by timestamp offset. ({issue}`26787`)

## SPI

* Remove default implementation from `Connector.shutdown()`. ({issue}`26718`)
* Remove the deprecated `ConnectorSplit.getSplitInfo` method. ({issue}`27063`)
* Deprecate `io.trino.spi.type.Type#appendTo` method. ({issue}`26922`)
