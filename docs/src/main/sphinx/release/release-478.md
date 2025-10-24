# Release 478 (22 Oct 2025)

## General

* Add support for column lineage in `UNNEST` clauses. ({issue}`16946`)
* Add `retry-policy.allowed` configuration property to specify which query retry
  policies can be selected by the user. ({issue}`26628`)
* Add support for loading plugins from multiple directories. ({issue}`26855`)
* Add the `/v1/integrations/gateway` endpoint that exposes additional cluster metrics for Trino Gateway.
  ({issue}`26548`)
* Allow dropping an uninitialized catalog that failed to load. ({issue}`26918`)
* Improve performance of queries with an `ORDER BY` clause using `varchar` or `varbinary` types. ({issue}`26725`)
* Improve performance of `MERGE` statements involving a `NOT MATCHED` case. ({issue}`26759`)
* Improve performance of queries with joins which spill to disk. ({issue}`26076`)
* Fix potential incorrect results when reading `row` type. ({issue}`26806`)
* Return all catalogs, including uninitialized ones, for queries from `metadata.catalogs`. ({issue}`26918`)
* Fix `EXPLAIN ANALYZE` planning so that it executes with the same plan as would be used to execute the query
  being analyzed. ({issue}`26938`)
* Fix row pattern matching logical navigations in running semantics to be always constraint to current match.
  Previously, a logical navigation function such as `FIRST` could return position outside of the current match.
  ({issue}`26981`)

## Security

* Propagate `queryId` to the [Open Policy Agent](/security/opa-access-control)
  authorizer. ({issue}`26851`)

## Docker image

* Run Trino on JDK 25.0.0 (build 36). ({issue}`26693`)

## Delta Lake connector

* Fix failure when reading `map` type with `json` value type when a value is `NULL`. ({issue}`26700`)
* Deprecate the `gcs.use-access-token` configuration property. Use `gcs.auth-type` instead. ({issue}`26681`)

## Google Sheets connector

* Fix potential query failure when the `gsheets.delegated-user-email` configuration property
  is used. ({issue}`26501`)

## Hive connector

* Add support for reading encrypted Parquet files. ({issue}`24517`, {issue}`9383`)
* Deprecate the `gcs.use-access-token` configuration property. Use `gcs.auth-type` instead. ({issue}`26681`)
* Improve performance of queries using complex predicates on `$path` column. ({issue}`27000`)
* Fix writing invalid dates and timestamps before `1582-10-15` when writing ORC files by setting calendar type
  in the file footer. Previously, Trino did not set the calendar type in the file footer, and values
  before `1582-10-15` could be read incorrectly by other query engines such as Apache Hive.  ({issue}`26507`)

## Hudi connector

* Deprecate the `gcs.use-access-token` configuration property. Use `gcs.auth-type` instead. ({issue}`26681`)

## Iceberg connector

* Improve performance when writing sorted tables and `iceberg.sorted-writing.local-staging-path`
 is set. ({issue}`24376`)
* Return execution metrics while running the `remove_orphan_files` command. ({issue}`26661`)
* Deprecate the `gcs.use-access-token` configuration property. Use `gcs.auth-type` instead. ({issue}`26681`)
* Collect distinct values count on all columns when replacing tables. ({issue}`26983`)
* Fix failure due to column count mismatch when executing the `add_files_from_table`
  procedure. ({issue}`26774`)
* Fix failure when executing `optimize_manifests` on tables without a snapshot. ({issue}`26970`)
* Fix incorrect results when reading Avro files migrated from Hive. ({issue}`26863`)
* Fix failure when executing `SHOW CREATE SCHEMA` on a schema with unsupported properties
  with REST, Glue or Nessie catalog. ({issue}`24744`)

## Kafka connector

* Fix failure when filtering partitions by timestamp offset. ({issue}`26787`)

## SPI

* Remove default implementation from `Connector.shutdown()`. ({issue}`26718`)
* Deprecate `io.trino.spi.type.Type#appendTo` method. ({issue}`26922`)
