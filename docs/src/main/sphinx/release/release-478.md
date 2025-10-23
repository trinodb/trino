# Release 478 (22 Oct 2025)

## General

* Add support for column lineage in `UNNEST` clauses. ({issue}`16946`)
* Add `retry-policy.allowed` configuration property to specify which retry
  policies can be selected by the user. ({issue}`26628`)
* Add support for loading plugins from multiple directories. ({issue}`26855`)
* Add the `/v1/integrations/gateway` endpoint for integration with Trino Gateway. ({issue}`26548`)
* Allow dropping an uninitialized catalog that failed to load. ({issue}`26918`)
* Improve performance of queries with an `ORDER BY` clause. ({issue}`26725`)
* Improve performance of `MERGE` statements involving a `NOT MATCHED` case. ({issue}`26759`)
* Improve performance of queries with joins which spill to disk. ({issue}`26076`)
* Fix potential incorrect results when reading `row` type. ({issue}`26806`)
* Return all catalogs, including uninitialized ones, for queries from `metadata.catalogs`. ({issue}`26918`)
* Ensure that queries with and without `EXPLAIN ANALYZE` are planned identically. ({issue}`26938`)
* In row pattern matching, restrict logical navigations to current match in running semantics. ({issue}`26981`)

## Security

* Propagate `queryId` to the [Open Policy Agent](/security/opa-access-control)
  authorizer. ({issue}`26851`)

## Web UI

* Add support for filtering queries by `X-Trino-Trace-Token` value in the [](/admin/preview-web-interface). ({issue}`26447`)
* Improve rendering performance of large query JSON in the [](/admin/preview-web-interface). ({issue}`26807`)
* Fix rendering of large query plans in the [](/admin/preview-web-interface). ({issue}`26749`)
* Fix rendering of the splits timeline for queued queries in the [](/admin/preview-web-interface) to prevent
  blank screen ({issue}`26920`)

## Docker image

* Run Trino on JDK 25.0.0 (build 36). ({issue}`26693`)

## Delta Lake connector

* Fix failure when reading `map` type with value type is `json` and value is `NULL`. ({issue}`26700`)
* Deprecate the `gcs.use-access-token` configuration property. Use `gcs.auth-type` instead. ({issue}`26681`)

## Google Sheets connector

* Fix potential query failure when the `gsheets.delegated-user-email` configuration property
  is used. ({issue}`26501`)

## Hive connector

* Add support for reading encrypted Parquet files. ({issue}`24517`, {issue}`9383`)
* Deprecate the `gcs.use-access-token` configuration property. Use `gcs.auth-type` instead. ({issue}`26681`)
* Improve performance of queries using complex predicates on `$path` column. ({issue}`27000`)
* Prevent writing invalid dates and timestamps before `1582-10-15` by the ORC writer. ({issue}`26507`)

## Hudi connector

* Deprecate the `gcs.use-access-token` configuration property. Use `gcs.auth-type` instead. ({issue}`26681`)

## Iceberg connector

* Improve performance when writing sorted tables and `iceberg.sorted-writing.local-staging-path`
 is set. ({issue}`24376`)
* Return execution metrics while running the `remove_orphan_files` procedure. ({issue}`26661`)
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

* Require `shutdown` to be implemented by the `Connector`. ({issue}`26718`)
* Deprecate `io.trino.spi.type.Type#appendTo` method. ({issue}`26922`)
