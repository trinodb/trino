# Release 478 (dd Oct 2025)

## General

* Add support for column lineage in `UNNEST` clause. ({issue}`16946`)
* Add `allowed-retry-policies` configuration property to specify which retry
  policies can be selected by user. ({issue}`26628`)
* Add support for loading plugins from multiple directories. ({issue}`26855`)
* Add `/v1/integrations/gateway` endpoint for integration with Trino Gateway. ({issue}`26548`)
* Allow to drop catalog that failed to load correctly. ({issue}`26918`)
* Improve performance of queries with `ORDER BY`. ({issue}`26725`)
* Improve performance of `MERGE` statement involving `NOT MATCHED` case. ({issue}`26759`)
* Improve performance of spilling join queries. ({issue}`26076`)
* Fix potential incorrect results when reading `row` type. ({issue}`26806`)
* Make `metadata.catalogs` table return all catalogs. ({issue}`26918`)
* Ensure that queries with and without `EXPLAIN ANALYZE` are planned in the same
  way. ({issue}`26938`)

## Security

* Propagate `queryId` to [Open Policy Agent](/security/opa-access-control)
  authorizer. ({issue}`26851`)

## Web UI

* Add support for filtering queries by `X-Trino-Trace-Token` value in [](/admin/preview-web-interface). ({issue}`26447`)
* Improve rendering performance of large query JSON in [](/admin/preview-web-interface). ({issue}`26807`)
* Fix rendering of a large query plans in [](/admin/preview-web-interface). ({issue}`26749`)
* Fix rendering of splits timeline for queued queries in [](/admin/preview-web-interface) to prevent
  blank screen ({issue}`26920`)

## JDBC driver


## Docker image

* Run Trino on JDK 25.0.0 (build 36). ({issue}`26693`)

## CLI


## BigQuery connector

## Blackhole connector

## Cassandra connector

## ClickHouse connector

## Delta Lake connector

* Fix failure when reading `NULL` map on `json` type. ({issue}`26700`)
* Deprecate `gcs.use-access-token` in favor of `gcs.auth-type` config property. ({issue}`26681`)

## Druid connector

## DuckDB connector

## Elasticsearch connector

## Exasol connector

## Faker connector

## Google Sheets connector

* Fix potential query failure when `gsheets.delegated-user-email` config property
  is used. ({issue}`26501`)

## Hive connector

* Add support for reading encrypted Parquet files. ({issue}`24517`, {issue}`9383`)
* Deprecate `gcs.use-access-token` in favor of `gcs.auth-type` config property. ({issue}`26681`)
* Improve performance of queries using complex predicates on `$path` column. ({issue}`27000`)
* Fix ORC writer to ensure that dates and timestamps older than `1582-10-15` are
  read correctly by Apache Hive. ({issue}`26507`)

## Hudi connector

* Deprecate `gcs.use-access-token` in favor of `gcs.auth-type` config property. ({issue}`26681`)

## Iceberg connector

* Improve performance when writing sorted tables and the `iceberg.sorted-writing.local-staging-path`
  config option is set. ({issue}`24376`)
* Return execution metrics while running `remove_orphan_files` procedure. ({issue}`26661`)
* Deprecate `gcs.use-access-token` in favor of `gcs.auth-type` config property. ({issue}`26681`)
* Fix failure due to column count mismatch when executing `add_files_from_table`
  procedure. ({issue}`26774`)
* Fix failure when executing `optimize_manifests` on tables without a snapshot. ({issue}`26970`)
* Fix incorrect results when reading Avro files migrated from Hive. ({issue}`26863`)

## Ignite connector

## JMX connector

## Kafka connector

* Fix failure when filtering partitions by timestamp offset. ({issue}`26787`)

## Loki connector

## MariaDB connector

## Memory connector

## MongoDB connector

## MySQL connector

## OpenSearch connector

## Oracle connector

## Pinot connector

## PostgreSQL connector

## Prometheus connector

## Redis connector

## Redshift connector

## SingleStore connector

## Snowflake connector

## SQL Server connector

## TPC-H connector

## TPC-DS connector

## Vertica connector

## SPI

* Require `shutdown` to be implemented by the `Connector`. ({issue}`26718`)
* Deprecate `io.trino.spi.type.Type#appendTo` method. ({issue}`26922`)
