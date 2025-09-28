# Release 478 (dd Oct 2025)

## General

* Add support for column lineage in `UNNEST` clause. ({issue}`16946`)
* Add `allowed-retry-policies` configuration property to specify which retry
  policies can be selected by user. ({issue}`26628`)
* Add support for loading plugins from multiple directories. ({issue}`26855`)
* Improve performance of queries with `ORDER BY`. ({issue}`26725`)
* Fix potential incorrect results when reading `row` type. ({issue}`26806`)

## Security

* Propagate `queryId` to [Open Policy Agent](/security/opa-access-control)
  authorizer. ({issue}`26851`)

## Web UI

* Improve rendering performance of large query JSON code in [](/admin/preview-web-interface). ({issue}`26807`)
* Fix query plan default viewport in [](/admin/preview-web-interface). ({issue}`26749`)

## JDBC driver

* Add support for zstd, brotli, and gzip compressions. ({issue}`26875`)

## Docker image

* Run Trino on JDK 25.0.0 (build 36). ({issue}`26693`)

## CLI

* Add support for zstd, brotli, and gzip compressions. ({issue}`26875`)

## BigQuery connector

## Blackhole connector

## Cassandra connector

## ClickHouse connector

## Delta Lake connector

* Fix failure when reading `NULL` map on `json` type. ({issue}`26700`)

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
* Fix ORC writer to ensure that dates and timestamps older than 1582 are read 
  correctly by Apache Hive. ({issue}`26507`)

## Hudi connector

## Iceberg connector

* Improve performance when writing sorted tables and the `iceberg.sorted-writing.local-staging-path`
  config option is set. ({issue}`24376`)
* Fix failure due to column count mismatch when executing `add_files_from_table`
  procedure. ({issue}`26774`)
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
