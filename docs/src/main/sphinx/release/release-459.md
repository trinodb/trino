# Release 459 (25 Sep 2024)

## General

* Fix possible query failure when `retry_policy` is set to `TASK` and when
  adaptive join reordering is enabled. ({issue}`23407`)

## Docker image

* Update Java runtime to Java 23. ({issue}`23482`)

## CLI

* Display data sizes and rates with binary (1024-based) abbreviations such as
  `KiB` and `MiB`. Add flag `--decimal-data-size` to use decimal (1000-based)
  values and abbreviations such as `KB` and `MB`. (#13054)

## BigQuery connector

* Improve performance of queries that access only a subset of fields from nested
  data. ({issue}`23443`)
* Fix query failure when the `bigquery.service-cache-ttl` property isn't `0ms`
  and case insensitive name matching is enabled. ({issue}`23481`)

## ClickHouse connector

* Improve performance for queries involving conditions with `varchar` data. ({issue}`23516`)

## Delta Lake connector

* Allow configuring maximum concurrent HTTP requests to Azure on every node in
  [](/object-storage/file-system-azure) with `azure.max-http-requests`.
  ({issue}`22915`)
* Add support for WASB to [](/object-storage/file-system-azure). ({issue}`23511`)
* Allow disabling caching of Delta Lake transaction logs when file system caching
  is enabled with the `delta.fs.cache.disable-transaction-log-caching` property. ({issue}`21451`)
* Improve cache hit ratio for the [](/object-storage/file-system-cache). ({issue}`23172`)
* Fix incorrect results when writing [deletion
  vectors](https://docs.delta.io/latest/delta-deletion-vectors.html). ({issue}`23229`)
* Fix failures for queries with containing aggregations with a `DISTINCT`
  clause on metadata tables. ({issue}`23529`)

## Elasticsearch connector

* Fix failures for `count(*)` queries with predicates containing non-ASCII
  strings. ({issue}`23425`)

## Hive connector

* Allow configuring maximum concurrent HTTP requests to Azure on every node in
  [](/object-storage/file-system-azure) with `azure.max-http-requests`.
  ({issue}`22915`)
* Add support for WASB to [](/object-storage/file-system-azure). ({issue}`23511`)
* Improve cache hit ratio for the [](/object-storage/file-system-cache). ({issue}`23172`)
* Fix failures for queries with containing aggregations with a `DISTINCT`
  clause on metadata tables. ({issue}`23529`)

## Hudi connector

* Allow configuring maximum concurrent HTTP requests to Azure on every node in
  [](/object-storage/file-system-azure) with `azure.max-http-requests`.
  ({issue}`22915`)
* Add support for WASB to [](/object-storage/file-system-azure). ({issue}`23511`)
* Fix failures for queries with containing aggregations with a `DISTINCT`
  clause on metadata tables. ({issue}`23529`)

## Iceberg connector

* Allow configuring maximum concurrent HTTP requests to Azure on every node in
  [](/object-storage/file-system-azure) with `azure.max-http-requests`.
  ({issue}`22915`)
* Add support for WASB to [](/object-storage/file-system-azure). ({issue}`23511`)
* Improve cache hit ratio for the [](/object-storage/file-system-cache). ({issue}`23172`)
* Fix failures for queries with containing aggregations with a `DISTINCT`
  clause on metadata tables. ({issue}`23529`)

## Local file connector

* {{breaking}} Remove the local file connector. ({issue}`23556`)

## OpenSearch connector

* Fix failures for `count(*)` queries with predicates containing non-ASCII
  strings. ({issue}`23425`)

## SPI

* Add `ConnectorAccessControl` argument to the
  `ConnectorMetadata.getTableHandleForExecute` method. ({issue}`23524`)