# Release 452 (11 Jul 2024)

## General

* Add [](/connector/exasol). ({issue}`16083`)
* Add support for processing the `X-Forwarded-Prefix` header when the
  `http-server.process-forwarded` property is enabled. ({issue}`22227`)
* Add support for the {func}`euclidean_distance`, {func}`dot_product`, and
  {func}`cosine_distance` functions. ({issue}`22397`)
* Improve performance of queries with selective joins by performing fine-grained
  filtering of rows using dynamic filters. This behavior is enabled by default
  and can be disabled using the `enable-dynamic-row-filtering` configuration
  property or the `enable_dynamic_row_filtering` session property. ({issue}`22411`)
* Fix sporadic query failure when the `retry_policy` property is set to `TASK`. ({issue}`22617`)

## Web UI

* Fix query plans occasionally not rendering the stage details page. ({issue}`22542`)

## BigQuery connector

* Add support for using the
  [BigQuery Storage Read API](https://cloud.google.com/bigquery/docs/reference/storage)
  when using the [`query` table function](bigquery-query-function). ({issue}`22432`)

## Black Hole connector

* Add support for adding, dropping and renaming columns. ({issue}`22620`)

## ClickHouse connector

* Add [`query` table function](clickhouse-query-function) for full query
  pass-through to ClickHouse. ({issue}`16182`)

## Delta Lake connector

* Add support for type coercion when adding new columns. ({issue}`19708`)
* Improve performance of reading from Parquet files with large schemas. ({issue}`22434`)
* Fix incorrect results when reading `INT32` values in Parquet files as
  `varchar` or `decimal` types in Trino. ({issue}`21556`)
* Fix a performance regression when using the native filesystem for Azure. ({issue}`22561`)

## Hive connector

* Add support for changing column types for structural data types for
  non-partitioned tables using ORC files. ({issue}`22326`)
* Add support for type coercion when adding new columns. ({issue}`19708`)
* Add support for changing a column's type from `varbinary` to `varchar`. ({issue}`22322`)
* Improve performance of reading from Parquet files with large schemas. ({issue}`22434`)
* Fix incorrect results when reading `INT32` values in Parquet files as
  `varchar` or `decimal` types in Trino. ({issue}`21556`)
* Fix `sync_partition_metadata` ignoring case-sensitive variations of partition
  names in storage. ({issue}`22484`)
* Fix a performance regression when using the native filesystem for Azure. ({issue}`22561`)

## Hudi connector

* Improve performance of reading from Parquet files with large schemas. ({issue}`22434`)
* Fix incorrect results when reading `INT32` values in Parquet files as
  `varchar` or `decimal` types in Trino. ({issue}`21556`)
* Fix a performance regression when using the native filesystem for Azure. ({issue}`22561`)

## Iceberg connector

* Add support for type coercion when adding new columns. ({issue}`19708`)
* Improve performance of reading from Parquet files with a large number of
  columns. ({issue}`22434`)
* Fix files being deleted when dropping tables with the Nessie catalog. ({issue}`22392`)
* Fix incorrect results when reading `INT32` values in Parquet files as
  `varchar` or `decimal` types in Trino. ({issue}`21556`)
* Fix failure when hidden partition names conflict with other columns. ({issue}`22351`)
* Fix failure when reading tables with `null` on partition columns while the
  `optimize_metadata_queries` session property is enabled. ({issue}`21844`)
* Fix failure when listing views with an unsupported dialect in the REST
  catalog. ({issue}`22598`)
* Fix a performance regression when using the native filesystem for Azure. ({issue}`22561`)

## Kudu connector

* Fix failure when adding new columns with a `decimal` type. ({issue}`22558`)

## Memory connector

* Add support for adding new columns. ({issue}`22610`)
* Add support for renaming columns. ({issue}`22607`)
* Add support for the `NOT NULL` constraint. ({issue}`22601`)

## PostgreSQL connector

* Improve performance of the {func}`reverse` function by pushing down execution
  to the underlying database. ({issue}`22203`)
