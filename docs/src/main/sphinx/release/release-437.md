# Release 437 (24 Jan 2024)

## General

* Add support for `char(n)` values in {func}`to_utf8`. ({issue}`20158`)
* Add support for `char(n)` values in {func}`lpad`. ({issue}`16907`)
* {{breaking}} Replace the `exchange.compression-enabled` configuration property
  and `exchange_compression` session property with
  [the `exchange.compression-codec`and `exchange_compression_codec` properties](prop-exchange-compression-codec),
  respectively. ({issue}`20274`)
* {{breaking}} Replace the `spill-compression-enabled` configuration property 
  with [the `spill-compression-codec` property](prop-spill-compression-codec). ({issue}`20274`)
* {{breaking}} Remove the deprecated `experimental.spill-compression-enabled`
  configuration property. ({issue}`20274`)
* Fix failure when invoking functions that may return null values. ({issue}`18456`)
* Fix `ArrayIndexOutOfBoundsException` with RowBlockBuilder during output
  operations. ({issue}`20426`)

## Delta Lake connector

* Improve query performance for queries that don't use table statistics. ({issue}`20054`)

## Hive connector

* Fix error when coercing union-typed data to a single type when reading Avro
  files. ({issue}`20310`)

## Iceberg connector

* Fix materialized views being permanently stale when they reference
  [table functions](/functions/table). ({issue}`19904`)
* Improve performance of queries with filters on `ROW` columns stored in Parquet
  files. ({issue}`17133`)
