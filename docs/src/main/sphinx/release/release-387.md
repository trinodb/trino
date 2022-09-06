# Release 387 (22 Jun 2022)

## General

* Add support for query parameters in table function arguments. ({issue}`12910`)
* Update minimum required Java version to 11.0.15. ({issue}`12841`)
* Fix incorrect result for {func}`to_iso8601` when the timestamp is in the
  daylight savings transition region. ({issue}`11619`)

## CLI

* Fix query history not being stored when a query starts with whitespace. ({issue}`12847`)

## Delta Lake connector

* Record table size when analyzing a table. ({issue}`12814`)
* Enable the optimized Parquet writer by default. This can be disabled via the
  `parquet.experimental-optimized-writer.enabled` configuration property. ({issue}`12757`)
* Disallow adding a new column to a table that has been written with an
  unsupported writer. ({issue}`12883`)

## Hive connector

* Add support for ORC bloom filters on `varchar` columns. ({issue}`11757`)

## Iceberg connector

* Allow `OPTIMIZE` on a table partitioned on a `timestamp with time zone` column
  when using `CAST(timestamp_col AS date) >= DATE '...'` syntax. ({issue}`12362`)
* Allow `OPTIMIZE` with a predicate on a table that does not have identity
  partitioning. ({issue}`12795`)
* Improve performance of `DELETE` when deleting whole partitions from a table
  that does not have identity partitioning. ({issue}`7905`)
* Fix incorrect results when a query contains a filter on a `UUID` column. ({issue}`12834`)
* Fail queries that attempt to modify old snapshots. ({issue}`12860`)
* Deprecate using synthetic `@`-based syntax for Iceberg snapshot access in
  favor of the `AS OF` syntax. The old behavior can be restored by setting the
  `allow_legacy_snapshot_syntax` session property or
  `iceberg.allow-legacy-snapshot-syntax` configuration property. ({issue}`10768`)

## Kudu connector

* Fix failure when inserting into a table with a `row_uuid` column. ({issue}`12915`)

## Pinot connector

* Add support for querying Pinot via the gRPC endpoint. ({issue}`9296 `)

## Redis connector

* Add support for predicate pushdown on columns of type `string`.  ({issue}`12218`)

## SPI

* Add information about query retry policy to `QueryCompletedEvent` and
  `QueryCreatedEvent`. ({issue}`12898`)
