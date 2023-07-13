# Release 415 (28 Apr 2023)

## General

* Improve performance of aggregations with variable file sizes. ({issue}`11361`)
* Perform missing permission checks for table arguments to table functions. ({issue}`17279`)

## Web UI

* Add CPU planning time to the query details page. ({issue}`15318`)

## Delta Lake connector

* Add support for commenting on tables and columns with an `id` and `name`
  column mapping mode. ({issue}`17139`)
* Add support for `BETWEEN` predicates in table check constraints. ({issue}`17120`)

## Hive connector

* Improve performance of queries with selective filters on primitive fields in
  `row` columns. ({issue}`15163`)

## Iceberg connector

* Improve performance of queries with filters when Bloom filter indexes are
  present in Parquet files. ({issue}`17192`)
* Fix failure when trying to use `DROP TABLE` on a corrupted table. ({issue}`12318`)

## Kafka connector

* Add support for Protobuf `oneof` types when using the Confluent table
  description provider. ({issue}`16836`)

## SPI

* Expose ``planningCpuTime`` in ``QueryStatistics``. ({issue}`15318`)
