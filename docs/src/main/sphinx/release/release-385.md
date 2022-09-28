# Release 385 (8 Jun 2022)

## General

* Add the `json_array` and `json_object` [JSON functions](/functions/json). ({issue}`9081`)
* Support all types that can be cast to `varchar` as parameters for the [JSON
  path](json-path-language). ({issue}`12682`)
* Allow `CREATE TABLE LIKE` clause on a table from a different catalog if
  explicitly excluding table properties. ({issue}`3171`)
* Reduce `Exceeded limit of N open writers for partitions` errors when
  fault-tolerant execution is enabled. ({issue}`12721`)

## Delta Lake connector

* Add support for the [appendOnly field](https://docs.delta.io/latest/delta-batch.html#-table-properties). ({issue}`12635`)
* Add support for column comments when creating a table or a column. ({issue}`12455`, {issue}`12715`)

## Hive connector

* Allow cancelling a query on a transactional table if it is waiting for a lock. ({issue}`11798`)
* Add support for selecting a compression scheme when writing Avro files via the
  `hive.compression-codec` config property or the `compression_codec` session
  property. ({issue}`12639`)

## Iceberg connector

* Improve query performance when a table consists of many small files. ({issue}`12579`)
* Improve query performance when performing a delete or update. ({issue}`12671`)
* Add support for the `[VERSION | TIMESTAMP] AS OF` clause. ({issue}`10258`)
* Show Iceberg location and `format_version` in `SHOW CREATE MATERIALIZED VIEW`. ({issue}`12504`)

## MariaDB connector

* Add support for `timestamp(p)` type. ({issue}`12200`)

## TPC-H connector

* Fix query failure when reading the `dbgen_version` table. ({issue}`12673`)
