# Release 425 (24 Aug 2023)

## General

* Improve performance of `GROUP BY`. ({issue}`18106`)
* Fix incorrect reporting of cumulative memory usage. ({issue}`18714`)

## BlackHole connector

* Remove support for materialized views. ({issue}`18628`)

## Delta Lake connector

* Add support for check constraints in `MERGE` statements. ({issue}`15411`)
* Improve performance when statistics are missing from the transaction log. ({issue}`16743`)
* Improve memory usage accounting of the Parquet writer. ({issue}`18756`)
* Improve performance of `DELETE` statements when they delete the whole table or 
  when the filters only apply to partition columns. ({issue}`18332 `)

## Hive connector

* Add support for `CASCADE` option in `DROP SCHEMA` statements. ({issue}`18320`)
* Create a new directory if the specified external location for a new table does
  not exist. ({issue}`17920`)
* Improve memory usage accounting of the Parquet writer. ({issue}`18756`)
* Improve performance of writing to JSON files. ({issue}`18683`)

## Iceberg connector

* Improve memory usage accounting of the Parquet writer. ({issue}`18756`)

## Kudu connector

* Add support for `CASCADE` option in `DROP SCHEMA` statements. ({issue}`18629`)

## MongoDB connector

* Add support for the `Decimal128` MongoDB type. ({issue}`18722`)
* Add support for `CASCADE` option in `DROP SCHEMA` statements. ({issue}`18629`)
* Fix query failure when reading the value of `-0` as a `decimal` type. ({issue}`18777`)
