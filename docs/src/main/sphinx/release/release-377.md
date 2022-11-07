# Release 377 (13 Apr 2022)

## General

* Add support for standard SQL `trim` syntax. ({issue}`11236`)
* Fix incorrect results when queries execute in fault-tolerant mode. ({issue}`11870`)

## Hive connector

* Add support for `date` type partition names with timestamp formatting. ({issue}`11873`)
* Improve performance of queries that use Glue metadata. ({issue}`11869`)
* Fix failure of the `sync_partition_metadata` procedure when partition names
  differ from partition paths on the file system. ({issue}`11864`)

## Iceberg connector

* Support setting Glue metastore catalog identifier with the
  `hive.metastore.glue.catalogid` catalog configuration property. ({issue}`11520`)
* Add support for materialized views when using Glue metastore. ({issue}`11780`)

## Kafka connector

* Add support for additional Kafka client properties specified with the
  `kafka.config.resources` catalog configuration property. ({issue}`8743`)

## SQL Server connector

* Improve performance of queries involving joins by pushing computation to the
  SQL Server database. ({issue}`11637`)
