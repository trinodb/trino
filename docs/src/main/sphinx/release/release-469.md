# Release 469 (8 Jan 2025)

## General

* Refrain from join pushdown for modified tables. ({issue}`24447`)
* Fix parsing of negative `0x`, `0b`, `0o` long literals. ({issue}`24601`)

## JDBC driver

* Add `physicalInputTimeMillis` to `io.trino.jdbc.QueryStats`. ({issue}`24604`)
* Fix loading spooled segment data when cluster is secured. ({issue}`24595`)

## CLI

* Fix loading spooled segment data when cluster is secured. ({issue}`24595`)

## Delta Lake connector

* Add support for SSE-C in S3 security mapping. ({issue}`24566`)
* Improve performance of reading from new Delta Lake table data by compressing
  files with `ZSTD` by default, instead of the previous `SNAPPY`.
  ({issue}`17426`)
* Improve performance of queries on tables with large transaction log JSON
  files. ({issue}`24491`)
* Fix JMX metrics for file system caching with multiple catalogs. ({issue}`24510`)
* Fix reading from tables when using the Alluxio file system. ({issue}`23815`)

## Faker connector

* Add support for views. ({issue}`24242`)
* Support generating sequences. ({issue}`24590`)
* {{breaking}} Replace predicate pushdown with `min`, `max`, and `options`
  column properties. ({issue}`24147`)

## Hive connector

* Add support for SSE-C in S3 security mapping. ({issue}`24566`)
* Fix reporting of AWS SDK client retry count metrics to report both client
  level logical retries and lower level HTTP client retries separately. ({issue}`24606`)
* Fix JMX metrics for file system caching with multiple catalogs. ({issue}`24510`)
* Fix reading from tables when using the Alluxio file system. ({issue}`23815`)

## Hudi connector

* Add support for SSE-C in S3 security mapping. ({issue}`24566`)

## Iceberg connector

* Add support for SSE-C in S3 security mapping. ({issue}`24566`)
* Add `$entries` metadata table. ({issue}`24172`)
* Add `$all_entries` metadata table. ({issue}`24543`)
* Add `system.iceberg_tables` table function to allow listing only Iceberg tables. ({issue}`24469`)
* Allow configuring the `parquet_bloom_filter_columns` table property. ({issue}`24573`)
* Improve performance when listing columns. ({issue}`23909`)
* Remove the oldest tracked version metadata files when
  `write.metadata.delete-after-commit.enabled` is set to `true`. ({issue}`19582`)
* Fix JMX metrics for file system caching with multiple catalogs. ({issue}`24510`)
* Fix reading from tables when using the Alluxio file system. ({issue}`23815`)

## Ignite connector

* Add support for `MERGE` statements. ({issue}`24443`)

## Redshift connector

* Improve performance of reading from Redshift tables by using the Redshift
  `UNLOAD` command. ({issue}`24117`)

## SPI

* Remove support for connector-level event listeners and the related
  `Connector.getEventListeners()` method. ({issue}`24609`)