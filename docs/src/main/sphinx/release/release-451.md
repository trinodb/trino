# Release 451 (27 Jun 2024)

## General

* Add support for configuring a proxy for the S3 native filesystem with the
  `s3.http-proxy.username`, `s3.http-proxy.password`,
  `s3.http-proxy.non-proxy-hosts`, and `s3.http-proxy.preemptive-basic-auth`
  configuration properties. ({issue}`22207`)
* Add support for the {func}`t_pdf` and {func}`t_cdf` functions. ({issue}`22507`)
* Improve performance of reading JSON array data. ({issue}`22379`)
* Improve performance of certain queries involving the {func}`row_number`,
  {func}`rank`, or {func}`dense_rank` window functions with partitioning and
  filters. ({issue}`22509`)
* Fix error when reading empty files with the native S3 file system. ({issue}`22469`)
* Fix rare error where query execution could hang when fault-tolerant execution
  is enabled. ({issue}`22472`)
* Fix incorrect results for CASE expressions of the form
  `CASE WHEN ... THEN true ELSE false END`. ({issue}`22530`)

## Delta Lake connector

* Improve performance of reading from Parquet files with large schemas. ({issue}`22451`)

## Hive connector

* Improve performance of reading from Parquet files with large schemas. ({issue}`22451`)

## Hudi connector

* Improve performance of reading from Parquet files with large schemas. ({issue}`22451`)

## Iceberg connector

* Add support for incremental refresh for basic materialized views. ({issue}`20959`)
* Add support for adding and dropping fields inside an array. ({issue}`22232`)
* Add support for specifying a resource
  [prefix](https://github.com/apache/iceberg/blob/a47937c0c1fcafe57d7dc83551d8c9a3ce0ab1b9/open-api/rest-catalog-open-api.yaml#L1449-L1455)
  in the Iceberg REST catalog. ({issue}`22441`)
* Add support for partitioning on nested `ROW` fields. ({issue}`15712`)
* Add support for writing Parquet Bloom filters. ({issue}`21570`)
* Add support for uppercase characters in the `partitioning` table property. ({issue}`12668`)
* Improve performance of reading from Parquet files with large schemas. ({issue}`22451`)

## Kudu connector

* Add support for the Kudu `DATE` type. ({issue}`22497`)
* Fix query failure when a filter is applied on a `varbinary` column. ({issue}`22496`)

## SPI

* Add a `Connector.getInitialMemoryRequirement()` API for pre-allocating memory
  during catalog initialization. ({issue}`22197`)
