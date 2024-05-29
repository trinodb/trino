# Release 449 (31 May 2024)

## General

* Add an event listener which exposes collected events to an HTTP endpoint. ({issue}`22158`)
* Fix rare query failure or incorrect results for array types when the data is
  dictionary encoded. ({issue}`21911`)
* Fix JMX metrics not exporting for resource groups. ({issue}`21343`)

## BigQuery connector

* Improve performance when listing schemas while the
  `bigquery.case-insensitive-name-matching` configuration property is enabled. ({issue}`22033`)

## ClickHouse connector

* Add support for pushing down execution of the `count(distinct)`, `corr`,
  `covar_samp`, and `covar_pop` functions to the underlying database. ({issue}`7100`)
* Improve performance when pushing down equality predicates on textual types. ({issue}`7100`)

## Delta Lake connector

* Add support for [the `$partitions` system table](delta-lake-partitions-table). ({issue}`18590`)
* Add support for reading from and writing to tables with
  [VACUUM Protocol Check](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#vacuum-protocol-check). ({issue}`21398`)
* Add support for configuring the query retry policy on the S3 filesystem with
  the `s3.retry-mode` and `s3.max-error-retries` configuration properties.
* Automatically use `varchar` in struct types as a type during table creation
  when `char` is specified. ({issue}`21511`)
* Improve performance of writing to Parquet files. ({issue}`22089`)
* Fix query failure when the `hive.metastore.glue.catalogid` configuration
  property is set. ({issue}`22048`)

## Hive connector

* Add support for specifying a catalog name in the Thrift metastore with the
  `hive.metastore.thrift.catalog-name` configuration property. (`10287`)
* Add support for configuring the query retry policy on the S3 filesystem with
  the `s3.retry-mode` and `s3.max-error-retries` configuration properties.
* Improve performance of writing to Parquet files. ({issue}`22089`)
* Fix failure when filesystem caching is enabled on Trino clusters with a single
  node. ({issue}`21987`)
* Fix failure when listing Hive tables with unsupported syntax. ({issue}`21981`)
* Fix query failure when the `hive.metastore.glue.catalogid` configuration
  property is set. ({issue}`22048`)
* Fix failure when running the `flush_metadata_cache` table procedure with the
  Glue v2 metastore. ({issue}`22075`)

## Hudi connector

* Add support for configuring the query retry policy on the S3 filesystem with
  the `s3.retry-mode` and `s3.max-error-retries` configuration properties.
* Improve performance of writing to Parquet files. ({issue}`22089`)

## Iceberg connector

* Add support for views when using the Iceberg REST catalog. ({issue}`19818`)
* Add support for configuring the query retry policy on the S3 filesystem with
  the `s3.retry-mode` and `s3.max-error-retries` configuration properties.
* Automatically use `varchar` in struct types as a type during table creation
  when `char` is specified. ({issue}`21511`)
* Automatically use microsecond precision for temporal types in struct types
  during table creation. ({issue}`21511`)
* Improve performance and memory usage when
  [equality delete](https://iceberg.apache.org/spec/#equality-delete-files)
  files are used. ({issue}`18396`)
* Improve performance of writing to Parquet files. ({issue}`22089`)
* Fix failure when writing to tables with Iceberg `VARBINARY` values. ({issue}`22072`)

## Pinot connector

* {{breaking}} Remove support for non-gRPC clients and the `pinot.grpc.enabled`
  and `pinot.estimated-size-in-bytes-for-non-numeric-column` configuration
  properties. ({issue}`22213`)

## Snowflake connector

* Fix incorrect type mapping for numeric values. ({issue}`20977`)
