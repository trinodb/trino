# Release 444 (3 Apr 2024)

## General

* Improve planning time for queries with a large number of joins. ({issue}`21360`)
* Fix failure for queries containing large numbers of `LIKE` terms in boolean
  expressions. ({issue}`21235`)
* Fix potential failure when queries contain filtered aggregations. ({issue}`21272`)

## Docker image

* Update Java runtime to Java 22. ({issue}`21161`)

## BigQuery connector

* Fix failure when reading BigQuery views with [Apache
  Arrow](https://arrow.apache.org/docs/). ({issue}`21337`)

## ClickHouse connector

* Improve performance of reading table comments. ({issue}`21238`)

## Delta Lake connector

* Add support for reading `BYTE_STREAM_SPLIT` encoding in Parquet files. ({issue}`8357`)
* Add support for [Canned ACLs](https://docs.aws.amazon.com/AmazonS3/latest/userguide/acl-overview.html#canned-acl)
  with the native S3 file system. ({issue}`21176`)
* Add support for concurrent, non-conflicting writes when a table is read and
  written to in the same query. ({issue}`20983`)
* Add support for reading tables with [v2
  checkpoints](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#v2-spec).
  ({issue}`19345`)
* Add support for reading [shallow cloned tables](delta-lake-shallow-clone).
  ({issue}`17011`)
* {{breaking}} Remove support for split size configuration with the catalog
  properties `delta.max-initial-splits` and `delta.max-initial-split-size`, and
  the catalog session property `max_initial_split_size`. ({issue}`21320`)
* Fix incorrect results when querying a table that's being modified
  concurrently. ({issue}`21324`)

## Druid connector

* Improve performance of reading table comments. ({issue}`21238`)

## Hive connector

* Add support for reading `BYTE_STREAM_SPLIT` encoding in Parquet files. ({issue}`8357`)
* Add support for [Canned ACLs](https://docs.aws.amazon.com/AmazonS3/latest/userguide/acl-overview.html#canned-acl)
  with the native S3 file system. ({issue}`21176`)

## Hudi connector

* Add support for reading `BYTE_STREAM_SPLIT` encoding in Parquet files. ({issue}`8357`)
* Add support for [Canned ACLs](https://docs.aws.amazon.com/AmazonS3/latest/userguide/acl-overview.html#canned-acl)
  with the native S3 file system. ({issue}`21176`)

## Iceberg connector

* Add support for the `metadata_log_entries` system table. ({issue}`20410`)
* Add support for reading `BYTE_STREAM_SPLIT` encoding in Parquet files. ({issue}`8357`)
* Add support for [Canned ACLs](https://docs.aws.amazon.com/AmazonS3/latest/userguide/acl-overview.html#canned-acl)
  with the native S3 file system. ({issue}`21176`)

## Ignite connector

* Improve performance of reading table comments. ({issue}`21238`)

## MariaDB connector

* Improve performance of reading table comments. ({issue}`21238`)

## MySQL connector

* Improve performance of reading table comments. ({issue}`21238`)

## Oracle connector

* Improve performance of reading table comments. ({issue}`21238`)

## PostgreSQL connector

* Improve performance of reading table comments. ({issue}`21238`)

## Redshift connector

* Improve performance of reading table comments. ({issue}`21238`)

## SingleStore connector

* Improve performance of reading table comments. ({issue}`21238`)

## Snowflake connector

* Add support for table comments. ({issue}`21305`)
* Improve performance of queries with `ORDER BY ... LIMIT` clause, or `avg`,
  `count(distinct)`, `stddev`, or `stddev_pop` aggregation functions when the
  computation can be pushed down to the underlying database. ({issue}`21219`,
  {issue}`21148`, {issue}`21130`, {issue}`21338`)
* Improve performance of reading table comments.  ({issue}`21161`)

## SQLServer connector

* Improve performance of reading table comments. ({issue}`21238`)

## SPI

* Change group id and capacity of `GroupedAccumulatorState` to `int` type. ({issue}`21333`)
