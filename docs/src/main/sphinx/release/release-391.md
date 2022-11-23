# Release 391 (22 Jul 2022)

## General

* Improve performance of repeated aggregations with ``CASE`` expressions. ({issue}`12548`)
* Improve query latency when there is high concurrency. ({issue}`13213`)
* Improve planning performance for join queries when tables have statistics. ({issue}`13047`)
* Fail queries that get stuck in long-running regular expression functions. ({issue}`12392`)
* Fix potential query failure when the ``UUID`` type is used. ({issue}`13265`)
* Set the default value of the ``optimizer.force-single-node-output``
  configuration property to false. ({issue}`13217`)

## BigQuery connector

* Add support for reading external tables. ({issue}`13164`)
* Add support for specifying table and column comments when creating a table. ({issue}`13105`)

## Delta Lake connector

* Improve optimized Parquet writer performance. ({issue}`13203`, {issue}`13208`)
* Store query ID when creating a new schema. ({issue}`13242`)
* Fix incorrect `schema already exists` error caused by a client timeout when
  creating a new schema. ({issue}`13242`)
* Fix incorrect query results when reading a table with an outdated cached
  representation of its active data files. ({issue}`13181`)

## Druid connector

* Fix potential query failure when using the ``query`` table function with
  metadata caching and the underlying table schema is changed via Trino. ({issue}`12526`)

## Hive connector

* Add support for [AWS Athena partition projection](partition_projection). ({issue}`11305`)
* Improve optimized Parquet writer performance. ({issue}`13203`, {issue}`13208`)
* Fix potential failure when creating empty ORC bucket files while using ZSTD
  compression. ({issue}`9775`)
* Fix query failure or potentially incorrect statistics when running concurrent
  `CREATE TABLE AS` queries with the `IF NOT EXISTS` clause for the same
  non-existent table. ({issue}`12895`)
* Fix incorrect results when using the Glue metastore with queries that contain
  `IS NULL` and additional filters. ({issue}`13122`)

## Iceberg connector

* Improve performance when writing Parquet writer data. ({issue}`13203`, {issue}`13208`)
* Fix query failure when reading an Iceberg table with deletion-tracking files. ({issue}`13035`)

## MariaDB connector

* Fix potential query failure when using the ``query`` table function with
  metadata caching and the underlying table schema is changed via Trino. ({issue}`12526`)

## MySQL connector

* Fix potential query failure when using the ``query`` table function with
  metadata caching and the underlying table schema is changed via Trino. ({issue}`12526`)

## Oracle connector

* Fix potential query failure when using the ``query`` table function with
  metadata caching and the underlying table schema is changed via Trino. ({issue}`12526`)

## PostgreSQL connector

* Fix potential query failure when using the ``query`` table function with
  metadata caching and the underlying table schema is changed via Trino. ({issue}`12526`)

## Redshift connector

* Fix potential query failure when using the ``query`` table function with
  metadata caching and the underlying table schema is changed via Trino. ({issue}`12526`)

## SQL Server connector

* Fix potential query failure when using the ``query`` table function with
  metadata caching and the underlying table schema is changed via Trino. ({issue}`12526`)

## SPI

* Removed deprecated methods and classes related to the grouped execution
  feature. ({issue}`13125`)
