# Release 434 (29 Nov 2023)

## General

* Add support for a `FILTER` clause to the `LISTAGG` function. ({issue}`19869`)
* {{breaking}} Rename the `query.max-writer-tasks-count` configuration property
  and the related `max_writer_tasks_count` session property to
  `query.max-writer-task-count` and `max_writer_task_count`. ({issue}`19793`)
* Improve performance of `INSERT ... SELECT` queries that contain a redundant
  `ORDER BY` clause. ({issue}`19916`)
* Fix incorrect results for queries involving comparisons between `double` and
  `real` zero and negative zero. ({issue}`19828`)
* Fix performance regression caused by suboptimal scalar subqueries planning. ({issue}`19922`)
* Fix failure when queries on data stored on HDFS involve table functions. ({issue}`19849`)
* Prevent sudden increases in memory consumption in some queries with
  joins involving `UNNEST`. ({issue}`19762`)

## BigQuery connector

* Add support for reading `json` columns. ({issue}`19790`)
* Add support for `DELETE` statement. ({issue}`6870`)
* Improve performance when writing rows. ({issue}`18897`)

## ClickHouse connector

* Add support for separate metadata caching configuration for schemas, tables,
  and metadata. ({issue}`19859`)

## Delta Lake connector

* {{breaking}} Disallow invalid configuration options. Previously, they were
  silently ignored.  ({issue}`19735`)
* Improve performance when reading large checkpoint files on partitioned tables.
  ({issue}`19588`, {issue}`19848`)
* Push down filters involving columns of type `timestamp(p) with time zone`. ({issue}`18664`)
* Fix query failure when reading Parquet column index for timestamp columns. ({issue}`16801`)

## Druid connector

* Add support for separate metadata caching configuration for schemas, tables,
  and metadata. ({issue}`19859`)

## Hive connector

* Add support for columns that changed from `timestamp` to `date` type. ({issue}`19513`)
* Fix query failure when reading Parquet column index for timestamp columns. ({issue}`16801`)

## Hudi connector

* Fix query failure when reading Parquet column index for timestamp columns. ({issue}`16801`)

## Iceberg connector

* {{breaking}} Remove support for legacy table statistics tracking. ({issue}`19803`)
* {{breaking}} Disallow invalid configuration options. Previously, they were
  silently ignored.  ({issue}`19735`)
* Fix query failure when reading Parquet column index for timestamp columns. ({issue}`16801`)
* Don't set owner for Glue materialized views when system security is enabled. ({issue}`19681`)

## Ignite connector

* Add support for separate metadata caching configuration for schemas, tables,
  and metadata. ({issue}`19859`)

## MariaDB connector

* Add support for separate metadata caching configuration for schemas, tables,
  and metadata. ({issue}`19859`)

## MySQl connector

* Add support for separate metadata caching configuration for schemas, tables,
  and metadata. ({issue}`19859`)

## Oracle connector

* Add support for separate metadata caching configuration for schemas, tables,
  and metadata. ({issue}`19859`)

## Phoenix connector

* Add support for separate metadata caching configuration for schemas, tables,
  and metadata. ({issue}`19859`)

## PostgreSQL connector

* Add support for separate metadata caching configuration for schemas, tables,
  and metadata. ({issue}`19859`)
* Prevent possible query failures when join is pushed down. ({issue}`18984`)

## Redshift connector

* Add support for separate metadata caching configuration for schemas, tables,
  and metadata. ({issue}`19859`)
* Prevent possible query failures when join is pushed down. ({issue}`18984`)

## SingleStore connector

* Add support for separate metadata caching configuration for schemas, tables,
  and metadata. ({issue}`19859`)

## SQL Server connector

* Add support for separate metadata caching configuration for schemas, tables,
  and metadata. ({issue}`19859`)
* Prevent possible query failures when join is pushed down. ({issue}`18984`)

## SPI

* Add bulk append methods to `BlockBuilder`. ({issue}`19577`)
* {{breaking}} Remove the `VariableWidthBlockBuilder.buildEntry` method. ({issue}`19577`)
* {{breaking}} Add required  `ConnectorSession` parameter to the method
  `TableFunctionProcessorProvider.getDataProcessor`. ({issue}`19778`)
