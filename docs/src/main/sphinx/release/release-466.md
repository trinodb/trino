# Release 466 (27 Nov 2024)

## General

* Add support for changing the type of row fields when they are in a columns of
  type `map`. ({issue}`24248`)
* Remove the requirement for a Python runtime on Trino cluster nodes. ({issue}`24271`)
* Improve performance of queries involving `GROUP BY` and joins. ({issue}`23812`)
* Improve client protocol throughput by introducing the [spooling
  protocol](protocol-spooling). ({issue}`24214`)

## Security

* Add support for [data access control with Apache
  Ranger](/security/ranger-access-control), including support for
  column masking, row filtering, and audit logging. ({issue}`22675`)

## JDBC driver

* Improve throughput by automatically using the [spooling
  protocol](jdbc-spooling-protocol) when it is configured on the Trino cluster,
  and add the parameter `encoding` to optionally set the preferred encoding from
  the JDBC driver. ({issue}`24214`)
* Improve decompression performance when running the client with Java 22 or
  newer. ({issue}`24263`)
* Improve performance `java.sql.DatabaseMetaData.getTables()`. ({issue}`24159`,
  {issue}`24110`)

## Server RPM

* Remove Python requirement. ({issue}`24271`)

## Docker image

* Remove Python runtime and libraries. ({issue}`24271`)

## CLI

* Improve throughput by automatically use the [spooling
  protocol](cli-spooling-protocol) when it is configured on the Trino cluster,
  and add the option `--encoding` to optionally set the preferred encoding from
  the CLI. ({issue}`24214`)
* Improve decompression performance when running the CLI with Java 22 or newer. ({issue}`24263`)

## BigQuery connector

* Add support for `LIMIT` pushdown. ({issue}`23937`)

## Iceberg connector

* Add support for the [object store file
  layout](https://iceberg.apache.org/docs/latest/aws/#object-store-file-layout).
  ({issue}`8861`)
* Add support for changing field types inside a map. ({issue}`24248`)
* Improve performance of queries with selective joins. ({issue}`24277`)
* Fix failure when reading columns containing nested row types that differ from
  the schema of the underlying Parquet data. ({issue}`22922`)

## Phoenix connector

* Improve performance for `MERGE` statements. ({issue}`24075`)

## SQL Server connector

* Rename the `sqlserver.experimental.stored-procedure-table-function-enabled`
  configuration property to `sqlserver.stored-procedure-table-function-enabled`.
  ({issue}`24239`)

## SPI

* Add `ConnectorSplit` argument to the `SystemTable.cursor()` method. ({issue}`24159`)
* Add support for partial row updates to the `ConnectorMetadata.beginMerge()`
  method. ({issue}`24075`)
