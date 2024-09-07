# Release 457 (6 Sep 2024)

## General

* Expose additional JMX metrics about resource groups, including CPU and memory
  usage, limits, and scheduling policy. ({issue}`22957`)
* Improve performance of queries involving joins when fault tolerant execution
  is enabled. This [adaptive plan
  optimization](/optimizer/adaptive-plan-optimizations) can be disabled with the
  `fault-tolerant-execution-adaptive-join-reordering-enabled` configuration
  property or the `fault_tolerant_execution_adaptive_join_reordering_enabled`
  session property. ({issue}`23046`)
* Improve performance for [LZ4, Snappy and ZSTD compression and
  decompression](file-compression) used for [exchange spooling with
  fault-tolerant execution](fte-exchange-manager). ({issue}`22532`)
* {{breaking}} Shorten the name for the Kafka event listener to `kafka`. ({issue}`23308`)
* Extend the Kafka event listener to send split completion events. ({issue}`23065`)

## JDBC driver

* Publish a [JDBC driver JAR](jdbc-installation) without bundled, third-party
  dependencies. ({issue}`22098`)

## BigQuery connector

* Fix failures with queries using table functions when `parent-project-id` is
  defined. ({issue}`23041`)

## Blackhole connector

* Add support for the `REPLACE` modifier as part of a `CREATE TABLE` statement. ({issue}`23004`)

## Delta Lake connector

* Add support for creating tables with
  [deletion vector](https://docs.delta.io/latest/delta-deletion-vectors.html).
  ({issue}`22104`)
* Improve performance for concurrent write operations on S3 by using lock-less
  Delta Lake write reconciliation. ({issue}`23145`)
* Improve performance for [LZ4, Snappy, and ZSTD compression and
  decompression](file-compression). ({issue}`22532`)
* Fix SSE configuration when using S3SecurityMapping with kmsKeyId configured. ({issue}`23299`)

## Hive connector

* Improve performance of queries that scan a large number of partitions. ({issue}`23194`)
* Improve performance for [LZ4, Snappy, and ZSTD compression and
  decompression](file-compression). ({issue}`22532`)
* Fix OpenX JSON decoding a JSON array line that resulted in data being written
  to the wrong output column. ({issue}`23120`)

## Hudi connector

* Improve performance for [LZ4, Snappy, and ZSTD compression and
  decompression](file-compression). ({issue}`22532`)

## Iceberg connector

* Improve performance for [LZ4, Snappy, and ZSTD compression and
  decompression](file-compression). ({issue}`22532`)

## Memory connector

* Add support for renaming schemas with `ALTER SCHEMA ... RENAME`. ({issue}`22659`)

## Prometheus connector

* Fix reading large Prometheus responses. ({issue}`23025`)

## SPI

* Remove the deprecated `ConnectorMetadata.createView` method. ({issue}`23208`)
* Remove the deprecated `ConnectorMetadata.beginRefreshMaterializedView` method.
  ({issue}`23212`)
* Remove the deprecated `ConnectorMetadata.finishInsert` method. ({issue}`23213`)
* Remove the deprecated `ConnectorMetadata.createTable(ConnectorSession session,
  ConnectorTableMetadata tableMetadata, boolean ignoreExisting)` method.
  ({issue}`23209`)
* Remove the deprecated `ConnectorMetadata.beginCreateTable` method. ({issue}`23211`)
* Remove the deprecated `ConnectorSplit.getInfo` method. ({issue}`23271`)
* Remove the deprecated `DecimalConversions.realToShortDecimal(long value, long
  precision, long scale)` method. ( {issue}`23275`)
* Remove the deprecated constructor from the `ConstraintApplicationResult`
  class. ({issue}`23272`)
