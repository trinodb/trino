# Release 482 (dd MMM 2026)

## General

* Add support for the `AT LOCAL` operator. ({issue}`29644`)
* Add support for named arguments in function calls using the `name => value`
  syntax. ({issue}`29530`)
* Add support for casting `char` values to numeric, `boolean`, `varbinary`, and
  temporal types. ({issue}`29374`)
* {{breaking}} Remove support for Alluxio-backed exchange storage for
  fault-tolerant execution. ({issue}`29596`)
* Disallow `OMIT QUOTES` in `JSON_QUERY` when the returned type is `json`.
  ({issue}`29587`)
* Reduce memory usage that accumulates over time on long-running clusters that
  execute many queries with aggregations. ({issue}`29523`, {issue}`29524`)
* Fix incorrect results for queries on `number` columns containing `NaN` values.
  ({issue}`29497`)
* Fix query failure when dereferencing a field of a `null` row value inside a
  lambda expression. ({issue}`29504`)
* Fix failure when executing a SQL user-defined function that declares a
  variable with a structural default value such as an array. ({issue}`29533`)
* Fix failure when a table function returns large pass-through columns.
  ({issue}`29561`)
* Fix `DESCRIBE OUTPUT` returning the `number` type to clients that do not
  support it. ({issue}`29671`)

## Web UI

* Add buttons to the preview UI to cancel or preempt a running query.
  ({issue}`29315`)

## Delta Lake connector

* Add the table location to the `$properties` metadata table. ({issue}`29521`)
* Add support for limiting the number of rows in Parquet writer row groups with
  the `parquet.writer.block-row-count` configuration property or the
  `parquet_writer_block_row_count` session property. ({issue}`29673`)
* Disallow creating tables partitioned by `varbinary` columns. ({issue}`24155`)
* {{breaking}} Remove support for the Alluxio file system. This does not affect
  support for the Alluxio file system cache. ({issue}`29596`)
* Improve compression and read performance for high-cardinality string columns
  in Parquet files. This can be disabled by setting the
  `parquet.writer.delta-length-byte-array-encoding-enabled` configuration
  property to `false`. ({issue}`29246`)
* Improve performance of queries that access nested fields inside lambda
  expressions. ({issue}`29532`)
* Fix sporadic failures when reading data from S3. ({issue}`29419`)

## Hive connector

* Add support for limiting the number of rows in Parquet writer row groups with
  the `parquet.writer.block-row-count` configuration property or the
  `parquet_writer_block_row_count` session property. ({issue}`29673`)
* {{breaking}} Remove support for the Alluxio file system. This does not affect
  support for the Alluxio file system cache. ({issue}`29596`)
* Improve compression and read performance for high-cardinality string columns
  in Parquet files. This can be disabled by setting the
  `parquet.writer.delta-length-byte-array-encoding-enabled` configuration
  property to `false`. ({issue}`29246`)
* Improve performance of queries that access nested fields inside lambda
  expressions. ({issue}`29532`)
* Fix sporadic failures when reading data from S3. ({issue}`29419`)

## Hudi connector

* {{breaking}} Remove support for the Alluxio file system. This does not affect
  support for the Alluxio file system cache. ({issue}`29596`)
* Fix sporadic failures when reading data from S3. ({issue}`29419`)

## Iceberg connector

* Allow configuring the `data_location` table property without enabling
  `object_store_layout_enabled`. ({issue}`28887`)
* Add support for limiting the number of rows in Parquet writer row groups with
  the `parquet.writer.block-row-count` configuration property or the
  `parquet_writer_block_row_count` session property. ({issue}`29673`)
* Add support for caching Parquet file footers, configurable with the
  `iceberg.parquet-footer-cache.type` configuration property. ({issue}`29684`)
* Add support for the `write.object-storage.partitioned-paths` table property.
  ({issue}`27633`)
* Add support for prefixed path storage credentials in the Iceberg REST
  catalog. ({issue}`29641`)
* {{breaking}} Change the `lower_bounds` and `upper_bounds` columns of the
  `$files` metadata table from `map(integer, varchar)` to typed `row` values.
  Queries using `lower_bounds[1]` or casting these columns to `json` must be
  updated. ({issue}`23147`)
* {{breaking}} Remove support for the Alluxio file system. This does not affect
  support for the Alluxio file system cache. ({issue}`29596`)
* Improve performance of queries on the `$partitions` metadata table.
  ({issue}`23147`)
* Improve compression and read performance for high-cardinality string columns
  in Parquet files. This can be disabled by setting the
  `parquet.writer.delta-length-byte-array-encoding-enabled` configuration
  property to `false`. ({issue}`29246`, {issue}`29616`)
* Improve performance of queries that access nested fields inside lambda
  expressions. ({issue}`29532`)
* Increase the maximum supported size of `variant` metadata from 16MB to 128MB.
  ({issue}`29423`)
* Fix loss of materialized view data when running `CREATE OR REPLACE
  MATERIALIZED VIEW` for a view with a fixed storage location. ({issue}`29481`)
* Fix failure when listing schemas in an Iceberg REST catalog and a nested
  namespace is dropped concurrently. ({issue}`29500`)
* Fix sporadic failures when reading data from S3. ({issue}`29419`)

## Lakehouse connector

* Add support for limiting the number of rows in Parquet writer row groups with
  the `parquet.writer.block-row-count` configuration property or the
  `parquet_writer_block_row_count` session property. ({issue}`29673`)
* Fix sporadic failures when reading data from S3. ({issue}`29419`)

## PostgreSQL connector

* Fix failure when reading tables with PostGIS `geometry` columns.
  ({issue}`29505`)

## SPI

* Add the `@StaticMethod` annotation to register a scalar function as a static
  method of a named type, invocable via `T::method(args)`. ({issue}`29385`)
* Add the `@InstanceMethod` annotation to register a scalar function as a method
  of its receiver type, invocable via `expr.method(args)`. ({issue}`29399`)
* Add the `@Name` annotation to declare names for the arguments of functions
  implemented in Java. ({issue}`29530`)
* Add support for lambda expressions in connector expression pushdown.
  ({issue}`29532`)
* {{breaking}} Remove deprecated methods from `ConnectorPageSourceProvider` and
  `ConnectorPageSinkProvider`. ({issue}`29562`)
* {{breaking}} Remove a deprecated method from `TableFunctionProcessorProvider`.
  ({issue}`29618`)
