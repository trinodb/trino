# Release 342 (24 Sep 2020)

## General Changes

* Add {func}`from_iso8601_timestamp_nanos` function. ({issue}`5048`)
* Improve performance of queries that use the `DECIMAL` type. ({issue}`4886`)
* Improve performance of queries involving `IN` with subqueries by extending support for dynamic filtering. ({issue}`5017`)
* Improve performance and latency of queries leveraging dynamic filters. ({issue}`4988`)
* Improve performance of queries joining tables with missing or incomplete column statistics when
  cost based optimization is enabled (which is the default). ({issue}`5141`)
* Reduce latency for queries that perform a broadcast join of a large table. ({issue}`5237`)
* Allow collection of dynamic filters for joins with large build side using the
  `enable-large-dynamic-filters` configuration property or the `enable_large_dynamic_filters`
  session property. ({issue}`5262`)
* Fix query failure when lambda expression references a table column containing a dot. ({issue}`5087`)

## Atop Connector Changes

* Fix incorrect query results when query contains predicates on `start_time` or `end_time` column. ({issue}`5125`)

## Elasticsearch Connector Changes

* Allow reading boolean values stored as strings. ({issue}`5269`)

## Hive Connector Changes

* Add support for S3 encrypted files. ({issue}`2536`)
* Add support for ABFS OAuth authentication. ({issue}`5052`)
* Support reading timestamp with microsecond or nanosecond precision. This can be enabled with the
  `hive.timestamp-precision` configuration property. ({issue}`4953`)
* Allow overwrite on insert by default using the `hive.insert-existing-partitions-behavior` configuration property. ({issue}`4999`)
* Allow delaying table scans until dynamic filtering can be performed more efficiently. This can be enabled
  using the `hive.dynamic-filtering-probe-blocking-timeout` configuration property or the
  `dynamic_filtering_probe_blocking_timeout` session property. ({issue}`4991`)
* Disable matching the existing user and group of the table or partition when creating new files on HDFS.
  The functionality was added in 341 and is now disabled by default. It can be enabled using the
  `hive.fs.new-file-inherit-ownership` configuration property. ({issue}`5187`)
* Improve performance when reading small files in `RCTEXT` or `RCBINARY` format. ({issue}`2536`)
* Improve planning time for queries with non-equality filters on partition columns when using the Glue metastore. ({issue}`5060`)
* Improve performance when reading `JSON` and `CSV` file formats. ({issue}`5142`)

## Iceberg Connector Changes

* Fix partition transforms for temporal columns for dates before 1970. ({issue}`5273`)

## Kafka Connector Changes

* Expose message headers as a `_headers` column of `MAP(VARCHAR, ARRAY(VARBINARY))` type. ({issue}`4462`)
* Add write support for `TIME`, `TIME WITH TIME ZONE`, `TIMESTAMP` and `TIMESTAMP WITH TIME ZONE`
  for Kafka connector when using the JSON encoder. ({issue}`4743`)
* Remove JSON decoder support for nonsensical combinations of input-format-type / data-type. The following
  combinations are no longer supported: ({issue}`4743`)
  - `rfc2822`:  `DATE`, `TIME`, `TIME WITH TIME ZONE`
  - `milliseconds-since-epoch`: `TIME WITH TIME ZONE`, `TIMESTAMP WITH TIME ZONE`
  - `seconds-since-epoch`: `TIME WITH TIME ZONE`, `TIMESTAMP WITH TIME ZONE`

## MySQL Connector Changes

* Improve performance of `INSERT` queries when GTID mode is disabled in MySQL. ({issue}`4995`)

## PostgreSQL Connector Changes

* Add support for variable-precision TIMESTAMP and TIMESTAMP WITH TIME ZONE types. ({issue}`5124`, {issue}`5105`)

## SQL Server Connector Changes

* Fix failure when inserting `NULL` into a `VARBINARY` column. ({issue}`4846`)
* Improve performance of aggregation queries by computing aggregations within SQL Server database.
  Currently, the following aggregate functions are eligible for pushdown:
  `count`,  `min`, `max`, `sum` and `avg`. ({issue}`4139`)

## SPI Changes

* Add `DynamicFilter.isAwaitable()` method that returns whether or not the dynamic filter is complete
  and can be awaited for using the `isBlocked()` method. ({issue}`5043`)
* Enable connectors to wait for dynamic filters derived from replicated joins before generating splits. ({issue}`4685`)
