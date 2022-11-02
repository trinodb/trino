# Release 399 (6 Oct 2022)

## General

* Add operator CPU and wall time distribution to `EXPLAIN ANALYZE VERBOSE`. ({issue}`14370`)
* Improve performance of joins. ({issue}`13352`)
* Remove support for the deprecated `row` to `json` cast behavior, and remove the
  `deprecated.legacy-row-to-json-cast` configuration property. ({issue}`14388`)
* Fix error when using `PREPARE` with `DROP VIEW` when the view name is quoted. ({issue}`14196`)
* Fix potential planning failure for queries involving `UNION`. ({issue}`14472`)
* Fix error when using aggregations in window expressions when the function
  loaded from a plugin. ({issue}`14486`)

## Accumulo connector

* Change the default value of the `accumulo.zookeeper.metadata.root`
  configuration property to `/trino-accumulo` from `/presto-accumulo`. ({issue}`14326`)

## BigQuery connector

* Add support for writing `array`, `row`, and `timestamp` columns. ({issue}`14418`, {issue}`14473`)

## ClickHouse connector

* Fix bug where the intended default value of the `domain-compaction-threshold`
  configuration property was incorrectly used as a maximum limit. ({issue}`14350`)

## Delta Lake connector

* Improve performance of reading decimal columns from Parquet files. ({issue}`14260`)
* Allow setting the AWS Security Token Service endpoint and region when using a
  Glue metastore. ({issue}`14412`)

## Hive connector

* Add `max-partition-drops-per-query` configuration property to limit the number
  of partition drops. ({issue}`12386`)
* Add `hive.s3.region` configuration property to force S3 to connect to a
  specific region. ({issue}`14398`)
* Improve performance of reading decimal columns from Parquet files. ({issue}`14260`)
* Reduce memory usage on the coordinator. ({issue}`14408`)
* Reduce query memory usage during inserts to S3. ({issue}`14212`)
* Change the name of the `partition_column` and `partition_value` arguments for
  the `flush_metadata_cache` procedure to `partition_columns` and
  `partition_values`, respectively, for parity with other procedures. ({issue}`13566`)
* Change field name matching to be case insensitive. ({issue}`13423`)
* Allow setting the AWS STS endpoint and region when using a Glue metastore. ({issue}`14412`)

## Hudi connector

* Fix failure when reading hidden columns. ({issue}`14341`)

## Iceberg connector

* Improve performance of reading decimal columns from Parquet files. ({issue}`14260`)
* Reduce planning time for complex queries. ({issue}`14443`)
* Store metastore `table_type` property value in uppercase for compatibility
  with other Iceberg catalog implementations. ({issue}`14384`)
* Allow setting the AWS STS endpoint and region when using a Glue metastore. ({issue}`14412`)

## Phoenix connector

* Fix bug where the intended default value of the `domain-compaction-threshold`
  configuration property was incorrectly used as a maximum limit. ({issue}`14350`)

## SQL Server connector

* Fix error when querying or listing tables with names that contain special
  characters. ({issue}`14286`)

## SPI

* Add stage output buffer distribution to `EventListener`. ({issue}`14400`)
* Remove deprecated `TimeType.TIME`, `TimestampType.TIMESTAMP` and
  `TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE` constants. ({issue}`14414`)
