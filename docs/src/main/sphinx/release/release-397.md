# Release 397 (21 Sep 2022)

## General

* Fix incorrect parsing of invalid values in cast from `varchar` to `timestamp`. ({issue}`14164`)
* Fix potential incorrect results for queries with a partitioned output which
  doesn't depend on any column data. ({issue}`14168`)
* Fix `EXPLAIN (TYPE IO)` query failure for certain queries on empty tables. ({issue}`10398`)

## Security

* Add support for multiple recipients with JWT authentication. ({issue}`13442 `)
* Fix OAuth 2.0 token refresh causing JWT authentication failure. ({issue}`13575`)

## JDBC driver

* Fix potential memory leak when cancelling statements. ({issue}`14176`)

## Delta Lake connector

* Rename the `parquet.experimental-optimized-writer.enabled` configuration
  property and `experimental_parquet_optimized_writer_enabled` session property
  to `parquet.optimized-writer.enabled` and `parquet_optimized_writer_enabled`,
  respectively. ({issue}`14137`)

## Hive connector

* Rename the `parquet.experimental-optimized-writer.enabled` configuration
  property and `experimental_parquet_optimized_writer_enabled` session property
  to `parquet.optimized-writer.enabled` and `parquet_optimized_writer_enabled`,
  respectively. ({issue}`14137`)
* Improve performance when querying JSON data and Hive S3 Select pushdown is
  enabled. ({issue}`14040`)
* Improve planning performance when querying tables in the Glue catalog that
  contain a large number of columns. ({issue}`14206`)
* Allow reading from a partitioned table after a column's data type was changed
  from `decimal` to `varchar` or `string`. ({issue}`2817`)
* Fix query failure when reading from a Hive view and
  `hive.hive-views.run-as-invoker` and `hive.hive-views.legacy-translation` are
  both enabled. ({issue}`14077`)

## Iceberg connector

* Improve performance of queries that contain predicates involving `date_trunc`
  with an `hour` unit on `date`, `timestamp`, or `timestamp with time zone`
  partition columns. ({issue}`14161`)
* Improve performance of reads after a `DELETE` removes all rows from a file. ({issue}`14198`)
* Reduce query latency when using a Glue catalog for metadata. ({issue}`13875`)
* Fix materialized views temporarily appearing empty when a refresh is about to
  complete. ({issue}`14145`)
* Fix potential table corruption when changing a table before it is known if
  committing to the Hive metastore has failed or succeeded. ({issue}`14174`)

## SPI

* Replace `DictionaryBlock` constructors with a factory method. ({issue}`14092`)
* Replace `RunLengthEncodedBlock` constructors with a factory method. ({issue}`14092`)
