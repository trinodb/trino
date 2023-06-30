# Release 417 (10 May 2023)

## General

* Improve performance of `UNION ALL` queries. ({issue}`17265`)

## Delta Lake connector

* Add support for [`COMMENT ON VIEW`](/sql/comment). ({issue}`17089`)
* Improve performance when reading Parquet data written by Trino. ({issue}`17373`, {issue}`17404`)
* Improve read performance for tables with `row` columns when only a subset of
  fields is needed for a query. ({issue}`17085`)

## Hive connector

* Add support for specifying arbitrary table properties via the
  `extra_properties` table property. ({issue}`954`)
* Improve performance when reading Parquet data written by Trino. ({issue}`17373`, {issue}`17404`)
* Improve performance when reading text files that contain more columns in the
  file than are mapped in the schema. ({issue}`17364`)
* Limit file listing cache based on in-memory size instead of number of entries.
  This is configured via the `hive.file-status-cache.max-retained-size` and
  `hive.per-transaction-file-status-cache.max-retained-size` configuration
  properties. The `hive.per-transaction-file-status-cache-maximum-size` and
  `hive.file-status-cache-size` configuration properties are deprecated. ({issue}`17285`)

## Hudi connector

* Improve performance when reading Parquet data written by Trino. ({issue}`17373`, {issue}`17404`)

## Iceberg connector

* Improve performance when reading Parquet data written by Trino. ({issue}`17373`, {issue}`17404`)
