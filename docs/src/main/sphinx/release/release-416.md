# Release 416 (3 May 2023)

## General

* Improve performance of partitioned `INSERT`, `CREATE TABLE AS .. SELECT`, and
  `EXECUTE` statements when the source table statistics are missing or 
  inaccurate. ({issue}`16802`)
* Improve performance of `LIKE` expressions that contain `%`. ({issue}`16167`)
* Remove the deprecated `preferred-write-partitioning-min-number-of-partitions`
  configuration property. ({issue}`16802`)

## Hive connector

* Reduce coordinator memory usage when file metadata caching is enabled. ({issue}`17270`)
