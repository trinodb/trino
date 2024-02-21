# Release 440 (8 Mar 2024)

## General

* Add [Snowflake connector](/connector/snowflake). ({issue}`17909`)
* Add support for sub-queries inside `UNNEST` clauses. ({issue}`17953`)
* Improve performance of {func}`arrays_overlap`. ({issue}`20900`)
* Export JMX statistics for resource groups by default. This can be disabled
  with the `jmxExport` resource group property. ({issue}`20810`)
* Fix query failure when a check constraint is null. ({issue}`20906`)
* Fix query failure for aggregations over `CASE` expressions when the input
  evaluation could throw an error. ({issue}`20652`)
* Fix incorrect behavior of the else clause in a SQL routines with a single
  if/end condition. ({issue}`20926`)
* Fix the `ALTER TABLE EXECUTE optimize` queries failing due to exceeding the
  open writer limit. ({issue}`20871`)
* Fix certain `INSERT` and `CREATE TABLE AS .. SELECT` queries failing due to
  exceeding the of open writer limit on partitioned tables. ({issue}`20871`)
* Fix "multiple entries with same key" query failure for queries with joins on
  partitioned tables. ({issue}`20917`)
* Fix incorrect results when using `GRANT`, `DENY`, and `REVOKE` clauses on
  views and materialized views. ({issue}`20812`)

## Security

* Add support for row filtering and column masking in Open Policy Agent access
  control. ({issue}`20921`)

## Web UI

* Fix error when using authentication tokens larger than 4 kB. ({issue}`20787`)

## Delta Lake connector

* Add support for concurrent `INSERT` queries. ({issue}`18506`)
* Improve latency for queries with file system caching enabled. ({issue}`20851`)
* Improve latency for queries on tables with checkpoints. ({issue}`20901`)
* Fix query failure due to "corrupted statistics" when reading Parquet files
  with a predicate on a long decimal column. ({issue}`20981`)

## Hive connector

* Add support for bearer token authentication for a Thrift metastore connection. ({issue}`20371`)
* Add support for commenting on partitioned columns in the Thrift metastore. ({issue}`20264`)
* Add support for changing a column's type from `varchar` to `float`. ({issue}`20719`)
* Add support for changing a column's type from `varchar` to `char`. ({issue}`20723`)
* Add support for changing a column's type from `varchar` to `boolean`. ({issue}`20741`)
* Add support for configuring a `region` and `endpoint` for S3 security mapping. ({issue}`18838`)
* Improve performance when reading JSON files. ({issue}`19396`)
* Fix incorrect truncation when decoding `varchar(n)` and `char(n)` in
  `TEXTFILE` and `SEQUENCEFILE` formats. ({issue}`20731`)
* Fix query failure when `hive.file-status-cache-tables` is enabled for a table
  and new manifest files have been added but not cached yet. ({issue}`20344`)
* Fix error when trying to `INSERT` into a transactional table that does not
  have partitions. ({issue}`19407`)
* Fix query failure due to "corrupted statistics" when reading Parquet files
  with a predicate on a long decimal column. ({issue}`20981`)

## Hudi connector

* Fix query failure due to "corrupted statistics" when reading Parquet files
  with a predicate on a long decimal column. ({issue}`20981`)

## Iceberg connector

* Improve latency of queries when file system caching is enabled. ({issue}`20803`)
* Disallow setting the materialized view owner when using system security with
  the Glue catalog. ({issue}`20647`)
* Rename the `orc.bloom.filter.columns` and `orc.bloom.filter.fpp` table
  properties to `write.orc.bloom.filter.columns` and
  `write.orc.bloom.filter.fpp`, respectively. ({issue}`20432`)
* Fix query failure due to "corrupted statistics" when reading Parquet files
  with a predicate on a long decimal column. ({issue}`20981`)

## SPI

* Add reset to position method to `BlockBuilder`. ({issue}`19577`)
* Remove the `getChildren` method from `Block`. ({issue}`19577`)
* Remove the `get{Type}` methods from `Block`.  Callers must unwrap a `Block`
  and downcast the `ValueBlock` to `Type.getValueBlockType()` implementation. ({issue}`19577`)
