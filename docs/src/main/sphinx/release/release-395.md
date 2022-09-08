# Release 395 (7 Sep 2022)

## General

* Reduce memory consumption when fault-tolerant execution is enabled. ({issue}`13855`)
* Reduce memory consumption of aggregations. ({issue}`12512`)
* Improve performance of aggregations with decimals. ({issue}`13573`)
* Improve concurrency for large clusters. ({issue}`13934`, `13986`)
* Remove `information_schema.role_authorization_descriptors` table. ({issue}`11341`)
* Fix `SHOW CREATE TABLE` or `SHOW COLUMNS` showing an invalid type for columns
  that use a reserved keyword as column name. ({issue}`13483`)

## ClickHouse connector

* Fix query failure when renaming or dropping a column with a name which matches
  a reserved keyword or has special characters which require it to be quoted. ({issue}`13839`)

## Delta Lake connector

* Add support for the `ALTER TABLE ... RENAME TO` statement with a Glue
  metastore. ({issue}`12985`)
* Improve performance of inserts by automatically scaling the number of writers
  within a worker node. ({issue}`13111`)
* Enforce `delta.checkpoint.writeStatsAsJson` and
  `delta.checkpoint.writeStatsAsStruct` table properties to ensure table
  statistics are written in the correct format. ({issue}`12031`)

## Hive connector

* Improve performance of inserts by automatically scaling the number of writers
  within a worker node. ({issue}`13111`)
* Improve performance of S3 Select when using CSV files as an input. ({issue}`13754`)
* Fix error where the S3 KMS key is not searched in the proper AWS region when
  S3 client-side encryption is used. ({issue}`13715`)

## Iceberg connector

* Improve performance of inserts by automatically scaling the number of writers
  within a worker node. ({issue}`13111`)
* Fix creating metadata and manifest files with a URL-encoded name on S3 when
  the metadata location has trailing slashes. ({issue}`13759`)

## MariaDB connector

* Fix query failure when renaming or dropping a column with a name which matches
  a reserved keyword or has special characters which require it to be quoted. ({issue}`13839`)

## Memory connector

* Add support for table and column comments. ({issue}`13936`)

## MongoDB connector

* Fix query failure when filtering on columns of `json` type. ({issue}`13536`)

## MySQL connector

* Fix query failure when renaming or dropping a column with a name which matches
  a reserved keyword or has special characters which require it to be quoted. ({issue}`13839`)

## Oracle connector

* Fix query failure when renaming or dropping a column with a name which matches
  a reserved keyword or has special characters which require it to be quoted. ({issue}`13839`)

## Phoenix connector

* Fix query failure when adding, renaming, or dropping a column with a name
  which matches a reserved keyword or has special characters which require it to
  be quoted. ({issue}`13839`)

## PostgreSQL connector

* Fix query failure when renaming or dropping a column with a name which matches
  a reserved keyword or has special characters which require it to be quoted. ({issue}`13839`)

## Prometheus connector

* Add support for case-insensitive table name matching with the
  `prometheus.case-insensitive-name-matching` configuration property. ({issue}`8740`)

## Redshift connector

* Fix query failure when renaming or dropping a column with a name which matches
  a reserved keyword or has special characters which require it to be quoted. ({issue}`13839`)

## SingleStore (MemSQL) connector

* Fix query failure when renaming or dropping a column with a name which matches
  a reserved keyword or has special characters which require it to be quoted. ({issue}`13839`)

## SQL Server connector

* Fix query failure when renaming or dropping a column with a name which matches
  a reserved keyword or has special characters which require it to be quoted. ({issue}`13839`)

## SPI

* Add support for dynamic function resolution. ({issue}`8`)
* Rename `LIKE_PATTERN_FUNCTION_NAME` to `LIKE_FUNCTION_NAME` in
  `StandardFunctions`. ({issue}`13965`)
* Remove the `listAllRoleGrants` method from `ConnectorMetadata`. ({issue}`11341`)
