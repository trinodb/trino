# Release 378 (21 Apr 2022)

## General

* Add {func}`to_base32` and {func}`from_base32` functions. ({issue}`11439`)
* Improve planning performance of queries with large `IN` lists.
  ({issue}`11902`, {issue}`11918`, {issue}`11956`)
* Improve performance of queries involving correlated `IN` or `EXISTS`
  predicates. ({issue}`12047`)
* Fix reporting of total spilled bytes in JMX metrics. ({issue}`11983`)

## Security

* Require value for [the shared secret configuration for internal
  communication](/security/internal-communication) when any authentication is
  enabled. ({issue}`11944`)

## CLI

* Allow disabling progress reporting during query executing in the CLI client by
  specifying `--no-progress` ({issue}`11894`)
* Reduce latency for very short queries. ({issue}`11768`)

## Delta Lake connector

* Improve query planning performance. ({issue}`11858`)
* Fix failure when reading from `information_schema.columns` when metastore
  contains views. ({issue}`11946`)
* Add support for dropping tables with invalid metadata. ({issue}`11924`)
* Fix query failure when partition column has a `null` value and query has a
  complex predicate on that partition column. ({issue}`12056`)

## Hive connector

* Improve query planning performance. ({issue}`11858`)

## Iceberg connector

* Add support for hidden `$path` columns. ({issue}`8769`)
* Add support for creating tables with either Iceberg format version 1, or 2. ({issue}`11880`)
* Add the `expire_snapshots` table procedure. ({issue}`10810`)
* Add the `delete_orphan_files` table procedure. ({issue}`10810`)
* Allow reading Iceberg tables written by Glue that have locations containing
  double slashes. ({issue}`11964`)
* Improve query planning performance. ({issue}`11858`)
* Fix query failure with a dynamic filter prunes a split on a worker node. ({issue}`11976`)
* Include missing `format_version` property in `SHOW CREATE TABLE` output. ({issue}`11980`)

## MySQL connector

* Improve query planning performance. ({issue}`11858`)

## Pinot connector

* Support querying tables having non-lowercase names in Pinot. ({issue}`6789`)
* Fix handling of hybrid tables in Pinot and stop returning duplicate data. ({issue}`10125`)

## PostgreSQL connector

* Improve query planning performance. ({issue}`11858`)

## SQL Server connector

* Improve query planning performance. ({issue}`11858`)

## SPI

* Deprecate passing constraints to `ConnectorMetadata.getTableStatistics()`.
  Constraints can be associated with the table handle in
  `ConnectorMetadata.applyFilter()`. ({issue}`11877`)
