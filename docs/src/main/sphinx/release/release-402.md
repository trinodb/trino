# Release 402 (2 Nov 2022)

## General

* Fix query processing when [fault-tolerant execution](/admin/fault-tolerant-execution)
  is enabled and a [stage](trino-concept-stage) of the query produces no data. ({issue}`14794`)

## Blackhole connector

* Add support for column comments on view columns. ({issue}`10705`)

## Clickhouse connector

* Reuse JDBC connections for metadata queries. This can be disabled with the
  `query.reuse-connection` configuration property. ({issue}`14653`)

## Delta Lake connector

* Remove the deprecated `hive.parquet.fail-on-corrupted-statistics` and
  `parquet.fail-on-corrupted-statistics` configuration properties. The
  `parquet.ignore-statistics` property can be used to allow querying Parquet
  files with corrupted or incorrect statistics. ({issue}`14777`)
* Fix memory leak and improve memory tracking during large `INSERT` queries. ({issue}`14823`)

## Druid connector

* Reuse JDBC connections for metadata queries. This can be disabled with the
  `query.reuse-connection` configuration property. ({issue}`14653`)

## Hive connector

* Add support for column comments on view columns. ({issue}`10705`)
* Remove the deprecated `hive.parquet.fail-on-corrupted-statistics` and
  `parquet.fail-on-corrupted-statistics` configuration properties. The
  `parquet.ignore-statistics` property can be used to allow querying Parquet
  files with corrupted or incorrect statistics. ({issue}`14777`)
* Fix memory leak and improve memory tracking during large `INSERT` queries. ({issue}`14823`)

## Hudi connector

* Remove the deprecated `hive.parquet.fail-on-corrupted-statistics` and
  `parquet.fail-on-corrupted-statistics` configuration properties. The
  `parquet.ignore-statistics` property can be used to allow querying Parquet
  files with corrupted or incorrect statistics. ({issue}`14777`)

## Iceberg connector

* Add support to skip archiving when committing to a table in the Glue
  metastore and the `iceberg.glue.skip-archive` configuration property is set
  to true. ({issue}`13413`)
* Add support for column comments on view columns. ({issue}`10705`)
* Remove the deprecated `hive.parquet.fail-on-corrupted-statistics` and
  `parquet.fail-on-corrupted-statistics` configuration properties. The
  `parquet.ignore-statistics` property can be used to allow querying Parquet
  files with corrupted or incorrect statistics. ({issue}`14777`)
* Fix incorrect results when the column order in the equality delete filter is
  different from the table definition. ({issue}`14693`)
* Fix memory leak and improve memory tracking during large `INSERT` queries. ({issue}`14823`)

## MariaDB connector

* Reuse JDBC connections for metadata queries. This can be disabled with the
  `query.reuse-connection` configuration property. ({issue}`14653`)

## Memory connector

* Add support for column comments on view columns. ({issue}`10705`)

## MySQL connector

* Reuse JDBC connections for metadata queries. This can be disabled with the
  `query.reuse-connection` configuration property. ({issue}`14653`)

## MongoDB connector

* Support predicate pushdown on `date`, `time(3)`, `timestamp(3)` and
  `timestamp(3) with time zone` columns. ({issue}`14795`)

## Oracle connector

* Reuse JDBC connections for metadata queries. This can be disabled with the
  `query.reuse-connection` configuration property. ({issue}`14653`)

## Phoenix connector

* Reuse JDBC connections for metadata queries. This can be disabled with the
  `query.reuse-connection` configuration property. ({issue}`14653`)

## PostgreSQL connector

* Reuse JDBC connections for metadata queries. This can be disabled with the
  `query.reuse-connection` configuration property. ({issue}`14653`)

## Redshift connector

* Reuse JDBC connections for metadata queries. This can be disabled with the
  `query.reuse-connection` configuration property. ({issue}`14653`)

## SingleStore (MemSQL) connector

* Reuse JDBC connections for metadata queries. This can be disabled with the
  `query.reuse-connection` configuration property. ({issue}`14653`)

## SQL Server connector

* Improve performance of certain queries which use the `OR` operator. ({issue}`14570`)
* Improve performance of queries with predicates involving the `nullif` function
  or arithmetic expressions. ({issue}`14570`)
