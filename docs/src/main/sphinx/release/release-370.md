# Release 370 (3 Feb 2022)

## General

* Add support for `DEFAULT` keyword in `ALTER TABLE...SET PROPERTIES...`.
  ({issue}`10331`)
* Improve performance of map and row types. ({issue}`10469`)
* Improve performance when evaluating expressions in `WHERE` and `SELECT`
  clauses. ({issue}`10322`)
* Prevent queries deadlock when using `phased` execution policy with dynamic
  filters in multi-join queries. ({issue}`10868`)
* Fix query scheduling regression introduced in Trino 360 that caused
  coordinator slowness in assigning splits to workers. ({issue}`10839`)
* Fix `information_schema` query failure when an `IS NOT NULL` predicate is
  used. ({issue}`10861`)
* Fix failure when nested subquery contains a `TABLESAMPLE` clause.
  ({issue}`10764`)

## Security

* Reduced the latency of successful OAuth 2.0 authentication. ({issue}`10929`)
* Fix server start failure when using JWT and OAuth 2.0 authentication together
  (`http-server.authentication.type=jwt,oauth2`). ({issue}`10811`)

## CLI

* Add support for ARM64 processors. ({issue}`10177`)
* Allow to choose the way how external authentication is handled with the
  `--external-authentication-redirect-handler` parameter. ({issue}`10248`)

## RPM package

* Fix failure when operating system open file count is set too low.
  ({issue}`8819`)

## Docker image

* Change base image to `registry.access.redhat.com/ubi8/ubi`, since CentOS 8 has
  reached end-of-life. ({issue}`10866`)

## Cassandra connector

* Fix query failure when pushing predicates on `uuid` partitioned columns.
  ({issue}`10799`)

## ClickHouse connector

* Support creating tables with Trino `timestamp(0)` type columns.
* Drop support for ClickHouse servers older than version 20.7 to avoid using a
  deprecated driver. You can continue to use the deprecated driver with the
  `clickhouse.legacy-driver` flag when connecting to old servers.
  ({issue}`10541`)
* Remove the legacy `allow-drop-table` configuration property. This defaulted to
  `false`, which disallowed dropping tables, but other modifications were still
  allowed. Use {doc}`/security/built-in-system-access-control` instead, if
  desired. ({issue}`588`)

## Druid connector

* Remove the legacy `allow-drop-table` configuration property. This defaulted to
  `false`, which disallowed dropping tables, but other modifications were still
  allowed. Use {doc}`/security/built-in-system-access-control` instead, if
  desired. ({issue}`588`)

## Hive connector

* Improve query performance when reading ORC data. ({issue}`10575`)
* Add configuration property `hive.single-statement-writes` to require
  auto-commit for writes. This can be used to disallow multi-statement write
  transactions. ({issue}`10820`)
* Fix sporadic query failure `Partition no longer exists` when working with wide
  tables using a AWS Glue catalog as metastore. ({issue}`10696`)
* Fix `SHOW TABLES` failure when `hive.hide-delta-lake-tables` is enabled, and
  Glue metastore references the table with no properties. ({issue}`10864`)

## Iceberg connector

* Fix query failure when reading from a table that underwent partitioning
  evolution. ({issue}`10770`)
* Fix data corruption when writing Parquet files. ({issue}`9749`)

## MySQL connector

* Remove the legacy `allow-drop-table` configuration property. This defaulted to
  `false`, which disallowed dropping tables, but other modifications were still
  allowed. Use {doc}`/security/built-in-system-access-control` instead, if
  desired. ({issue}`588`)

## Oracle connector

* Remove the legacy `allow-drop-table` configuration property. This defaulted to
  `false`, which disallowed dropping tables, but other modifications were still
  allowed. Use {doc}`/security/built-in-system-access-control` instead, if
  desired. ({issue}`588`)

## Phoenix connector

* Fix incorrect result when a `date` value is older than or equal to
  `1899-12-31`. ({issue}`10749`)

## PostgreSQL connector

* Remove the legacy `allow-drop-table` configuration property. This defaulted to
  `false`, which disallowed dropping tables, but other modifications were still
  allowed. Use {doc}`/security/built-in-system-access-control` instead, if
  desired. ({issue}`588`)

## Redshift connector

* Remove the legacy `allow-drop-table` configuration property. This defaulted to
  `false`, which disallowed dropping tables, but other modifications were still
  allowed. Use {doc}`/security/built-in-system-access-control` instead, if
  desired. ({issue}`588`)

## SingleStore (MemSQL) connector

* Remove the legacy `allow-drop-table` configuration property. This defaulted to
  `false`, which disallowed dropping tables, but other modifications were still
  allowed. Use {doc}`/security/built-in-system-access-control` instead, if
  desired. ({issue}`588`)

## SQL Server connector

* Remove the legacy `allow-drop-table` configuration property. This defaulted to
  `false`, which disallowed dropping tables, but other modifications were still
  allowed. Use {doc}`/security/built-in-system-access-control` instead, if
  desired. ({issue}`588`)

## SPI

* Allow null property names in `ConnetorMetadata#setTableProperties`.
  ({issue}`10331`)
* Rename `ConnectorNewTableLayout`  to `ConnectorTableLayout`. ({issue}`10587`)
* Connectors no longer need to explicitly declare handle classes.  The
  `ConnectorFactory.getHandleResolver` and `Connector.getHandleResolver` methods
  are removed. ({issue}`10858`, {issue}`10872`)
* Remove unnecessary `Block.writePositionTo` and `BlockBuilder.appendStructure`
  methods. Use of these methods can be replaced with the existing
  `Type.appendTo` or `writeObject` methods. ({issue}`10602`)
