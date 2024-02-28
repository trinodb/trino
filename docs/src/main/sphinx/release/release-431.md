# Release 431 (27 Oct 2023)

## General

* Add support for [](/routines). ({issue}`19308`)
* Add support for [](/sql/create-function) and [](/sql/drop-function) statements. ({issue}`19308`)
* Add support for the `REPLACE` modifier to the `CREATE TABLE` statement. ({issue}`13180`)
* Disallow a `null` offset for the {func}`lead` and {func}`lag` functions. ({issue}`19003`)
* Improve performance of queries with short running splits. ({issue}`19487`)

## Security

* Support defining rules for procedures in file-based access control. ({issue}`19416`)
* Mask additional sensitive values in log files. ({issue}`19519`)

## JDBC driver

* Improve latency for prepared statements for Trino versions that support
  `EXECUTE IMMEDIATE` when the `explicitPrepare` parameter to is set to `false`.
  ({issue}`19541`)

## Delta Lake connector

* Replace the `hive.metastore-timeout` Hive metastore configuration property
  with the `hive.metastore.thrift.client.connect-timeout` and
  `hive.metastore.thrift.client.read-timeout` properties. ({issue}`19390`)

## Hive connector

* Add support for [SQL routine management](sql-routine-management). ({issue}`19308`)
* Replace the `hive.metastore-timeout` Hive metastore configuration property
  with the `hive.metastore.thrift.client.connect-timeout` and
  `hive.metastore.thrift.client.read-timeout` properties. ({issue}`19390`)
* Improve support for concurrent updates of table statistics in Glue. ({issue}`19463`)
* Fix Hive view translation failures involving comparisons between char and
  varchar fields. ({issue}`18337`)

## Hudi connector

* Replace the `hive.metastore-timeout` Hive metastore configuration property
  with the `hive.metastore.thrift.client.connect-timeout` and
  `hive.metastore.thrift.client.read-timeout` properties. ({issue}`19390`)

## Iceberg connector

* Add support for the `REPLACE` modifier to the `CREATE TABLE` statement. ({issue}`13180`)
* Replace the `hive.metastore-timeout` Hive metastore configuration property
  with the `hive.metastore.thrift.client.connect-timeout` and
  `hive.metastore.thrift.client.read-timeout` properties. ({issue}`19390`)

## Memory connector

* Add support for [SQL routine management](sql-routine-management). ({issue}`19308`)

## SPI

* Add `ValueBlock` abstraction along with `VALUE_BLOCK_POSITION` and
  `VALUE_BLOCK_POSITION_NOT_NULL` calling conventions. ({issue}`19385`)
* Require a separate block position for each argument of aggregation functions.
  ({issue}`19385`)
* Require implementations of `Block` to implement `ValueBlock`. ({issue}`19480`)
