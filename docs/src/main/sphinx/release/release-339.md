# Release 339 (21 Jul 2020)

## General

* Add {func}`approx_most_frequent`. ({issue}`3425`)
* Physical bytes scan limit for queries can be configured via `query.max-scan-physical-bytes` configuration property
  and `query_max_scan_physical_bytes` session property. ({issue}`4075`)
* Remove support for addition and subtraction between `TIME` and `INTERVAL YEAR TO MONTH` types. ({issue}`4308`)
* Fix planning failure when join criteria contains subqueries. ({issue}`4380`)
* Fix failure when subquery appear in window function arguments. ({issue}`4127`)
* Fix failure when subquery in `WITH` clause contains hidden columns. ({issue}`4423`)
* Fix failure when referring to type names with different case in a `GROUP BY` clause. ({issue}`2960`)
* Fix failure for queries involving `DISTINCT` when expressions in `ORDER BY` clause differ by case from expressions in `SELECT` clause. ({issue}`4233`)
* Fix incorrect type reporting for `TIMESTAMP` and `TIMESTAMP WITH TIME ZONE` for legacy clients. ({issue}`4408`)
* Fix failure when querying nested `TIMESTAMP` or `TIMESTAMP WITH TIME ZONE` for legacy clients. ({issue}`4475`, {issue}`4425`)
* Fix failure when parsing timestamps with time zone with an offset of the form `+NNNN`. ({issue}`4490`)

## JDBC driver

* Fix reading `TIMESTAMP` and `TIMESTAMP WITH TIME ZONE` values with a negative year
  or a year higher than 9999. ({issue}`4364`)
* Fix incorrect column size metadata for `TIMESTAMP` and `TIMESTAMP WITH TIME ZONE` types. ({issue}`4411`)
* Return correct value from `ResultSet.getDate()`, `ResultSet.getTime()` and `ResultSet.getTimestamp()` methods
  when session zone is set to a different zone than the default zone of the JVM the JDBC is run in.
  The previous behavior can temporarily be restored using `useSessionTimeZone` JDBC connection
  parameter. ({issue}`4017`)

## Druid connector

* Fix handling of table and column names containing non-ASCII characters. ({issue}`4312`)

## Hive connector

* Make `location` parameter optional for the `system.register_partition` procedure. ({issue}`4443`)
* Avoid creating tiny splits at the end of block boundaries. ({issue}`4485`)
* Remove requirement to configure `metastore.storage.schema.reader.impl` in Hive 3.x metastore
  to let Presto access CSV tables. ({issue}`4457`)
* Fail query if there are bucket files outside of the bucket range.
  Previously, these extra files were skipped. ({issue}`4378`)
* Fix a query failure when reading from Parquet file containing `real` or `double` `NaN` values,
  if the file was written by a non-conforming writer. ({issue}`4267`)

## Kafka connector

* Add insert support for Avro. ({issue}`4418`)
* Add insert support for CSV. ({issue}`4287`)

## Kudu connector

* Add support for grouped execution. It can be enabled with the `kudu.grouped-execution.enabled`
  configuration property or the `grouped_execution` session property. ({issue}`3715`)

## MongoDB connector

* Allow querying Azure Cosmos DB. ({issue}`4415`)

## Oracle connector

* Allow providing credentials via the `connection-user` and `connection-password`
  configuration properties. These properties were previously ignored if connection pooling
  was enabled. ({issue}`4430`)

## Phoenix connector

* Fix handling of row key definition with white space. ({issue}`3251`)

## SPI

* Allow connectors to wait for dynamic filters before splits are generated via the new
  `DynamicFilter` object passed to `ConnectorSplitManager.getSplits()`. ({issue}`4224`)
