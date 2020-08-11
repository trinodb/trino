# Release 340 (8 Aug 2020)

## General Changes

* Add support for query parameters in `LIMIT`, `OFFSET` and `FETCH FIRST` clauses. ({issue}`4529`, {issue}`4601`)
* Add experimental support for recursive queries. ({issue}`4250`)
* Add {func}`bitwise_left_shift`, {func}`bitwise_right_shift` and {func}`bitwise_right_shift_arithmetic`. ({issue}`740`)
* Add {func}`luhn_check`. ({issue}`4011`)
* Add `IF EXISTS `and `IF NOT EXISTS` syntax to `ALTER TABLE`. ({issue}`4651`)
* Include remote host in error info for page transport errors. ({issue}`4511`)
* Improve minimum latency for dynamic partition pruning. ({issue}`4388`)
* Reduce cluster load by cancelling query stages from which data is no longer required. ({issue}`4290`)
* Reduce query memory usage by improving retained size estimation for `VARCHAR` and `CHAR` types. ({issue}`4123`)
* Improve query performance for queries containing {func}`starts_with`. ({issue}`4669`)
* Improve performance of queries that use `DECIMAL` data type. ({issue}`4730`)
* Fix failure when `GROUP BY` clause contains duplicate expressions. ({issue}`4609`)
* Fix potential hang during query planning ({issue}`4635`).

## Security Changes

* Fix unprivileged access to table's schema via `CREATE TABLE LIKE`. ({issue}`4472`)

## JDBC Driver Changes

* Fix handling of dates before 1582-10-15. ({issue}`4563`)
* Fix handling of timestamps before 1900-01-01. ({issue}`4563`)

## Elasticsearch Connector Changes

* Fix failure when index mapping is missing. ({issue}`4535`)

## Hive Connector Changes

* Allow creating a table with `external_location` when schema's location is not valid. ({issue}`4069`)
* Add read support for tables that were created as non-transactional and converted to be
  transactional later. ({issue}`2293`)
* Allow creation of transactional tables. Note that writing to transactional tables
  is not yet supported. ({issue}`4516`)
* Add `hive.metastore.glue.max-error-retries` configuration property for the
  number of retries performed when accessing the Glue metastore. ({issue}`4611`)
* Support using Java KeyStore files for Thrift metastore TLS configuration. ({issue}`4432`)
* Expose hit rate statistics for Hive metastore cache via JMX. ({issue}`4458`)
* Improve performance when querying a table with large files and with `skip.header.line.count` property set to 1. ({issue}`4513`)
* Improve performance of reading JSON tables. ({issue}`4705`)
* Fix query failure when S3 data location contains a `_$folder$` marker object. ({issue}`4552`)
* Fix failure when referencing nested fields of a `ROW` type when table and partition metadata differs. ({issue}`3967`)

## Kafka Connector Changes

* Add insert support for Raw data format. ({issue}`4417`)
* Add insert support for JSON. ({issue}`4477`)
* Remove unused `kafka.connect-timeout` configuration properties. ({issue}`4664`)

## MongoDB Connector Changes

* Add `mongodb.max-connection-idle-time` properties to limit the maximum idle time of a pooled connection. ({issue}`4483`)

## Phoenix Connector Changes

* Add table level property to specify data block encoding when creating tables. ({issue}`4617`)
* Fix query failure when listing schemas. ({issue}`4560`)

## PostgreSQL Connector Changes

* Push down {func}`count` aggregations over constant expressions.
  For example, `SELECT count(1)`. ({issue}`4362`)

## SPI Changes

* Expose information about query type in query Event Listener. ({issue}`4592`)
* Add support for TopN pushdown via the `ConnectorMetadata.applyLimit()` method. ({issue}`4249`)
* Deprecate the older variants of `ConnectorSplitManager.getSplits()`. ({issue}`4508`)
