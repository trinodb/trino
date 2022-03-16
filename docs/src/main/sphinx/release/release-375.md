# Release 375 (28 Mar 2022)

## General

* Change behavior of `ALTER TABLE qualified_name TO unqualified_name` to leave
  the table in the schema. This is backwards-incompatible behavioral change. ({issue}`11282`)
* Disallow table redirections for the `GRANT`, `REVOKE`, and
  `SET TABLE AUTHORIZATION` table tasks. ({issue}`11302`)
* Improve performance of queries that contain filter predicates on aggregation
  results. ({issue}`11469`)
* Improve performance of queries that contain `array_distinct(array_sort(…))`
  expressions. ({issue}`8777`)
* Fix `SHOW CREATE TABLE` to show actual table name in case of table
  redirections, so that the statement can be used to recreate the table. ({issue}`11604`)
* Fix scheduling for non-remotely accessible splits in fault-toleration execution. ({issue}`11581`)
* Fix incorrect `is_nullable` value in `information_schema.columns` table. ({issue}`11613`)

## JDBC driver

* Allow `PreparedStatement.close()` to be called multiple times. ({issue}`11620`)
* Fix incorrect `is_nullable` value in `DatabaseMetaData.getColumns()` method. ({issue}`11613`)

## Cassandra connector

* Return `0` instead of row count in completed bytes. ({issue}`11644`)

## Delta Lake connector

* Add access control to `drop_extended_stats` and `vacuum` procedures. ({issue}`11633`)
* Fix incorrect query results when query executes concurrent with `DROP TABLE`. ({issue}`11562`)

## Hive connector

* Fix infinite loop in the query optimizer when query contains predicates on a
  struct field. ({issue}`11559`)
* Fix query failure when reading a partitioned table with a predicate on a
  partition column with a specific name such as `table`, `key`, `order`, and
  others. ({issue}`11512`)

## Iceberg connector

* Fix failure when query contains predicates on a struct field. ({issue}`11560`)
* Fix query failure when reading from `$files` system table after a table column
  has been dropped. ({issue}`11576`)

## Kudu connector

* Improve write performance by flushing operations in batches. ({issue}`11264`)
* Fix failure when multiple queries run concurrently and schema emulation is enabled. ({issue}`11264`)

## MongoDB connector

* Support predicate pushdown on `boolean` columns. ({issue}`11536`)
* Return `0` instead of row count in completed bytes. ({issue}`11679`)

## MySQL connector

* Add support for table comments. ({issue}`11211`)

## Pinot connector

* Fix handling of passthrough queries that contain aggregation functions. ({issue}`9137`)
* Fix incorrect results when aggregation functions on columns having
  non-lowercase names are pushed down to Pinot. ({issue}`9137`, {issue}`10148`)
* Fix possible incorrect results when grouping on columns of array types. ({issue}`9781`)

## PostgreSQL connector

* Improve performance of queries involving `OR` with `IS NULL`, `IS NOT NULL`
  predicates, or involving `NOT` expression by pushing predicate computation to
  the PostgreSQL database. ({issue}`11514`)
* Improve performance of queries with predicates involving `nullif` function by
  pushing predicate computation to the PostgreSQL database. ({issue}`11532`)
* Improve performance of queries involving joins by pushing computation to the
  PostgreSQL database. ({issue}`11635`)
* Improve performance of queries involving predicates with arithmetic
  expressions by pushing predicate computation to the PostgreSQL database. ({issue}`11510`)
* Fix deletion of too much data when delete query involves a `LIKE` predicate. ({issue}`11615`)

## SPI

* Add processed input bytes and rows to query events in event listener. ({issue}`11623`)
* Remove deprecated constructors from `ColumnMetadata`. ({issue}`11621`)
