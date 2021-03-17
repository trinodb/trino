# Release 354 (19 Mar 2021)

## General

* Improve performance of certain queries involving `LEFT`, `RIGHT` and `FULL JOIN`
  when one side of the join is known to produce a single row. ({issue}`7090`)
* Preferred write partitioning is now triggered automatically when the estimated number
  of written partitions exceeds or is equal to `preferred-write-partitioning-min-number-of-partitions`.
  This estimation requires that the input tables of the query have statistics. To enable
  preferred partitioning unconditionally, set `preferred-write-partitioning-min-number-of-partitions` to `1`.
  To disable preferred write partitioning, set `use-preferred-write-partitioning` to `false`.
  ({issue}`6920`)
* Fix incorrect results when multiple similar subqueries contain `UNION` clauses that differ
  only in the `ALL` vs `DISTINCT` qualifier. ({issue}`7345`)
* Fix `DELETE` and `UPDATE` for connectors that subsume filters. ({issue}`7302`)
* Fix failure when subqueries contain `EXCEPT` or `INTERSECT`. ({issue}`7342`)
* Fix failure of queries that contain `RIGHT JOIN` when late materialization is enabled. ({issue}`6784`)

## Security

* Fix retries for OAuth 2.0 authentication in case of token expiry. ({issue}`7172`)

## CLI

* Support OAuth 2.0 authentication. ({issue}`7054`)

## ClickHouse connector

* Use correct case for name of the schema in `CREATE SCHEMA`. ({issue}`7239`)

## Elasticsearch connector

* Fix failure when reading single-valued fields for array types. ({issue}`7012`)

## Hive connector

* Respect partition filter for `DELETE` and `UPDATE` of ACID tables. Previously, the partition
  filter was ignored, resulting in the deletion or update of too many rows. ({issue}`7302`)
* Fix allocation of statement IDs for ACID tables, which could result in query failure or
  data loss due to creating multiple delta files with the same name. ({issue}`7302`)
* Fix incorrect query results when reading from an incorrectly bucketed table created and registered
  with the metastore by Spark. ({issue}`6848`)
* Avoid leaking file system connections or other resources when using the Avro file format. ({issue}`7178`)
* Fix query failure when columns of a CSV table are declared as a type other than `varchar` (`string`) in Glue
  metastore. Columns are now interpreted as `varchar` values, instead. ({issue}`7059`)
* Rename `hive.parallel-partitioned-bucketed-inserts` configuration property to `hive.parallel-partitioned-bucketed-writes`. ({issue}`7259`)

## Iceberg connector

* Fix queries on empty tables without a snapshot ID that were created by Spark. ({issue}`7233`)
* Update to Iceberg 0.11.0 behavior for transforms of dates and timestamps
  before 1970. Data written by older versions of Trino and Iceberg will be
  read correctly. New data will be written correctly, but may be read
  incorrectly by older versions of Trino and Iceberg. ({issue}`7049`)

## MemSQL connector

* Add support for MemSQL 3.2. ({issue}`7179`)
* Use correct case for name of the schema in `CREATE SCHEMA`. ({issue}`7239`)
* Improve performance of queries with `ORDER BY ... LIMIT` clause when the computation
  can be pushed down to the underlying database. ({issue}`7326`)

## MySQL connector

* Use proper column type (`datetime(3)`) in MySQL when creating a table with `timestamp(3)` column.
  Previously, the second fraction was being truncated. ({issue}`6909`)
* Use correct case for name of the schema in `CREATE SCHEMA`. ({issue}`7239`)
* Improve performance of queries with `ORDER BY ... LIMIT` clause when the computation
  can be pushed down to the underlying database. ({issue}`7326`)

## PostgreSQL connector

* Fix incorrect query results for `ORDER BY ... LIMIT` clause when sorting on  `char` or `varchar` columns
  and `topn-pushdown.enabled` configuration property is enabled. The optimization is now enabled by default.
  ({issue}`7170`, {issue}`7314`)
* Use correct case for name of the schema in `CREATE SCHEMA`. ({issue}`7239`)

## Redshift connector

* Fix failure when query contains a `LIMIT` exceeding 2147483647. ({issue}`7236`)
* Use correct case for name of the schema in `CREATE SCHEMA`. ({issue}`7239`)

## SQL Server connector

* Add support for parametric `time` type. ({issue}`7122`)
* Use correct case for name of the schema in `CREATE SCHEMA`. ({issue}`7239`)
* Improve performance of queries with `ORDER BY ... LIMIT` clause when the computation
  can be pushed down to the underlying database. ({issue}`7324`)
