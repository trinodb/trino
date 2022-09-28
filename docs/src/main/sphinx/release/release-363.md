# Release 363 (6 Oct 2021)

## General

* Add {doc}`/admin/event-listeners-http` implementation which sends JSON serialized events to a remote HTTP endpoint. ({issue}`8821`)
* Improve performance of queries that group by `bigint` columns. ({issue}`9510`)
* Improve performance of queries that process row or array data. ({issue}`9402`)
* Improve query planning performance. ({issue}`9462`)
* Reduce query memory usage when spilling occurs. ({issue}`9270`, {issue}`9275`)
* Reduce memory usage for processing `JOIN` clauses. ({issue}`9327`)
* Fix potential data loss in query results when clients retry requests to the coordinator. ({issue}`9453`)
* Fix incorrect result for comparisons between zero-valued decimals. ({issue}`8985`)
* Fix `SHOW ROLES` failure when there are no roles to display. ({issue}`9319`)
* Fix `EXPLAIN ANALYZE` to show estimates. ({issue}`9396`)
* Fix incorrect result for {func}`round` with precision set to 0. ({issue}`9371`) 
* Respect `deprecated.legacy-catalog-roles=true` configuration property in `SHOW ROLES`, 
  `SHOW CURRENT ROLES` and `SHOW ROLE GRANTS` statements. ({issue}`9496`)

## Python client

* Fix column type reported in `cursor.description` for `time with time zone` column. ({issue}`9460`)

## BigQuery connector

* Fix failure for queries where predicates on `geography`, `array` or `struct` column are pushed down to BigQuery. ({issue}`9391`)

## Cassandra connector

* Add support for Cassandra `tuple` type. ({issue}`8570`)

## Elasticsearch connector

* Add support for `scaled_float` type. ({issue}`9358`)

## Hive connector

* Support usage of `avro_schema_url` table property in partitioned tables. ({issue}`9370`}
* Add support for insert overwrite operations on S3-backed tables. ({issue}`9234`)
* Improve query performance when reading Parquet data with predicate on a `decimal` column. ({issue}`9338`)
* Fix `Failed reading parquet data: Socket is closed by peer` query failure when reading from Parquet table with a predicate. ({issue}`9097`)
* Fix query failure when updating or deleting from an ORC ACID transactional table that has some rows deleted since the last major compaction. ({issue}`9354`)
* Fix failure when reading large Parquet files. ({issue}`9469`)
* Fix failures for some `UPDATE` queries, such as those where the `SET` clause contains the same constant more than once. ({issue}`9295`)
* Fix incorrect results when filtering on Parquet columns containing a dot in their name. ({issue}`9516`)

## Iceberg connector

* Improve query performance when reading Parquet data with predicate on a `decimal` column. ({issue}`9338`)
* Fix support for comments when adding a new column. Previously, they were silently ignored. ({issue}`9123`)
* Fix page and block sizing when writing Parquet data. ({issue}`9326`)
* Fix failure when reading large Parquet files. ({issue}`9469`)

## MySQL connector

* Add support for variable precision `time` type. ({issue}`9339`)
* Support `CREATE TABLE` and `CREATE TABLE AS` statements for `time` type. ({issue}`9339`)

## Phoenix connector

* Allowing forcing the mapping of certain types to `varchar`. This can be enabled by
  setting the `jdbc-types-mapped-to-varchar` configuration property to a comma-separated
  list of type names. ({issue}`2084`)

## Pinot connector

* Fix failure when a column name is a reserved keyword. ({issue}`9373`)

## SQL Server connector

* Add support for SQL Server `datetimeoffset` type. ({issue}`9329`)
* Fix failure for queries where predicates on `text` or `ntext` typed columns are pushed down to SQL Server. ({issue}`9387`)
