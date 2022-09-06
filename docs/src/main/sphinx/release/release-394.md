# Release 394 (29 Aug 2022)

## General

* Add support for JSON as an output format of `EXPLAIN`. ({issue}`12968`)
* Improve performance of queries involving joins on a single `bigint` column. ({issue}`13432`)
* Improve performance of `LIKE` expressions. ({issue}`13479`)
* Ensure `UPDATE` queries cannot insert null values into columns with a
  `NOT NULL` constraint. ({issue}`13435`)
* Fix failure when an `UPDATE` query contains a `WHERE` clause which always
  evaluates to false. ({issue}`12422`)
* Fix potential failure for queries involving joins and implicit or explicit
  casts of `null` to a concrete type. ({issue}`13565`)

## Docker image

* Ensure Trino stops running with insufficient resources to avoid partial
  failures. ({issue}`13736`)

## BigQuery connector

* Add `query` table function for full query pass-through to the connector. ({issue}`12502`)
* Add support for the `INSERT` statement. ({issue}`6868`)
* Add support for the `CREATE TABLE ... AS SELECT ...` statement. ({issue}`6869`)

## Delta Lake connector

* Disallow adding a column with a `NOT NULL` constraint to a table which is not
  empty. ({issue}`13785`)
* Fix failure when reading Parquet data that contains only null values. ({issue}`9424`)
* Fix potential failure of unrelated queries after dropping a schema. ({issue}`13810`)

## Druid connector

* Improve performance of queries that perform filtering on `varchar` columns that
  contain temporal data with the format `YYYY-MM-DD`. ({issue}`12925`)

## Elasticsearch connector

* Add support for multiple hosts in the `elasticsearch.host` configuration
  property. ({issue}`12530`)

## Hive connector

* Add support for a Kerberos credential cache. ({issue}`13482`)
* Fix failure when reading Parquet data that contains only null values. ({issue}`9424`)
* Fix failure when the metastore returns duplicated column statistics. ({issue}`13787`)
* Fix potential failure of unrelated queries after dropping a schema. ({issue}`13810`)

## Iceberg connector

* Improve query planning performance when a `varchar` partitioning column
  contains date values in the `YYYY-MM-DD` format. ({issue}`12925`)
* Fix query failure when using the `[VERSION | TIMESTAMP] AS OF` clause on a
  table created with Iceberg versions older than 0.12. ({issue}`13613`)
* Fix failure when reading Parquet data that contains only null values. ({issue}`9424`)

## Oracle connector

* Improve performance of queries that perform filtering on `varchar` columns that
  contain temporal data with the format `YYYY-MM-DD`. ({issue}`12925`)

## Phoenix connector

* Improve performance of queries that perform filtering on `varchar` columns that
  contain temporal data with the format `YYYY-MM-DD`. ({issue}`12925`)

## Pinot connector

* Add support for TLS when connecting to the Pinot controllers and brokers. ({issue}`13410`)
* Fix query failure when using the `HAVING` clause. ({issue}`13429`)

## PostgreSQL connector

* Improve performance of queries that perform filtering on `varchar` columns
  that contain temporal data with the format `YYYY-MM-DD`. ({issue}`12925`)
* Prevent using a column name which is longer than the maximum length supported
  by PostgreSQL. Previously, long names were truncated. ({issue}`13742`)

## SQL Server connector

* Prevent renaming a column to a name which is longer than the maximum length
  supported by SQL Server. Previously, long names were truncated. ({issue}`13742`)

## SPI

* Add the query plan in JSON format to `QueryCompletedEvent`, and allow
  connectors to request anonymized query plans in the `QueryCompletedEvent`. ({issue}`12968`)
