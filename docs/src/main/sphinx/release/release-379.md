# Release 379 (28 Apr 2022)

## General

* Add {doc}`/connector/mariadb`. ({issue}`10046`)
* Improve performance of queries that contain `JOIN` and `UNION` clauses. ({issue}`11935`)
* Improve performance of queries that contain `GROUP BY` clauses. ({issue}`12095`)
* Fail `DROP TABLE IF EXISTS` when deleted entity is not a table. Previously the
  statement did not delete anything. ({issue}`11555`)
* Fail `DROP VIEW IF EXISTS` when deleted entity is not a view. Previously the
  statement did not delete anything. ({issue}`11555`)
* Fail `DROP MATERIALIZED VIEW IF EXISTS` when deleted entity is not a
  materialized view. Previously the statement did not delete anything.
  ({issue}`11555`)

## Web UI

* Group information about tasks by stage. ({issue}`12099`)
* Show aggregated statistics for failed tasks of queries that are executed with
  `retry-policy` set to `TASK`. ({issue}`12099`)
* Fix reporting of `physical input read time`. ({issue}`12135`)

## Delta Lake connector

* Add support for Google Cloud Storage. ({issue}`12144`)
* Fix failure when reading from `information_schema.columns` when non-Delta
  tables are present in the metastore. ({issue}`12122`)

## Iceberg connector

* Add support for {doc}`/sql/delete` with arbitrary predicates. ({issue}`11886`)
* Improve compatibility when Glue storage properties are used. ({issue}`12164`)
* Prevent data loss when queries modify a table concurrently when Glue catalog
  is used. ({issue}`11713`)
* Enable commit retries when conflicts occur writing a transaction to a Hive Metastore. ({issue}`12419`)
* Always return the number of deleted rows for {doc}`/sql/delete` statements. ({issue}`12055`)

## Pinot connector

* Add support for Pinot 0.10. ({issue}`11475`)

## Redis connector

* Improve performance when reading data from Redis. ({issue}`12108`)

## SQL Server connector

* Properly apply snapshot isolation to all connections when it is enabled. ({issue}`11662`)
