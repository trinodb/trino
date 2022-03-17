# Release 374 (17 Mar 2022)

## General

* Add support for query parameters in `CREATE SCHEMA`. ({issue}`11485`)
* Improve performance when reading from S3-based spool for
  [fault-tolerant execution](/admin/fault-tolerant-execution). ({issue}`11050`)
* Improve performance of queries with `GROUP BY` clauses. ({issue}`11392`)
* Improve performance of `GROUP BY` with a large number of groups. ({issue}`11011`)
* Improve handling of queries where individual tasks require lots of memory when
  `retry-policy` is set to `TASK`. ({issue}`10432`)
* Produce better query plans by improving cost-based-optimizer estimates in the
  presence of correlated columns. ({issue}`11324`)
* Fix memory accounting and improve performance for queries involving certain
  variable-width data types such as `varchar` or `varbinary`. ({issue}`11315`)
* Fix performance regression for `GROUP BY` queries. ({issue}`11234`)
* Fix `trim`, `ltrim` and `rtim` function results when the argument is `char`
  type. Previously, it returned padded results as `char` type. It returns
  `varchar` type without padding now. ({issue}`11440`)

## JDBC driver

* Add support for `DatabaseMetaData.getImportedKeys`. ({issue}`8708`)
* Fix `Driver.getPropertyInfo()`, and validate allowed properties. ({issue}`10624`)

## CLI

* Add support for selecting Vim or Emacs editing modes with the `--editing-mode`
  command line argument. ({issue}`3377`)

## Cassandra connector

* Add support for [TRUNCATE TABLE](/sql/truncate). ({issue}`11425`)
* Fix incorrect query results for certain complex queries. ({issue}`11083`)

## ClickHouse connector

* Add support for `uint8`, `uint16`, `uint32` and `uint64` types. ({issue}`11490`)

## Delta Lake connector

* Allow specifying STS endpoint to be used when connecting to S3. ({issue}`10169`)
* Fix query failures due to exhausted file system resources after `DELETE` or
  `UPDATE`. ({issue}`11418`)

## Hive connector

* Allow specifying STS endpoint to be used when connecting to S3. ({issue}`10169`)
* Fix shared metadata caching with Hive ACID tables. ({issue}`11443`)

## Iceberg connector

* Allow specifying STS endpoint to be used when connecting to S3. ({issue}`10169`)
* Add support for using Glue metastore as Iceberg catalog. ({issue}`10845`)

## MongoDB connector

* Add support for [`CREATE SCHEMA`](/sql/create-schema) and
  [`DROP SCHEMA`](/sql/drop-schema). ({issue}`11409`)
* Add support for [`COMMENT ON TABLE`](/sql/comment). ({issue}`11424`)
* Add support for [`COMMENT ON COLUMN`](/sql/comment). ({issue}`11457`)
* Support storing a comment when adding new columns. ({issue}`11487`)

## PostgreSQL connector

* Improve performance of queries involving `OR` with simple comparisons and
  `LIKE` predicates by pushing predicate computation to the PostgreSQL database.
  ({issue}`11086`)
* Improve performance of aggregation queries with certain complex predicates by
  computing predicates and aggregations within PostgreSQL. ({issue}`11083`)
* Fix possible connection leak when connecting to PostgreSQL failed. ({issue}`11449`)

## SingleStore (MemSQL) connector

* The connector now uses the official Single Store JDBC Driver. As a result,
  `connection-url` in catalog configuration files needs to be updated from
  `jdbc:mariadb:...` to `jdbc:singlestore:...`. ({issue}`10669`)
* Deprecate `memsql` as the connector name. We recommend using `singlestore` in
  the `connector.name` configuration property. ({issue}`11459`)
