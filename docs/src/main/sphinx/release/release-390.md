# Release 390 (13 Jul 2022)

## General

* Update minimum required Java version to 17.0.3. ({issue}`13014`)
* Add support for [setting comments on views](/sql/comment). ({issue}`8349`)
* Improve performance of queries with an `UNNEST` clause. ({issue}`10506`)
* Fix potential query failure when spilling to disk is enabled by the
  `force-spilling-join-operator` configuration property or the
  `force_spilling_join` session property. ({issue}`13123`)
* Fix incorrect results for certain join queries containing filters involving
  explicit or implicit casts. ({issue}`13145 `)

## Cassandra connector

* Change mapping for Cassandra `inet` type to Trino `ipaddress` type.
  Previously, `inet` was mapped to `varchar`. ({issue}`851`)
* Remove support for the
  `cassandra.load-policy.use-token-aware`,
  `cassandra.load-policy.shuffle-replicas`, and
  `cassandra.load-policy.allowed-addresses` configuration properties. ({issue}`12223`)

## Delta Lake connector

* Add support for filtering splits based on `$path` column predicates. ({issue}`13169`)
* Add support for Databricks runtime 10.4 LTS. ({issue}`13081`)
* Expose AWS Glue metastore statistics via JMX. ({issue}`13087`)
* Fix failure when using the Glue metastore and queries contain `IS NULL` or
  `IS NOT NULL` filters on numeric partition columns. ({issue}`13124`)

## Hive connector

* Expose AWS Glue metastore statistics via JMX. ({issue}`13087`)
* Add support for [setting comments on views](/sql/comment). ({issue}`13147`)
* Fix failure when using the Glue metastore and queries contain `IS NULL` or
  `IS NOT NULL` filters on numeric partition columns. ({issue}`13124`)
* Fix and re-enable usage of Amazon S3 Select for uncompressed files. ({issue}`12633`)

## Iceberg connector

* Add `added_rows_count`, `existing_rows_count`, and `deleted_rows_count`
  columns to the `$manifests` table. ({issue}`10809`)
* Add support for [setting comments on views](/sql/comment). ({issue}`13147`)
* Expose AWS Glue metastore statistics via JMX. ({issue}`13087`)
* Fix failure when using the Glue metastore and queries contain `IS NULL` or
  `IS NOT NULL` filters on numeric partition columns. ({issue}`13124`)

## Memory connector

* Add support for [setting comments on views](/sql/comment). ({issue}`8349`)

## Prometheus connector

* Fix failure when reading a table without specifying a `labels` column. ({issue}`12510`)
