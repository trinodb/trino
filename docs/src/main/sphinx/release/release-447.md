# Release 447 (8 May 2024)

## General

* Add support for [](/sql/show-create-function). ({issue}`21809`)
* Add support for the {func}`bitwise_xor_agg` aggregation function. ({issue}`21436`)
* {{breaking}} Require JDK 22 to run Trino. ({issue}`20980`)
* Improve performance of `ORDER BY` queries with `LIMIT` on large data sets. ({issue}`21761`)
* Improve performance of queries containing the {func}`rank` or
  {func}`row_number` window functions. ({issue}`21639`)
* Improve performance of correlated queries with `EXISTS`. ({issue}`21422`)
* Fix potential failure for expressions involving `try_cast(parse_json(...))`. ({issue}`21877`)

## CLI

* Fix incorrect error location markers for SQL routines causing CLI to print
  exceptions. ({issue}`21357`)

## Delta Lake connector

* Add support for concurrent `DELETE` and `TRUNCATE` queries. ({issue}`18521`)
* Fix under-accounting of memory usage when writing strings to Parquet files. ({issue}`21745`)

## Hive connector

* Add support for metastore caching on tables that have not been analyzed, which
  can be enabled with the `hive.metastore-cache.cache-missing-stats` and
  `hive.metastore-cache.cache-missing-partitions` configuration properties. ({issue}`21822`)
* Fix under-accounting of memory usage when writing strings to Parquet files. ({issue}`21745`)
* Fix failure when translating Hive views that contain `EXISTS` clauses. ({issue}`21829`)

## Hudi connector

* Fix under-accounting of memory usage when writing strings to Parquet files. ({issue}`21745`)

## Iceberg connector

* Fix under-accounting of memory usage when writing strings to Parquet files. ({issue}`21745`)

## Phoenix connector

* {{breaking}} Remove support for Phoenix versions 5.1.x and earlier. ({issue}`21569`)

## Pinot connector

* Add support for specifying an explicit broker URL with the `pinot.broker-url`
  configuration property. ({issue}`17791`)

## Redshift connector

* {{breaking}} Remove deprecated legacy type mapping and the associated
  `redshift.use-legacy-type-mapping` configuration property. ({issue}`21855`)
