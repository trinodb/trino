# Release 448 (15 May 2024)

## General

* Fix query failure when reading columns with names that contain only
  non-alphanumeric characters. ({issue}`21833`)
* Fix potential incorrect results for queries with complex predicates. ({issue}`21887`)
* Fix potential loss of a query completion event when multiple queries fail at
  the same time. ({issue}`21896`)
* Fix startup failure when fault-tolerant execution is enabled with Google
  Cloud Storage exchange. ({issue}`21951`)
* Fix potential failure when queries contain `try_cast`. ({issue}`21952`)
* Fix graceful shutdown potentially hanging indefinitely when a worker node has
  crashed. ({issue}`18329`)

## Delta Lake connector

* Add support for caching Glue metadata. ({issue}`20657`)
* Update Glue to V2 REST interface. The old implementation can be temporarily
  restored by setting the `hive.metastore` configuration property to `glue-v1`. ({issue}`20657`)
* Improve performance of reading from Parquet files. ({issue}`21465`)

## Hive connector

* Add support for reading integers and timestamps in Parquet files as `DOUBLE`
  and `VARCHAR` columns, respectively, in Trino. ({issue}`21509`)
* Add support for caching Glue metadata. ({issue}`20657`)
* Update Glue to V2 REST interface. The old implementation can be temporarily
  restored by setting the `hive.metastore` configuration property to `glue-v1`. ({issue}`20657`)
* Improve performance of reading from Parquet files. ({issue}`21465`)
* Fix potential failure when reading ORC files larger than 2GB. ({issue}`21587`)

## Hudi connector

* Improve performance of reading from Parquet files. ({issue}`21465`)
* Fix potential failure when reading ORC files larger than 2GB. ({issue}`21587`)

## Iceberg connector

* Improve performance of reading from Parquet files. ({issue}`21465`)
* Fix potential failure when reading ORC files larger than 2GB. ({issue}`21587`)

## Phoenix connector

* Remove incorrect type mapping for `TIME` values. ({issue}`21879`)
