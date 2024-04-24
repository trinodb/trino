# Release 436 (11 Jan 2024)

## General

* {{breaking}} Require JDK 21.0.1 to run Trino. ({issue}`20010`)
* Improve performance by not generating redundant predicates. ({issue}`16520`)
* Fix query failure when invoking the `json_table` function. ({issue}`20122`)
* Fix query hang when a [SQL routine](/routines) dereferences a row field. ({issue}`19997`).
* Fix potential incorrect results when using the {func}`ST_Centroid` and
  {func}`ST_Buffer` functions for tiny geometries. ({issue}`20237`)

## Delta Lake connector

* Add support for querying files with corrupt or incorrect statistics, which can
  be enabled with the `parquet_ignore_statistics` catalog session property. ({issue}`20228`)
* Improve performance of queries with selective joins on partition columns. ({issue}`20261`)
* Reduce the number of requests made to AWS Glue when listing tables, schemas,
  or functions. ({issue}`20189`)
* Fix incorrect results when querying Parquet files containing column indexes
  when the query has filters on multiple columns. ({issue}`20267`)

## ElasticSearch connector

* {{breaking}} Add support for ElasticSearch
  [version 8](https://www.elastic.co/guide/en/elasticsearch/reference/current/es-release-notes.html),
  and remove support for ElasticSearch version 6. ({issue}`20258`)
* Add [OpenSearch connector](/connector/opensearch). ({issue}`11377`)

## Hive connector

* Reduce the number of requests made to AWS Glue when listing tables, schemas,
  or functions. ({issue}`20189`)
* Fix failure when reading certain Avro data with Union data types. ({issue}`20233`)
* Fix incorrect results when querying Parquet files containing column indexes
  when the query has filters on multiple columns. ({issue}`20267`)

## Hudi connector

* Add support for enforcing that a filter on a partition key must be present in
  the query. This can be enabled by with the
  ``hudi.query-partition-filter-required`` configuration property or the
  ``query_partition_filter_required`` catalog session property. ({issue}`19906`)
* Fix incorrect results when querying Parquet files containing column indexes
  when the query has filters on multiple columns. ({issue}`20267`)

## Iceberg connector

* Add support for querying files with corrupt or incorrect statistics, which can
  be enabled with the `parquet_ignore_statistics` catalog session property. ({issue}`20228`)
* Improve performance of queries with selective joins on partition columns. ({issue}`20212`)
* Reduce the number of requests made to AWS Glue when listing tables, schemas,
  or functions. ({issue}`20189`)
* Fix potential loss of data when running multiple `INSERT` queries at the same
  time. ({issue}`20092`)
* Fix incorrect results when providing a nonexistent namespace while listing
  namespaces. ({issue}`19980`)
* Fix predicate pushdown not running for Parquet files when columns have been
  renamed. ({issue}`18855`)

## SQL Server connector

* Fix incorrect results for `DATETIMEOFFSET` values before the year 1400. ({issue}`16559`)
