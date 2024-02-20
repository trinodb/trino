# Release 430 (20 Oct 2023)

## General

* Improve performance of queries with `GROUP BY`. ({issue}`19302`)
* Fix incorrect results for queries involving `ORDER BY` and window functions
 with ordered frames. ({issue}`19399`)
* Fix incorrect results for query involving an aggregation in a correlated
  subquery. ({issue}`19002`)

## Security

* Enforce authorization capability of client when receiving commands `RESET` and
  `SET` for `SESSION AUTHORIZATION`. ({issue}`19217`)

## JDBC driver

* Add support for a `timezone` parameter to set the session timezone. ({issue}`19102`)

## Iceberg connector

* Add an option to require filters on partition columns. This can be enabled by
  setting the ``iceberg.query-partition-filter-required`` configuration property
  or the ``query_partition_filter_required`` session property. ({issue}`17263`)
* Improve performance when reading partition columns. ({issue}`19303`)

## Ignite connector

* Fix failure when a query contains `LIKE` with `ESCAPE`. ({issue}`19464`)

## MariaDB connector

* Add support for table statistics. ({issue}`19408`)

## MongoDB connector

* Fix incorrect results when a query contains several `<>` or `NOT IN`
  predicates. ({issue}`19404`)

## SPI

* Change the Java stack type for a `map` value to `SqlMap` and a `row` value to
  `SqlRow`, which do not implement `Block`. ({issue}`18948`)
