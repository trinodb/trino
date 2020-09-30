# Release 343 (25 Sep 2020)

## BigQuery Connector Changes

* Add support for yearly partitioned tables. ({issue}`5298`)

## Hive Connector Changes

* Fix query failure when read from or writing to a bucketed table containing a column of `timestamp` type. ({issue}`5295`)

## SQL Server Connector Changes

* Improve performance of aggregation queries with `stddev`, `stddev_samp`, `stddev_pop`, `variance`, `var_samp`, `var_pop`
  aggregate functions by computing aggregations within SQL Server database. ({issue}`5299`)
