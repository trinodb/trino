# Release 368 (11 Jan 2022)

## General

* Allow setting per task memory limits via `query.max-total-memory-per-task`
  config property or via `query_max_total_memory_per_task` session property.
  ({issue}`10308`)
* Improve wall time for query processing with the `phased` scheduling policy.
  The previous behavior can be restored by setting the `query.execution-policy`
  configuration property to `legacy-phased`. ({issue}`10350`)
* Enable `phased` scheduling policy by default. The previous behavior can be
  restored by setting the `query.execution-policy` configuration property to
  `all-at-once`. ({issue}`10455`)
* Improve performance of arithmetic operations involving decimals with precision
  larger than 18. ({issue}`10051`)
* Reduce risk of out-of-memory failure on congested clusters with high memory
  usage. ({issue}`10475`)
* Fix queries not being unblocked when placed in reserved memory pool.
  ({issue}`10475`)
* Prevent execution of `REFRESH MATERIALIZED VIEW` from getting stuck.
  ({issue}`10360`)
* Fix double reporting of scheduled time for scan operators in
  `EXPLAIN ANALYZE`. ({issue}`10472`)
* Fix issue where the length of log file names grow indefinitely upon log
  rotation. ({issue}`10394`)

## Hive connector

* Improve performance of decoding decimal values with precision larger than 18
  in ORC, Parquet and RCFile data. ({issue}`10051`)
* Disallow querying the properties system table for Delta Lake tables, since
  Delta Lake tables are not supported. This fixes the previous behavior of
  silently returning incorrect values. ({issue}`10447`)
* Reduce risk of worker out-of-memory exception when scanning ORC files.
  ({issue}`9949`)

## Iceberg connector

* Fix Iceberg table creation with location when schema location inaccessible.
  ({issue}`9732`)
* Support file based access control. ({issue}`10493`)
* Display the Iceberg table location in `SHOW CREATE TABLE` output.
  ({issue}`10459`)

## SingleStore (MemSQL) connector

* Add support for `time` type. ({issue}`10332`)

## Oracle connector

* Fix incorrect result when a `date` value is older than or equal to
  `1582-10-14`. ({issue}`10380`)

## Phoenix connector

* Add support for reading `binary` type. ({issue}`10539`)

## PostgreSQL connector

* Add support for accessing tables created with declarative partitioning in
  PostgreSQL. ({issue}`10400`)

## SPI

* Encode long decimal values using two's complement representation and change
  their carrier type to `io.trino.type.Int128` instead of
  `io.airlift.slice.Slice`. ({issue}`10051`)
* Fix `ClassNotFoundException` when using aggregation with a custom state type.
  ({issue}`10408`)
