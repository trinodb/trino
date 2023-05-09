# Release 414 (19 Apr 2023)

## General

* Add [recursive member access](json-descendant-member-accessor) to the
  [JSON path language](json-path-language). ({issue}`16854`)
* Add the [`sequence()`](built-in-table-functions) table function. ({issue}`16716`)
* Add support for progress estimates when
  [fault-tolerant execution](/admin/fault-tolerant-execution) is enabled. ({issue}`13072`)
* Add support for `CUBE` and `ROLLUP` with composite sets. ({issue}`16981`)
* Add experimental support for tracing using [OpenTelemetry](https://opentelemetry.io/).
  This can be enabled by setting the `tracing.enabled` configuration property to
  `true` and optionally configuring the
  [OLTP/gRPC endpoint](https://opentelemetry.io/docs/reference/specification/protocol/otlp/)
  by setting the `tracing.exporter.endpoint` configuration property. ({issue}`16950`)
* Improve performance for certain queries that produce no values. ({issue}`15555`, {issue}`16515`)
* Fix query failure for recursive queries involving lambda expressions. ({issue}`16989`)
* Fix incorrect results when using the {func}`sequence` function with values
  greater than 2<sup>31</sup> (about 2.1 billion). ({issue}`16742`)

## Security

* Disallow [graceful shutdown](/admin/graceful-shutdown) with the `default`
  [system access control](/security/built-in-system-access-control). Shutdowns
  can be re-enabled by using the `allow-all` system access control, or by
  configuring [system information rules](system-file-auth-system-information)
  with the `file` system access control. ({issue}`17105`)

## Delta Lake connector

* Add support for `INSERT`, `UPDATE`, and `DELETE` operations on
  tables with a `name` column mapping. ({issue}`12638`)
* Add support for [Databricks 12.2 LTS](https://docs.databricks.com/release-notes/runtime/12.2.html). ({issue}`16905`)
* Disallow reading tables with [deletion vectors](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#deletion-vectors).
  Previously, this returned incorrect results. ({issue}`16884`)

## Iceberg connector

* Add support for Hive external tables in the `migrate` table procedure. ({issue}`16704`)

## Kafka connector

* Fix query failure when a Kafka topic contains tombstones (messages with a
  ``NULL`` value). ({issue}`16962`)

## Kudu connector

* Fix query failure when merging two tables that were created by
  `CREATE TABLE ... AS SELECT ...`. ({issue}`16848`)

## Pinot connector

* Fix incorrect results due to incorrect pushdown of aggregations. ({issue}`12655`)

## PostgreSQL connector

* Fix failure when fetching table statistics for PostgreSQL 14.0 and later. ({issue}`17061`)

## Redshift connector

* Add support for [fault-tolerant execution](/admin/fault-tolerant-execution). ({issue}`16860`)
