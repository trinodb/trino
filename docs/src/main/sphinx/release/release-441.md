# Release 441 (13 Mar 2024)

## General

* Fix incorrect results of window aggregations when any input data includes 
  `NaN` or infinity. ({issue}`20946`)
* Fix `NoSuchMethodError` in filtered aggregations. ({issue}`21002`)

## Cassandra connector

* Fix incorrect results when a query contains predicates on clustering columns. ({issue}`20963`)

## Hive connector

* {{breaking}} Remove the default `legacy` mode for the `hive.security`
  configuration property, and change the default value to `allow-all`.
  Additionally, remove the legacy properties `hive.allow-drop-table`,
  `hive.allow-rename-table`, `hive.allow-add-column`, `hive.allow-drop-column`,
  `hive.allow-rename-column`, `hive.allow-comment-table`, and
  `hive.allow-comment-column`. ({issue}`21013`)
* Fix query failure when reading array types from Parquet files produced by some
  legacy writers. ({issue}`20943`)

## Hudi connector

* Disallow creating files on non-existent partitions. ({issue}`20133`)
