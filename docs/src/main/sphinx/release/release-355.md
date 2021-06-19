# Release 355 (8 Apr 2021)

## General

* Report tables that are directly referenced by a query in `QueryCompletedEvent`. ({issue}`7330`)
* Report columns that are the target of `INSERT` or `UPDATE` queries in `QueryCompletedEvent`. This includes
  information about which input columns they are derived from. ({issue}`7425`, {issue}`7465`)  
* Rename `optimizer.plan-with-table-node-partitioning` config property to `optimizer.use-table-scan-node-partitioning`. ({issue}`7257`)
* Improve query parallelism when table bucket count is small compared to number of nodes. 
  This optimization is now triggered automatically when the ratio between table buckets and 
  possible table scan tasks exceeds or is equal to `optimizer.table-scan-node-partitioning-min-bucket-to-task-ratio`. ({issue}`7257`)
* Include information about {doc}`/admin/spill` in {doc}`/sql/explain-analyze`. ({issue}`7427`)
* Disallow inserting data into tables that have row filters. ({issue}`7346`)
* Improve performance of queries that can benefit from both {doc}`/optimizer/cost-based-optimizations` and join pushdown
  by giving precedence to cost-based optimizations. ({issue}`7331`)
* Fix inconsistent behavior for {func}`to_unixtime` with values of type `timestamp(p)`. ({issue}`7450`)
* Change return type of {func}`from_unixtime` and {func}`from_unixtime_nanos` to `timestamp(p) with time zone`. ({issue}`7460`)

## Security

* Add support for configuring multiple password authentication plugins. ({issue}`7151`)

## JDBC driver

* Add `assumeLiteralNamesInMetadataCallsForNonConformingClients` parameter for use as a workaround when
  applications do not properly escape schema or table names in calls to `DatabaseMetaData` methods. ({issue}`7438`)

## ClickHouse connector

* Support creating tables with MergeTree storage engine. ({issue}`7135`)

## Hive connector

* Support Hive views containing `LATERAL VIEW json_tuple(...) AS ...` syntax. ({issue}`7242`)
* Fix incorrect results when reading from a Hive view that uses array subscript operators. ({issue}`7271`)
* Fix incorrect results when querying the `$file_modified_time` hidden column. ({issue}`7511`)

## Phoenix connector

* Improve performance when fetching table metadata during query analysis. ({issue}`6975`)
* Improve performance of queries with `ORDER BY ... LIMIT` clause when the computation
  can be pushed down to the underlying database. ({issue}`7490`)

## SQL Server connector

* Improve performance when fetching table metadata during query analysis. ({issue}`6975`)

## SPI

* Engine now uses `ConnectorMaterializedViewDefinition#storageTable`
  to determine materialized view storage table. ({issue}`7319`)

