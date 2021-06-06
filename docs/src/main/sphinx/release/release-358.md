# Release 358 (1 Jun 2021)

## General

* Support arbitrary queries in {doc}`/sql/show-stats`. ({issue}`8026`)
* Improve performance of complex queries involving joins and `TABLESAMPLE`. ({issue}`8094`)
* Improve performance of `ORDER BY ... LIMIT` queries on sorted data. ({issue}`6634`)
* Reduce graceful shutdown time for worker nodes. ({issue}`8149`)
* Fix query failure columns of non-orderable types (e.g. `HyperLogLog`, `tdigest`, etc.), are involved in a join. ({issue}`7723`)
* Fix failure for queries containing repeated ordinals in a `GROUP BY` clause.
  Example: `SELECT x FROM t GROUP BY 1, 1`. ({issue}`8023`)
* Fix failure for queries containing repeated expressions in the `ORDER BY` clause of an aggregate function.
  Example: `SELECT array_agg(x ORDER BY y, y) FROM (VALUES ('a', 2)) t(x, y)`. ({issue}`8080`)

## JDBC Driver

* Remove legacy JDBC URL prefix `jdbc:presto:`. ({issue}`8042`)
* Remove legacy driver classes `io.prestosql.jdbc.PrestoDriver`
  and `com.facebook.presto.jdbc.PrestoDriver`. ({issue}`8042`)

## Hive connector

* Add support for reading from Hive views that use `LATERAL VIEW EXPLODE`
  or `LATERAL VIEW OUTER EXPLODE` over array of `STRUCT`. ({issue}`8120`)
* Improve performance of `ORDER BY ... LIMIT` queries on sorted data. ({issue}`6634`)

## Iceberg connector

* Fix failure when listing materialized views in `information_schema.tables` or via the 
  `java.sql.DatabaseMetaData.getTables()` JDBC API. ({issue}`8151`)

## Memory connector

* Improve performance of certain complex queries involving joins. ({issue}`8095`)

## SPI

* Remove deprecated `ConnectorPageSourceProvider.createPageSource()` method overrides. ({issue}`8077`)
* Add support for casting the columns of a redirected table scan when source column types don't match. ({issue}`6066`)
* Add `ConnectorMetadata.redirectTable()` to allow connectors to redirect table reads and metadata listings. ({issue}`7606`)
* Add `ConnectorMetadata.streamTableColumns()` for streaming column metadata in a redirection-aware manner. The
  alternate method for column listing `ConnectorMetadata.listTableColumns()` is now deprecated. ({issue}`7606`)

