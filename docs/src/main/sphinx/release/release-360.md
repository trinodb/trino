# Release 360 (30 Jul 2021)

## General

* Improve support for correlated subqueries with `GROUP BY` or `LIMIT` and
  complex correlated filter conditions. ({issue}`8554`)
* Report cumulative query system memory usage. ({issue}`8615`)
* Fix `DROP SCHEMA` and `DROP SCHEMA RESTRICT` not to drop then schema if it is not empty. ({issue}`8660`)
* Fix query failure when there is a constant predicate on some
  column `col` (e.g `col=1`), followed by  `ORDER BY col` and `LIMIT`. ({issue}`8535`)
* Fix `SHOW CREATE SCHEMA` failure. ({issue}`8598`)
* Fix query failure when running `SHOW CREATE SCHEMA information_schema`. ({issue}`8600`)
* Improve performance of `WHERE` clause evaluation. ({issue}`8624`)
* Reduce coordinator network load. ({issue}`8460`)
* Improve query performance by sending collected dynamic filters from coordinator to workers. ({issue}`5183`)
* Improve performance of inequality joins where join condition sides have different type. ({issue}`8500`)
* Improve performance of `IN (<subquery>)` expressions. ({issue}`8639`)

## Security

* Add support for automatic configuration of TLS for {doc}`/security/internal-communication`. This removes
  the need to provision per-worker TLS certificates. ({issue}`7954`)

## CLI

* Fix auto completion when pressing the tab button. ({issue}`8529`)

## ClickHouse connector

* Add `write.batch-size` connector configuration property to control JDBC batch size used during writes.
  It can also be controlled using the `write_batch_size` session property. ({issue}`8434`)
* Add new configuration property `insert.non-transactional-insert.enabled` to allow INSERT queries to write directly
  to the target table. This can improve performance in some cases by sacrificing transactional behaviour. It can also
  be controlled using `non_transactional_insert` session property. ({issue}`8496`)

## Elasticsearch connector

* Add support for assuming an IAM role. ({issue}`8714`)

## Hive connector

* Fix data corruption when performing `UPDATE` or `INSERT` on ORC ACID transactional table. ({issue}`8268`, {issue}`8452`) 

## Iceberg connector

* Add support for Trino views. ({issue}`8540`)
* Prevent incorrect query results by failing a query when Iceberg table has row-level deletes. ({issue}`8450`)
* Fix query failure when joining with a bucketed Iceberg table. ({issue}`7502`)
* Fix query failure when showing stats for a bucketed Iceberg table. ({issue}`8616`)
* Fix query failure when joining with a partitioned table that has structural columns (`array`, `map` or `row`). ({issue}`8647`)
* Fix failures for queries that write tables in Parquet format. ({issue}`5201`)
* Improve query planning time by reducing calls to the metastore. ({issue}`8676`, {issue}`8689`)

## MemSQL connector

* Add `write.batch-size` connector configuration property to control JDBC batch size used during writes.
  It can also be controlled using the `write_batch_size` session property. ({issue}`8434`)
* Add new configuration property `insert.non-transactional-insert.enabled` to allow INSERT queries to write directly
  to the target table. This can improve performance in some cases by sacrificing transactional behaviour. It can also
  be controlled using `non_transactional_insert` session property. ({issue}`8496`)
* Partial support for `DELETE` statement where predicate can be fully pushed down to the remote datasource. ({issue}`6287`)
* Fix performance regression of writes. ({issue}`8559`)

## MongoDB connector

* Add support for `json` type. ({issue}`8352`)
* Support reading MongoDB `DBRef` type. ({issue}`3134`)

## MySQL connector

* Add `write.batch-size` connector configuration property to control JDBC batch size used during writes.
  It can also be controlled using the `write_batch_size` session property. ({issue}`8434`)
* Add new configuration property `insert.non-transactional-insert.enabled` to allow INSERT queries to write directly
  to the target table. This can improve performance in some cases by sacrificing transactional behaviour. It can also
  be controlled using `non_transactional_insert` session property. ({issue}`8496`)
* Partial support for `DELETE` statement where predicate can be fully pushed down to the remote datasource. ({issue}`6287`)
* Fix performance regression of writes. ({issue}`8559`)

## Oracle connector

* Add `write.batch-size` connector configuration property to control JDBC batch size used during writes.
  It can also be controlled using the `write_batch_size` session property. ({issue}`8434`)
* Add new configuration property `insert.non-transactional-insert.enabled` to allow INSERT queries to write directly
  to the target table. This can improve performance in some cases by sacrificing transactional behaviour. It can also
  be controlled using `non_transactional_insert` session property. ({issue}`8496`)
* Partial support for `DELETE` statement where predicate can be fully pushed down to the remote datasource. ({issue}`6287`)

## Phoenix connector

* Add `write.batch-size` connector configuration property to control JDBC batch size used during writes.
  It can also be controlled using the `write_batch_size` session property. ({issue}`8434`)
* Add new configuration property `insert.non-transactional-insert.enabled` to allow INSERT queries to write directly
  to the target table. This can improve performance in some cases by sacrificing transactional behaviour. It can also
  be controlled using `non_transactional_insert` session property. ({issue}`8496`)
* Improve performance of `ORDER BY ... LIMIT` queries on sorted data for Phoenix 5. ({issue}`8171`)
* Partial support for `DELETE` statement where predicate can be fully pushed down to the remote datasource. ({issue}`6287`)
* Fix performance regression of writes. ({issue}`8559`)

## PostgreSQL connector

* Add `write.batch-size` connector configuration property to control JDBC batch size used during writes.
  It can also be controlled using the `write_batch_size` session property. ({issue}`8434`)
* Add new configuration property `insert.non-transactional-insert.enabled` to allow INSERT queries to write directly
  to the target table. This can improve performance in some cases by sacrificing transactional behaviour. It can also
  be controlled using `non_transactional_insert` session property. ({issue}`8496`)
* Partial support for `DELETE` statement where predicate can be fully pushed down to the remote datasource. ({issue}`6287`)

## Redshift connector

* Add `write.batch-size` connector configuration property to control JDBC batch size used during writes.
  It can also be controlled using the `write_batch_size` session property. ({issue}`8434`)
* Add new configuration property `insert.non-transactional-insert.enabled` to allow INSERT queries to write directly
  to the target table. This can improve performance in some cases by sacrificing transactional behaviour. It can also
  be controlled using `non_transactional_insert` session property. ({issue}`8496`)
* Partial support for `DELETE` statement where predicate can be fully pushed down to the remote datasource. ({issue}`6287`)

## SQL Server connector

* Add `write.batch-size` connector configuration property to control JDBC batch size used during writes.
  It can also be controlled using the `write_batch_size` session property. ({issue}`8434`)
* Add new configuration property `insert.non-transactional-insert.enabled` to allow INSERT queries to write directly
  to the target table. This can improve performance in some cases by sacrificing transactional behaviour. It can also
  be controlled using `non_transactional_insert` session property. ({issue}`8496`)
* Partial support for `DELETE` statement where predicate can be fully pushed down to the remote datasource. ({issue}`6287`)
* Fix performance regression of writes. ({issue}`8559`)

## SPI

* Cast materialized view storage table columns to match view column types. ({issue}`8408`)
* Remove deprecated `ConnectorSplitManager#getSplits` method overrides. ({issue}`8569`)
* Introduce `ConnectorPageSource#getCompletedPositions` for tracing physically read positions. ({issue}`8524`)
* Remove deprecated `TupleDomain.transform`. ({issue}`8056`)
