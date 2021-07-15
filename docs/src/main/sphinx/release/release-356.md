# Release 356 (30 Apr 2021)

## General

* Add support for {doc}`/sql/match-recognize`. ({issue}`6111`)
* Add {func}`soundex` function. ({issue}`4022`)  
* Introduce `system.metadata.materialized_view_properties` table for listing available materialized view properties. ({issue}`7615`)
* Add support for limiting the maximum planning time via the `query.max-planning-time` configuration property. ({issue}`7213`)
* Allow redirecting clients to an alternative location to fetch query information. This can be configured 
  via the `query.info-url-template` configuration property. ({issue}`7678`)
* Allow cancellation of queries during planning phase. ({issue}`7213`)
* Improve performance of `ORDER BY ... LIMIT` queries over a `LEFT JOIN`. ({issue}`7028`)
* Improve performance of queries with predicates on boolean columns. ({issue}`7263`)
* Improve planning time for queries with large `IN` predicates. ({issue}`7556`)
* Improve performance of queries that contain joins on `varchar` keys of different length. ({issue}`7644`)
* Improve performance of queries when late materialization is enabled. ({issue}`7695`)
* Reduce coordinator network overhead when scheduling queries. ({issue}`7351`)
* Fix possible deadlock for `JOIN` queries when spilling is enabled. ({issue}`7455`)
* Fix incorrect results for queries containing full outer join with an input that is known to produce one row. ({issue}`7629`)
* Fix failure when quantified comparison expressions contain scalar subqueries. ({issue}`7792`)
  
## Security

* Materialized views require `UPDATE` privilege to be refreshed. ({issue}`7707`)
* Add dedicated access control for creating and dropping materialized views. ({issue}`7645`)
* Add dedicated access control for refreshing materialized views. Insert privilege on storage table is no
  longer required. ({issue}`7707`)
* Fix authentication failure when providing multiple scope values for `http-server.authentication.oauth2.scopes`. ({issue}`7706`)

## JDBC driver

* Add support for caching OAuth2 credentials in memory to avoid unnecessary authentication flows. ({issue}`7309`)

## BigQuery connector

* Add support for `CREATE SCHEMA` and `DROP SCHEMA` statements. ({issue}`7543`)
* Improve table listing performance when case insensitive matching is enabled. ({issue}`7628`)

## Cassandra connector

* Fix `NullPointerException` when reading an empty timestamp value. ({issue}`7433`)

## Hive connector

* Improve performance when reading dictionary-encoded Parquet files. ({issue}`7754`)
* Fix incorrect results when referencing nested fields with non-lowercase names from ORC files. ({issue}`7350`)
* Always use row-by-row deletes for ACID tables rather than dropping partitions. ({issue}`7621`)
* Allow reading from ORC ACID transactional tables when `_orc_acid_version` metadata files are missing. ({issue}`7579`)
* Add `UPDATE` support for ACID tables that were originally created as non-transactional. ({issue}`7622`)
* Add support for connection proxying for Azure ADLS endpoints. ({issue}`7509`)

## Iceberg connector

* Show Iceberg tables created by other engines in `SHOW TABLES` output. ({issue}`1592`)
* Improve performance when reading dictionary-encoded Parquet files. ({issue}`7754`)
* Improve query planning through table metadata caching. ({issue}`7336`)
* Fix failure querying materialized views that were created using the session catalog and schema. ({issue}`7711`)
* Fix listing of materialized views when using `SHOW TABLES` query. ({issue}`7790`)

## Kafka connector

* Add support for TLS security protocol. ({issue}`6929`)

## MemSQL connector

* Improve metadata caching hit rate. ({issue}`7039`)

## MongoDB connector

* Fix handling of non-lowercase MongoDB views. ({issue}`7491`)

## MySQL connector

* Improve metadata caching hit rate. ({issue}`7039`)
* Exclude an internal `sys` schema from schema listings. ({issue}`6337`)

## Oracle connector

* Improve metadata caching hit rate. ({issue}`7039`)

## Phoenix connector

* Improve metadata caching hit rate. ({issue}`7039`)

## PostgreSQL connector

* Improve metadata caching hit rate. ({issue}`7039`)
* Cancel query on PostgreSQL when the Trino query is cancelled. ({issue}`7306`)
* Discontinue support for PostgreSQL 9.5, which has reached end of life. ({issue}`7676`)

## Redshift connector

* Improve metadata caching hit rate. ({issue}`7039`)

## SQL Server connector

* Improve metadata caching hit rate. ({issue}`7039`)
* Fix query failure when snapshot isolation is disabled in target SQL Server database,
  but `READ_COMMITTED_SNAPSHOT` is still enabled. ({issue}`7548`)
* Fix reading `date` values before 1583-10-14. ({issue}`7634`)

## SPI

* Require that `ConnectorMaterializedViewDefinition` provides a view owner. ({issue}`7489`)
* Add `Connector#getMaterializedViewPropertyManager` for specifying materialized view properties. ({issue}`7615`)
* Add `ConnectorAccessControl.checkCanCreateMaterializedView()` and
  `ConnectorAccessControl.checkCanDropMaterializedView()` for authorizing
  creation and removal of materialized views. ({issue}`7645`)
* Allow a materialized view to return a storage table in a different catalog or schema. ({issue}`7638`)
* Add `ConnectorAccessControl.checkCanRefreshMaterializedView()` for authorizing
  refresh of materialized views. ({issue}`7707`)
