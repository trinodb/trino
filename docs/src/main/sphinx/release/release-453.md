# Release 453 (25 Jul 2024)

## General

* Improve accuracy of the {func}`cosine_distance` function. ({issue}`22761`)
* Improve performance of non-equality joins. ({issue}`22521`)
* Improve performance for column masking with [](/security/opa-access-control). ({issue}`21359`)
* Fix incorrect evaluation of repeated non-deterministic functions. ({issue}`22683`)
* Fix potential failure for queries involving `GROUP BY`, `UNNEST`, and filters
  over expressions that may produce an error for certain inputs. ({issue}`22731`)
* Fix planning failure for queries with a filter on an aggregation. ({issue}`22716`)
* Fix planning failure for queries involving multiple aggregations and `CASE`
  expressions. ({issue}`22806`)
* Fix optimizer timeout for certain queries involving aggregations and `CASE`
  expressions. ({issue}`22813`)

## Security

* Add support for `IF EXISTS` to `DROP ROLE`. ({issue}`21985`)

## JDBC driver

* Add support for using certificates from the operating system keystore. ({issue}`22341`)
* Add support for setting the default [SQL PATH](/sql/set-path). ({issue}`22703`)
* Allow Trino host URI specification without port for the default ports 80 for
  HTTP and 443 for HTTPS. ({issue}`22724`)

## CLI

* Add support for using certificates from the operating system keystore. ({issue}`22341`)
* Add support for setting the default [SQL PATH](/sql/set-path). ({issue}`22703`)
* Allow Trino host URI specification without port for the default ports 80 for
  HTTP and 443 for HTTPS. ({issue}`22724`)

## BigQuery connector

* Improve performance when querying information schema. ({issue}`22770`)

## Cassandra connector

* Add support for the `execute` procedure. ({issue}`22556`)

## ClickHouse connector

* Add support for the `execute` procedure. ({issue}`22556`)

## Delta Lake connector

* Add support for reading partition columns whose type changed via [type
  widening](https://docs.delta.io/latest/delta-type-widening.html). ({issue}`22433`)
* Add support for authenticating with Glue with a Kubernetes service account.
  This can be enabled via the
  `hive.metastore.glue.use-web-identity-token-credentials-provider`
  configuration property. ({issue}`15267`)
* Fix failure when executing the [VACUUM](delta-lake-vacuum) procedure on tables
  without old transaction logs. ({issue}`22816`)

## Druid connector

* Add support for the `execute` procedure. ({issue}`22556`)

## Exasol connector

* Add support for the `execute` procedure. ({issue}`22556`)

## Hive connector

* Add support for authenticating with Glue with a Kubernetes service account.
  This can be enabled via the
  `hive.metastore.glue.use-web-identity-token-credentials-provider`
  configuration property. ({issue}`15267`)
* Fix failure to read Hive tables migrated to Iceberg with Apache Spark. ({issue}`11338`)
* Fix failure for `CREATE FUNCTION` with SQL routine storage in Glue when
  `hive.metastore.glue.catalogid` is set. ({issue}`22717`)

## Hudi connector

* Add support for authenticating with Glue with a Kubernetes service account.
  This can be enabled via the
  `hive.metastore.glue.use-web-identity-token-credentials-provider`
  configuration property. ({issue}`15267`)

## Iceberg connector

* {{breaking}} Change the schema version for the JDBC catalog database to `V1`.
  The previous value can be restored by setting the
  `iceberg.jdbc-catalog.schema-version` configuration property to `V0`. ({issue}`22576`)
* Add support for views with the JDBC catalog. Requires an upgrade
  of the schema for the JDBC catalog database to `V1`. ({issue}`22576`)
* Add support for specifying on which schemas to enforce the presence of a
  partition filter in queries. This can be configured
  `query-partition-filter-required-schemas` property. ({issue}`22540`)
* Add support for authenticating with Glue with a Kubernetes service account.
  This can be enabled via the
  `hive.metastore.glue.use-web-identity-token-credentials-provider`
  configuration property. ({issue}`15267`)
* Fix failure when executing `DROP SCHEMA ... CASCADE` using the REST catalog
  with Iceberg views. ({issue}`22758`)

## Ignite connector

* Add support for the `execute` procedure. ({issue}`22556`)

## MariaDB connector

* Add support for the `execute` procedure. ({issue}`22556`)

## MySQL connector

* Add support for the `execute` procedure. ({issue}`22556`)

## Oracle connector

* Add support for the `execute` procedure. ({issue}`22556`)

## Phoenix connector

* Add support for the `execute` procedure. ({issue}`22556`)

## PostgreSQL connector

* Add support for reading the `vector` type on
  [pgvector](https://github.com/pgvector/pgvector/). ({issue}`22630`)
* Add support for the `execute` procedure. ({issue}`22556`)

## Redshift connector

* Add support for the `execute` procedure. ({issue}`22556`)

## SingleStore connector

* Add support for the `execute` procedure. ({issue}`22556`)

## Snowflake connector

* Add support for the `execute` procedure. ({issue}`22556`)

## SQL Server connector

* Add support for the `execute` procedure. ({issue}`22556`)

## SPI

* Add `SystemAccessControl.getColumnMasks` as replacement for the deprecated
  `SystemAccessControl.getColumnMask`. ({issue}`21997`)