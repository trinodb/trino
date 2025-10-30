# Release 479 (TBD 2025)

## General

* Support automatic TLS certificate generation in the `ANNOUNCE` node discovery mode. ({issue}`27030`)
* Add support for specifying the location of the Exchange Manager configuration file. ({issue}`26611`)
* Publish query finishing time in event listener. ({issue}`27202`)
* Add lineage support for output columns in `SELECT` queries. ({issue}`26241`)
* Require JDK 25 to build and run Trino. ({issue}`27153`)
* Require the JDK Vector API to be enabled at runtime. ({issue}`27340`)
* Add support for `SET DEFAULT` and `DROP DEFAULT` statements. ({issue}`26162`)
* Add {func}`array_first` and {func}`array_last` functions. ({issue}`27295`)
* Allow field name declaration in ROW literal.  For example, `row(1 as a, 2 as b)` is now legal. ({issue}`25261`)
* Deprecate `EXPLAIN (TYPE LOGICAL)`. `EXPLAIN (TYPE DISTRIBUTED)` should be used instead. ({issue}`27434`)
* Improve performance of remote data exchanges on CPUs supporting the required SIMD extensions. This can be disabled by setting `exchange.experimental.vectorized-serde.enabled=false`. ({issue}`27426`, {issue}`26919`)
* Enhance after `RemoveTrivialFilters` call `RemoveEmptyUnionBranches` to prune empty union branches. ({issue}`21506`)
* Improve performance of {func}`array_sort` function. ({issue}`27272`)
* Improve performance of {func}`repeat` function. ({issue}`27369`)
* Improve performance of data exchanges involving variable width data. ({issue}`27377`)
* Fix planning failure of certain queries where part of the plan is optimized to empty values. ({issue}`21506`)
* Fix configuring partitioned layout for spooling protocol. ({issue}`27247`)
* Fix `EXPLAIN (TYPE IO)` failure when query constraint contains type which cannot be cast to `varchar`. ({issue}`27433`)

## Security

## Web UI

* Fix preview UI to render single line queries. ({issue}`27328`)

## JDBC driver

## Docker image

* Update JDK to 25.0.1. ({issue}`27117`)

## CLI

* Add a `--extra-header` argument to the trino-cli to support sending arbitrary HTTP headers to Trino. ({issue}`15826`)

## BigQuery connector

## Blackhole connector

## Cassandra connector

## ClickHouse connector

## Delta Lake connector

* Add `APPLICATION_DEFAULT` authentication type for GCS. ({issue}`26984`)
* Remove unauthentication access support in `SERVICE_ACCOUNT`. ({issue}`26984`)
* Rename `s3.exclusive-create` config to `delta.s3.transaction-log-conditional-writes.enabled`. ({issue}`27372`)
* Fix incorrect results due to incorrect pushdown of `IS NOT DISTINCT FROM`. ({issue}`27213`)
* Fix failure when writing to tables written by Databricks 17.3. ({issue}`27100`)
* Harden hierarchical namespace check in Azure with root blob fallback check. ({issue}`27278`)
* Avoid reading unusually large pages from parquet files. ({issue}`27148`)
* Fix potential `FileAlreadyExistsException` failure when network failures during s3 write. ({issue}`27330`)
* Prevent log writer from failing with FileAlreadyExistsException when there are network failures during write. 
  writing to s3 now requires permissions for PutObjectTagging and GetObjectTagging operations (breaking change). ({issue}`27388`)
* Fix potential failure when reading [cloned tables](https://docs.databricks.com/aws/en/delta/clone). ({issue}`27098`)

## Druid connector

## DuckDB connector

## Elasticsearch connector

## Exasol connector

## Faker connector

## Google Sheets connector

## Hive connector

* Remove `s3.exclusive-create` config. ({issue}`27372`)
* Fix updating table statistics when running INSERT queries with OVERWRITE behaviour. ({issue}`26517`)
* Harden hierarchical namespace check in Azure with root blob fallback check. ({issue}`27278`)
* Avoid reading unusually large pages from parquet files. ({issue}`27148`)
* Fix potential `FileAlreadyExistsException` failure when network failures during s3 write. ({issue}`27330`)

## Hudi connector

* Add `APPLICATION_DEFAULT` authentication type for GCS. ({issue}`26984`)
* Remove unauthentication access support in `SERVICE_ACCOUNT`. ({issue}`26984`)
* Harden hierarchical namespace check in Azure with root blob fallback check. ({issue}`27278`)
* Avoid reading unusually large pages from parquet files. ({issue}`27148`)
* Fix potential `FileAlreadyExistsException` failure when network failures during s3 write. ({issue}`27330`)

## Iceberg connector

* Add support for disabling token exchange via the `iceberg.rest-catalog.oauth2.token-exchange-enabled` config property. ({issue}`27174`)
* Add `retain_last` and `clean_expired_metadata` options to `expire_snapshots` command. ({issue}`27357`)
* Remove `s3.exclusive-create` config. ({issue}`27372`)
* Add `APPLICATION_DEFAULT` authentication type for GCS. ({issue}`26984`)
* Remove unauthentication access support in `SERVICE_ACCOUNT`. ({issue}`26984`)
* Fix failures when querying `$files` table after changes to table partitioning. ({issue}`26746`)
* Fix incorrect results due to incorrect pushdown of `IS NOT DISTINCT FROM`. ({issue}`27213`)
* Harden hierarchical namespace check in Azure with root blob fallback check. ({issue}`27278`)
* Avoid reading unusually large pages from parquet files. ({issue}`27148`)
* Fix potential `FileAlreadyExistsException` failure when network failures during s3 write. ({issue}`27330`)
* Fix potential failure when dropping a schema with cascade. ({issue}`27361`)

## Ignite connector

## JMX connector

## Kafka connector

## Loki connector

* Fix failure when initializing the connector. ({issue}`27180`)

## MariaDB connector

## Memory connector

* Add support for setting and dropping column defaults via `ALTER TABLE ... ALTER COLUMN`. ({issue}`26162`)
* Fix concurrent modification exception in `RENAME SCHEMA` of the `memory` connector. ({issue}`27205`)

## MongoDB connector

## MySQL connector

* Fix incorrect results due to incorrect pushdown of `IS NOT DISTINCT FROM`. ({issue}`27213`)

## OpenSearch connector

## Oracle connector

## Pinot connector

## PostgreSQL connector

* Fix incorrect results due to incorrect pushdown of `IS NOT DISTINCT FROM`. ({issue}`27213`)

## Prometheus connector

## Redis connector

## Redshift connector

* Fix failure when reading Redshift `character varying` type. ({issue}`27224`)

## SingleStore connector

## Snowflake connector

## SQL Server connector

* Fix potential failure when listing tables and columns. ({issue}`10846`)

## TPC-H connector

## TPC-DS connector

## Vertica connector

## SPI

* Add non-callback based entry builder to RowBlockBuilder. ({issue}`27198`)
* Add non-callback based entry builder to ArrayBlockBuilder. ({issue}`27198`)
