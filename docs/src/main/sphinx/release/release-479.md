# Release 479 (14 Dec 2025)

## General

* Generate TLS certificates for internal cluster communications automatically when node discovery type is set to `ANNOUNCE`. ({issue}`27030`)
* Add support for specifying the location of the Exchange Manager configuration file. ({issue}`26611`)
* Publish time taken by query in `FINISHING` state to event listener. ({issue}`27202`)
* Add lineage support for output columns of `SELECT` queries. ({issue}`26241`)
* Add support for setting and dropping column defaults via `ALTER TABLE ... ALTER COLUMN` statement. ({issue}`26162`)
* Add {func}`array_first` and {func}`array_last` functions. ({issue}`27295`)
* Add `GRACE PERIOD` to `SHOW CREATE MATERIALIZED VIEW` output. ({issue}`27529`)
* Allow field name declaration in row literals. For example, `row(1 as a, 2 as b)` is now legal. ({issue}`25261`)
* Add `queryText` as a regular expression in resource group selector. ({issue}`27129`)
* Require JDK 25 to build and run Trino. ({issue}`27153`)
* {{breaking}} The configuration property `task.statistics-cpu-timer-enabled` is now defunct and must be removed. ({issue}`27504`)
* Deprecate `EXPLAIN` type `LOGICAL` and `DISTRIBUTED`. Use `EXPLAIN` without a type clause, instead. ({issue}`27434`)
* Remove `prefer_streaming_operators` session property. ({issue}`27506`)
* Add experimental performance improvements for remote data exchanges on newer CPU architectures, such as `Graviton 3`, `Skylake`,
  `Icelake`, or `Zen 4+`. This can be disabled by setting `exchange.experimental.vectorized-serde.enabled=false`. ({issue}`27426`, {issue}`26919`)
* Improve performance of {func}`array_sort` function. ({issue}`27272`)
* Improve performance of {func}`repeat` function. ({issue}`27369`)
* Improve performance of data exchanges involving variable width data. ({issue}`27377`)
* Improve performance of queries referencing fresh materialized views. ({issue}`27551`)
* Fix query failure when one of the branches of a `UNION` is known to produce zero rows during query planning. ({issue}`21506`)
* Fix configuring partitioned layout for spooling protocol. ({issue}`27247`)
* Fix `EXPLAIN (TYPE IO)` failure when the `WHERE` clause involves a type which cannot be cast to `varchar`. ({issue}`27433`)

## Web UI

* Render long single-line query string correctly in preview UI query details page. ({issue}`27328`)
* Render catalog properties correctly in preview UI. ({issue}`27327`)

## JDBC driver

* Add a `extraHeaders` option to support sending arbitrary HTTP headers. ({issue}`15826`)

## Docker image

* Update JDK to 25.0.1. ({issue}`27117`)

## CLI

* Add a `--extra-header` option to support sending arbitrary HTTP headers. ({issue}`15826`)
* Fix TLS connection failures when using unqualified (single-label, without ".") hostnames. ({issue} `27478`)

## BigQuery connector

* Fix query failure when reusing {func}`query` table function result. ({issue}`27573`)

## Delta Lake connector

* Add `APPLICATION_DEFAULT` authentication type for GCS. ({issue}`26984`)
* {{breaking}} Remove support for unauthenticated access when GCS authentication type is set to `SERVICE_ACCOUNT`. ({issue}`26984`)
* Rename `s3.exclusive-create` config to `delta.s3.transaction-log-conditional-writes.enabled`. ({issue}`27372`)
* Fix incorrect results for queries involving `IS NOT DISTINCT FROM`. ({issue}`27213`)
* Fix failure when writing to tables created by Databricks 17.3. ({issue}`27100`)
* Fix failure when checking Azure hierarchical namespaces. ({issue}`27278`)
* Avoid worker crashes by failing queries attempting to read columns with huge values in parquet files. ({issue}`27148`)
* Fix potential `FileAlreadyExistsException` failure when network failures occur during writes to S3. ({issue}`27330`, {issue}`27388`)
* Fix potential failure when reading [cloned tables](https://docs.databricks.com/aws/en/delta/clone). ({issue}`27098`)

## Hive connector

* Add `APPLICATION_DEFAULT` authentication type for GCS. ({issue}`26984`)
* {{breaking}} Remove support for unauthenticated access when GCS authentication type is set to `SERVICE_ACCOUNT`. ({issue}`26984`)
* Rename `s3.exclusive-create` config to `delta.s3.transaction-log-conditional-writes.enabled`. ({issue}`27372`)
* Improve accuracy of table statistics written by INSERT queries with OVERWRITE behaviour. ({issue}`26517`)
* Fix failure when checking Azure hierarchical namespaces. ({issue}`27278`)
* Avoid worker crashes by failing queries attempting to read columns with huge values in parquet files. ({issue}`27148`)
* Fix potential `FileAlreadyExistsException` failure when network failures occur during writes to S3. ({issue}`27330`)
* Fix failure when listing tables with invalid table metadata in AWS Glue. ({issue}`27525`)

## Hudi connector

* Add `APPLICATION_DEFAULT` authentication type for GCS. ({issue}`26984`)
* {{breaking}} Remove support for unauthenticated access when GCS authentication type is set to `SERVICE_ACCOUNT`. ({issue}`26984`)
* Fix failure when checking Azure hierarchical namespaces. ({issue}`27278`)
* Avoid worker crashes by failing queries attempting to read columns with huge values in parquet files. ({issue}`27148`)
* Fix potential `FileAlreadyExistsException` failure when network failures occur during writes to S3. ({issue}`27330`)
* Fix failure when querying HDFS that includes empty directories. ({issue}`26897`)

## Iceberg connector

* Add support for updating [token-exchange-enabled](https://iceberg.apache.org/docs/nightly/configuration/#oauth2-auth-properties) via the `iceberg.rest-catalog.oauth2.token-exchange-enabled` config property. ({issue}`27174`)
* Add `retain_last` and `clean_expired_metadata` options to `expire_snapshots` command. ({issue}`27357`)
* Add `APPLICATION_DEFAULT` authentication type for GCS. ({issue}`26984`)
* {{breaking}} Remove support for unauthenticated access when GCS authentication type is set to `SERVICE_ACCOUNT`. ({issue}`26984`)
* Reduce memory pressure when the table contains highly nested fields. ({issue}`25077`)
* Fix failures when querying `$files` table after changes to table partitioning. ({issue}`26746`)
* Fix incorrect results for queries involving `IS NOT DISTINCT FROM`. ({issue}`27213`)
* Fix failure when checking Azure hierarchical namespaces. ({issue}`27278`)
* Avoid worker crashes by failing queries attempting to read columns with huge values in parquet files. ({issue}`27148`)
* Fix potential `FileAlreadyExistsException` failure when network failures occur during writes to S3. ({issue}`27330`)
* Fix potential failure when dropping a schema with `CASCADE` option. ({issue}`27361`)
* Ignore non-existent or corrupted base Iceberg tables when querying materialized views within the grace period. ({issue}`27606`)

## Loki connector

* Fix failure when initializing the connector. ({issue}`27180`)

## Memory connector

* Add support for setting and dropping column defaults via `ALTER TABLE ... ALTER COLUMN`. ({issue}`26162`)
* Fix failure in `RENAME SCHEMA` when there is more than one table in the schema. ({issue}`27205`)

## MySQL connector

* Fix incorrect results for queries involving `IS NOT DISTINCT FROM`. ({issue}`27213`)

## PostgreSQL connector

* Fix incorrect results for queries involving `IS NOT DISTINCT FROM`. ({issue}`27213`)

## Redshift connector

* Fix failure when reading Redshift `character varying` type. ({issue}`27224`)

## SQL Server connector

* Fix potential failure when listing tables and columns. ({issue}`10846`)

## SPI

* Add non-callback based entry builder to RowBlockBuilder. ({issue}`27198`)
* Add non-callback based entry builder to ArrayBlockBuilder. ({issue}`27198`)
* Fix `ColumnMetadata.builderFrom` to retain column default value. ({issue}`27503`)
