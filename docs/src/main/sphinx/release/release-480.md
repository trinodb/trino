# Release 480 (updated to 13/01/2026)

## General

* {{breaking}} Remove `enable-large-dynamic-filters` configuration property and the 
  corresponding system session property `enable_large_dynamic_filters`. Large dynamic
  filters are used by default. ({issue}`27637`)
* {{breaking}} Remove `dynamic-filtering.small*` and `dynamic-filtering.large-broadcast*` 
  configuration properties. ({issue}`27637`)
* Extend experimental performance improvements for remote data exchanges on newer CPU 
  architectures. ({issue}`27586`)
* Enable experimental performance improvements for remote data exchanges on Graviton 4 
  CPUs. ({issue}`27586`)
* Improve performance of queries with data exchanges or aggregations. ({issue}`27657`)
* Reduce out-of-memory errors in window queries when spill is enabled. ({issue}`27873`)
* Fix double rounding in {func}`localtimestamp` for sub-micro precision values. ({issue}`27806`)
* Fix {func}`localtimestamp` failure for precisions 7-8. ({issue}`27807`)

## Web UI

* Add cluster status info to header in preview UI. ({issue}`27712`)
* Fix numeric ordering of stages. ({issue}`27655`)

## ClickHouse connector

* Fix failure when creating table caused by incorrect cleanup of the tables after a failed
  `CREATE TABLE ... AS SELECT` operation. ({issue}`27702`)

## Delta Lake connector

* {{breaking}} Remove live files table metadata cache. The configuration 
  properties `metadata.live-files.cache-size`, `metadata.live-files.cache-ttl` and 
  `checkpoint-filtering.enabled` are now defunct and must be removed from server 
  configurations. ({issue}`27618`)
* {{breaking}} Remove `hive.write-validation-threads` configuration property. ({issue}`27729`)
* {{breaking}} Remove `parquet.optimized-writer.validation-percentage` configuration
  property, use `parquet.writer.validation-percentage`, instead. ({issue}`27729`)
* {{breaking}} Remove `hive.parquet.writer.block-size` configuration property, use
  `parquet.writer.block-size`, instead. ({issue}`27729`)
* {{breaking}} Remove `hive.parquet.writer.page-size` configuration property, use
  `parquet.writer.page-size`, instead. ({issue}`27729`)
* {{breaking}} Remove the deprecated `gcs.use-access-token` config property. ({issue}`26941`)
* Improve effectiveness of bloom filters written in Parquet files for high cardinality 
  columns. ({issue}`27656`)
* Do not require `PutObjectTagging` AWS S3 permission when writing to Delta Lake tables 
  on S3. ({issue}`27701`)

## DuckDB connector

* Fix failure when creating table caused by incorrect cleanup of the tables after a failed
  CTAS operation. ({issue}`27702`)

## Hive connector

* Add support for reading Parquet files with timestamps stored in nanosecond units as a 
  `timestamp with time zone` column. ({issue}`27861`)
* {{breaking}} Remove `hive.write-validation-threads` configuration property. ({issue}`27729`)
* {{breaking}} Remove `parquet.optimized-writer.validation-percentage` configuration
  property, use `parquet.writer.validation-percentage`, instead. ({issue}`27729`)
* {{breaking}} Remove `hive.parquet.writer.block-size` configuration property, use
  `parquet.writer.block-size`, instead. ({issue}`27729`)
* {{breaking}} Remove `hive.parquet.writer.page-size` configuration property, use 
  `parquet.writer.page-size`, instead. ({issue}`27729`)
* {{breaking}} Remove the deprecated `gcs.use-access-token` config property. ({issue}`26941`)
* Improve effectiveness of bloom filters written in Parquet files for high cardinality 
  columns. ({issue}`27656`)

## Hudi connector

* {{breaking}} Remove `hive.write-validation-threads` configuration property. ({issue}`27729`)
* {{breaking}} Remove `parquet.optimized-writer.validation-percentage` configuration
  property, use `parquet.writer.validation-percentage`, instead. ({issue}`27729`)
* {{breaking}} Remove `hive.parquet.writer.block-size` configuration property, use
  `parquet.writer.block-size`, instead. ({issue}`27729`)
* {{breaking}} Remove `hive.parquet.writer.page-size` configuration property, use
  `parquet.writer.page-size`, instead. ({issue}`27729`)
* {{breaking}} Remove the deprecated `gcs.use-access-token` config property. ({issue}`26941`)
* Improve effectiveness of bloom filters written in Parquet files for high cardinality columns. ({issue}`27656`)

## Iceberg connector

* Add support for BigLake metastore in Iceberg REST catalog. ({issue}`26219`)
* Add `delete_after_commit_enabled` and `max_previous_versions` table properties. ({issue}`14128`)
* Allow creating Iceberg format version 3 tables, upgrading v2 tables to v3, and inserting 
  into v3 tables. Unsupported v3 features are explicitly rejected. ({issue}`27786`)
* {{breaking}} Remove `hive.write-validation-threads` configuration property. ({issue}`27729`)
* {{breaking}} Remove `parquet.optimized-writer.validation-percentage` configuration
  property, use `parquet.writer.validation-percentage`, instead. ({issue}`27729`)
* {{breaking}} Remove `hive.parquet.writer.block-size` configuration property, use
  `parquet.writer.block-size`, instead. ({issue}`27729`)
* {{breaking}} Remove `hive.parquet.writer.page-size` configuration property, use
  `parquet.writer.page-size`, instead. ({issue}`27729`)
* {{breaking}} Remove the deprecated `gcs.use-access-token` config property. ({issue}`26941`)
* Optimize Iceberg materialized view freshness checks based on grace period. ({issue}`27608`)
* Fix failure when reading `$files` metadata table with partition evolution using 
  `truncate` or `bucket` on the same column. ({issue}`26109`)
* Fix failure when reading `$file_modified_time` metadata column on tables with equality
  deletes. ({issue}`27850`)
* Avoid Parquet footer explosion when binary columns contain certain pathological values. ({issue}`27903`)

## Ignite connector

* Fix failure when creating table caused by incorrect cleanup of the tables after a failed
  CTAS operation. ({issue}`27702`)

## Lakehouse

* Fix failure when reading Iceberg `$files` tables. ({issue}`26751`)

## MariaDB connector

* Fix failure when creating table caused by incorrect cleanup of the tables after a failed
  CTAS operation. ({issue}`27702`)

## MySQL connector

* Fix failure when creating table caused by incorrect cleanup of the tables after a failed
  CTAS operation. ({issue}`27702`)

## Oracle connector

* Add support for configuring a connection wait timeout via the `oracle.connection-pool.wait-timeout` 
  catalog property. ({issue}`27744`)
* Fix failure when creating table caused by incorrect cleanup of the tables after a failed
  CTAS operation. ({issue}`27702`)

## PostgreSQL connector

* Fix failure when creating table caused by incorrect cleanup of the tables after a failed
  CTAS operation. ({issue}`27702`)

## Redshift connector

* Fix failure when creating table caused by incorrect cleanup of the tables after a failed
  CTAS operation. ({issue}`27702`)

## SingleStore connector

* Fix failure when creating table caused by incorrect cleanup of the tables after a failed
  CTAS operation. ({issue}`27702`)

## Snowflake connector

* Fix failure when creating table caused by incorrect cleanup of the tables after a failed
  CTAS operation. ({issue}`27702`)

## SQL Server connector

* Fix failure when creating table caused by incorrect cleanup of the tables after a failed
  CTAS operation. ({issue}`27702`)

## Vertica connector

* Fix failure when creating table caused by incorrect cleanup of the tables after a failed
  CTAS operation. ({issue}`27702`)

## SPI

* Remove support for `TypeSignatureParameter`. Use `TypeParameter`, instead. ({issue}`27574`)
* Remove support for `ParameterKind`. Use `TypeParameter.Type`, `TypeParameter.Numeric`, 
  `TypeParameter.Variable`, instead. ({issue}`27574`)
* Remove support for `NamedType`, `NamedTypeSignature` and `NamedTypeParameter`. Use 
  `TypeParameter.Type`, instead. ({issue}`27574`)
* Deprecate `MaterializedViewFreshness#getLastFreshTime`. ({issue}`27803`)