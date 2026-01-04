# Release 480 (TBD)

## General

* Add the `WHEN STALE [INLINE | FAIL]` clause to `CREATE MATERIALIZED VIEW`. ({issue}`27502`)
* {{breaking}} Remove `enable-large-dynamic-filters` configuration property and the corresponding system 
  session property `enable_large_dynamic_filters`. Large dynamic filters are used by default. ({issue}`27637`)
* {{breaking}} Remove `dynamic-filtering.small*` configuration properties. ({issue}`27637`)
* {{breaking}} Remove `dynamic-filtering.large-broadcast*` configuration. ({issue}`27637`).
* Extend experimental performance improvements for remote data exchanges on newer CPU architectures. ({issue}`27586`)
* Enable experimental performance improvements for remote data exchanges on Graviton 4 CPUs. ({issue}`27586`)
* Improve performance of queries with data exchanges or aggregations. ({issue}`27657`)

## Web UI

* Fix numeric ordering of stages in the UI. ({issue}`27655`)

## ClickHouse connector

* Fix failure when creating table caused by incorrect cleanup of the tables after a failed CTAS operation. ({issue}`27702`)

## Delta Lake connector

* {{breaking}} Remove live files table metadata cache. The configuration properties 
  `metadata.live-files.cache-size`, `metadata.live-files.cache-ttl` and `checkpoint-filtering.enabled` 
  are now defunct and must be removed from server configurations. ({issue}`27618`)
* {{breaking}} Remove `hive.write-validation-threads` configuration property. ({issue}`27729`)
* {{breaking}} Remove `parquet.optimized-writer.validation-percentage` configuration property, use 
  `parquet.writer.validation-percentage`, instead. ({issue}`27729`)
* {{breaking}} Remove `hive.parquet.writer.block-size` configuration property, use `parquet.writer.block-size`,
  instead. ({issue}`27729`)
* {{breaking}} Remove `hive.parquet.writer.page-size` configuration property, use `parquet.writer.page-size`, 
  instead. ({issue}`27729`)
* Improve effectiveness of bloom filters written in parquet files for high cardinality columns. ({issue}`27656`)
* Do not require `PutObjectTagging` AWS S3 permission when writing to Delta Lake tables on S3. ({issue}`27701`)

## DuckDB connector

* Fix failure when creating table caused by incorrect cleanup of the tables after a failed CTAS operation. ({issue}`27702`)

## Hive connector

* {{breaking}} Remove live files table metadata cache. The configuration properties
  `metadata.live-files.cache-size`, `metadata.live-files.cache-ttl` and `checkpoint-filtering.enabled`
  are now defunct and must be removed from server configurations. ({issue}`27618`)
* {{breaking}} Remove `hive.write-validation-threads` configuration property. ({issue}`27729`)
* {{breaking}} Remove `parquet.optimized-writer.validation-percentage` configuration property, use
  `parquet.writer.validation-percentage`, instead. ({issue}`27729`)
* {{breaking}} Remove `hive.parquet.writer.block-size` configuration property, use `parquet.writer.block-size`,
  instead. ({issue}`27729`)
* {{breaking}} Remove `hive.parquet.writer.page-size` configuration property, use `parquet.writer.page-size`,
  instead. ({issue}`27729`)
* Improve effectiveness of bloom filters written in parquet files for high cardinality columns. ({issue}`27656`)

## Hudi connector

* {{breaking}} Remove `hive.write-validation-threads` configuration property. ({issue}`27729`)
* {{breaking}} Remove `parquet.optimized-writer.validation-percentage` configuration property, use
  `parquet.writer.validation-percentage`, instead. ({issue}`27729`)
* {{breaking}} Remove `hive.parquet.writer.block-size` configuration property, use `parquet.writer.block-size`,
  instead. ({issue}`27729`)
* {{breaking}} Remove `hive.parquet.writer.page-size` configuration property, use `parquet.writer.page-size`,
  instead. ({issue}`27729`)
* Improve effectiveness of bloom filters written in parquet files for high cardinality columns. ({issue}`27656`)

## Iceberg connector

* Add support for BigLake metastore in Iceberg REST catalog. ({issue}`26219`)
* Add `delete_after_commit_enabled` and `max_previous_versions` table properties. ({issue}`14128`)
* Add support for the `WHEN STALE` clause in the Iceberg connector. ({issue}`27502`)
* {{breaking}} Remove `hive.write-validation-threads` configuration property. ({issue}`27729`)
* {{breaking}} Remove `parquet.optimized-writer.validation-percentage` configuration property, use
  `parquet.writer.validation-percentage`, instead. ({issue}`27729`)
* {{breaking}} Remove `hive.parquet.writer.block-size` configuration property, use `parquet.writer.block-size`,
  instead. ({issue}`27729`)
* {{breaking}} Remove `hive.parquet.writer.page-size` configuration property, use `parquet.writer.page-size`,
  instead. ({issue}`27729`)
* Fix failure when reading `$files` metadata table with partition evolution using `truncate` or `bucket` 
  on the same column. ({issue}`26109`)
* Optimize Iceberg materialized view freshness checks based on grace period. ({issue}`27608`)

## Ignite connector

* Fix failure when creating table caused by incorrect cleanup of the tables after a failed CTAS operation. ({issue}`27702`)

## Lakehouse

* Add support for the `WHEN STALE` clause in the Lakehouse connector. ({issue}`27502`)
* Fix failure when reading Iceberg `$files` table. ({issue}`26751`)

## MariaDB connector

* Fix failure when creating table caused by incorrect cleanup of the tables after a failed CTAS operation. ({issue}`27702`)

## MySQL connector

* Fix failure when creating table caused by incorrect cleanup of the tables after a failed CTAS operation. ({issue}`27702`)

## Oracle connector

* Fix failure when creating table caused by incorrect cleanup of the tables after a failed CTAS operation. ({issue}`27702`)

## PostgreSQL connector

* Fix failure when creating table caused by incorrect cleanup of the tables after a failed CTAS operation. ({issue}`27702`)

## Redshift connector

* Fix failure when creating table caused by incorrect cleanup of the tables after a failed CTAS operation. ({issue}`27702`)

## SingleStore connector

* Fix failure when creating table caused by incorrect cleanup of the tables after a failed CTAS operation. ({issue}`27702`)

## Snowflake connector

* Fix failure when creating table caused by incorrect cleanup of the tables after a failed CTAS operation. ({issue}`27702`)

## SQL Server connector

* Fix failure when creating table caused by incorrect cleanup of the tables after a failed CTAS operation. ({issue}`27702`)

## Vertica connector

* Fix failure when creating table caused by incorrect cleanup of the tables after a failed CTAS operation. ({issue}`27702`)

## SPI

* Remove support for `TypeSignatureParameter`. Use `TypeParameter`, instead. ({issue}`27574`)
* Remove support for `ParameterKind`. Use `TypeParameter.Type`, `TypeParameter.Numeric`, `TypeParameter.Variable`, instead. ({issue}`27574`)
* Remove support for `NamedType`, `NamedTypeSignature` and `NamedTypeParameter`. Use `TypeParameter.Type`, instead. ({issue}`27574`)
* Deprecate `MaterializedViewFreshness#getLastFreshTime`. ({issue}`27803`)