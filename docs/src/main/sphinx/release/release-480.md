# Release 480 (xxx Feb 2026)

## General

* Add coordinator and worker counts to `/metrics` endpoint. ({issue}`27408`)
* Allow configuring the maximum amount of memory to use while writing tables
  through the `task.scale-writers.max-writer-memory-percentage` configuration
  property. ({issue}`27874`)
* Add `array_first(array(E), function(E, boolean))` to return an element
  matching the predicate. ({issue}`27706`)
* {{breaking}} Remove `enable-large-dynamic-filters` configuration property and the 
  corresponding system session property `enable_large_dynamic_filters`. ({issue}`27637`)
* {{breaking}} Remove the `dynamic-filtering.small*` and `dynamic-filtering.large-broadcast*` 
  configuration properties. ({issue}`27637`)
* Improve performance for remote data exchanges on newer CPU architectures and Graviton
  4 CPUs. ({issue}`27586`)
* Improve performance of queries with remote data exchanges or aggregations. ({issue}`27657`)
* Reduce out-of-memory errors for queries involving window functions when spilling is enabled. ({issue}`27873`)
* Fix incorrect results when using {func}`localtimestamp` with precision 3. ({issue}`27806`)
* Fix {func}`localtimestamp` failure for precisions 7 and 8. ({issue}`27807`)
* Fix spurious query failures when querying the `system` catalog during catalog
  drop operations. ({issue}`28017`)
* Fix failure when executing `date_add` function with larger value. ({issue}`27899`)

## Web UI

* Add cluster status info to the header in the preview UI. ({issue}`27712`)
* Sort stages in the query details page numerically rather than alphabetically. ({issue}`27655`)

## ClickHouse connector

* Fix failure when creating a table if a prior `CREATE TABLE ... AS SELECT` 
  operation for the same table failed. ({issue}`27702`)

## Delta Lake connector

* {{breaking}} Remove live files table metadata cache. The configuration 
  properties `metadata.live-files.cache-size`, `metadata.live-files.cache-ttl` and 
  `checkpoint-filtering.enabled` are now defunct and must be removed from server 
  configurations. ({issue}`27618`)
* {{breaking}} Remove the `hive.write-validation-threads` configuration property. ({issue}`27729`)
* {{breaking}} Remove the `parquet.optimized-writer.validation-percentage` configuration
  property, use `parquet.writer.validation-percentage`, instead. ({issue}`27729`)
* {{breaking}} Remove the `hive.parquet.writer.block-size` configuration property, use
  `parquet.writer.block-size`, instead. ({issue}`27729`)
* {{breaking}} Remove the `hive.parquet.writer.page-size` configuration property, use
  `parquet.writer.page-size`, instead. ({issue}`27729`)
* {{breaking}} Remove the `gcs.use-access-token` configuration property. ({issue}`26941`)
* {{breaking}} Remove the `hive.fs.new-file-inherit-ownership` configuration property. ({issue}`28029`)
* Improve the effectiveness of Bloom filters for high-cardinality columns in Parquet files. ({issue}`27656`)
* Remove the requirement for the `PutObjectTagging` AWS S3 permission when 
  writing to Delta Lake tables on S3. ({issue}`27701`)
* Fix potential table corruption when executing `CREATE OR REPLACE` with table definition
  changes. ({issue}`27805`)
* Fix Azure Storage connectivity issues ({issue}`28058`)

## DuckDB connector

* Fix failure when creating a table if a prior `CREATE TABLE ... AS SELECT`
  operation for the same table failed. ({issue}`27702`)

## Hive connector

* Add support for reading nanosecond-precision timestamps from Parquet files 
  into `timestamp(p) with time zone` columns. ({issue}`27861`)
* {{breaking}} Remove the `hive.write-validation-threads` configuration property. ({issue}`27729`)
* {{breaking}} Remove the `parquet.optimized-writer.validation-percentage` configuration
  property, use the `parquet.writer.validation-percentage`, instead. ({issue}`27729`)
* {{breaking}} Remove the `hive.parquet.writer.block-size` configuration property, use
  `parquet.writer.block-size`, instead. ({issue}`27729`)
* {{breaking}} Remove the `hive.parquet.writer.page-size` configuration property, use 
  `parquet.writer.page-size`, instead. ({issue}`27729`)
* {{breaking}} Remove the deprecated `gcs.use-access-token` configuration property. ({issue}`26941`)
* {{breaking}} Remove the `hive.fs.new-file-inherit-ownership` configuration property. ({issue}`28029`)
* Improve the effectiveness of Bloom filters for high-cardinality columns in Parquet files. ({issue}`27656`)
* Fix Azure Storage connectivity issues ({issue}`28058`)

## Hudi connector

* {{breaking}} Remove the `hive.write-validation-threads` configuration property. ({issue}`27729`)
* {{breaking}} Remove the `parquet.optimized-writer.validation-percentage` configuration
  property, use `parquet.writer.validation-percentage`, instead. ({issue}`27729`)
* {{breaking}} Remove the `hive.parquet.writer.block-size` configuration property, use
  `parquet.writer.block-size`, instead. ({issue}`27729`)
* {{breaking}} Remove the `hive.parquet.writer.page-size` configuration property, use
  `parquet.writer.page-size`, instead. ({issue}`27729`)
* {{breaking}} Remove the deprecated `gcs.use-access-token` configuration property. ({issue}`26941`)
* {{breaking}} Remove the `hive.fs.new-file-inherit-ownership` configuration property. ({issue}`28029`)
* Fix Azure Storage connectivity issues ({issue}`28058`)

## Iceberg connector

* Add support for the BigLake metastore in Iceberg REST catalog. ({issue}`26219`)
* Add `delete_after_commit_enabled` and `max_previous_versions` table properties. ({issue}`14128`)
* Add support for column default values in Iceberg v3 tables. ({issue}`27837`)
* Add support for creating, writing to or deleting from Iceberg v3 tables. ({issue}`27786`, {issue}`27788`)
* Add `content` column to `$manifests` and `$all_manifests` metadata tables. ({issue}`27975`)
* Add support for changing nested types in `array` and `map` types. ({issue}`27998`)
* Clean up unused files from materialized views when they are refreshed. ({issue}`28008`)
* {{breaking}} Remove the `hive.write-validation-threads` configuration property. ({issue}`27729`)
* {{breaking}} Remove the `parquet.optimized-writer.validation-percentage` configuration
  property, use `parquet.writer.validation-percentage`, instead. ({issue}`27729`)
* {{breaking}} Remove the `hive.parquet.writer.block-size` configuration property, use
  `parquet.writer.block-size`, instead. ({issue}`27729`)
* {{breaking}} Remove the `hive.parquet.writer.page-size` configuration property, use
  `parquet.writer.page-size`, instead. ({issue}`27729`)
* {{breaking}} Remove the deprecated `gcs.use-access-token` configuration property. ({issue}`26941`)
* {{breaking}} Remove the `hive.fs.new-file-inherit-ownership` configuration property. ({issue}`28029`)
* {{breaking}} Remove support for the `iceberg.extended-statistics.enabled` configuration option and
  `extended_statistics_enabled` session property. ({issue}`27914`)
* Improve the effectiveness of Bloom filters for high-cardinality columns in Parquet files. ({issue}`27656`)
* Improve query performance when querying a fresh materialized view. ({issue}`27608`)
* Improve `optimize_manifests` to cluster manifests by partitions for improving
  performance of reads with partition filters. ({issue}`27358`)
* Reduce planning time of queries on tables containing delete files. ({issue}`27955`)
* Reduce planning time for queries involving simple `FROM` and `WHERE` clauses. ({issue}`27973`)
* Reduce query planning time on large tables. ({issue}`28068`)
* Fix failure when reading `$files` metadata tables when
  scheme involving `bucket` or `truncate` changes. ({issue}`26109`)
* Fix failure when reading `$file_modified_time` metadata column on tables with equality
  deletes. ({issue}`27850`)
* Avoid large footers in Parquet files from certain rare string inputs. ({issue}`27903`)
* Fix failures for queries with joins on metadata columns. ({issue}`27984`)
* Fix Azure Storage connectivity issues ({issue}`28058`)

## Ignite connector

* Fix failure when creating a table if a prior `CREATE TABLE ... AS SELECT`
  operation for the same table failed. ({issue}`27702`)

## Lakehouse connector

* Fix failure when reading Iceberg `$files` tables. ({issue}`26751`)

## MariaDB connector

* Fix failure when creating a table if a prior `CREATE TABLE ... AS SELECT`
  operation for the same table failed. ({issue}`27702`)

## MySQL connector

* Fix failure when creating a table if a prior `CREATE TABLE ... AS SELECT`
  operation for the same table failed. ({issue}`27702`)

## Oracle connector

* Add support for configuring the connection wait timeout through the 
  `oracle.connection-pool.wait-timeout` catalog property. ({issue}`27744`)
* Fix failure when creating a table if a prior `CREATE TABLE ... AS SELECT`
  operation for the same table failed. ({issue}`27702`)
* Fix failure when reading `float` type in `query` table function. ({issue}`27880`)

## PostgreSQL connector

* Fix failure when creating a table if a prior `CREATE TABLE ... AS SELECT`
  operation for the same table failed. ({issue}`27702`)

## Redshift connector

* Fix failure when creating a table if a prior `CREATE TABLE ... AS SELECT`
  operation for the same table failed. ({issue}`27702`)

## SingleStore connector

* Fix failure when creating a table if a prior `CREATE TABLE ... AS SELECT`
  operation for the same table failed. ({issue}`27702`)

## Snowflake connector

* Fix failure when creating a table if a prior `CREATE TABLE ... AS SELECT`
  operation for the same table failed. ({issue}`27702`)

## SQL Server connector

* Fix failure when creating a table if a prior `CREATE TABLE ... AS SELECT`
  operation for the same table failed. ({issue}`27702`)

## Vertica connector

* Fix failure when creating a table if a prior `CREATE TABLE ... AS SELECT`
  operation for the same table failed. ({issue}`27702`)

## SPI

* Remove support for `TypeSignatureParameter`. Use `TypeParameter` instead. ({issue}`27574`)
* Remove support for `ParameterKind`. Use `TypeParameter.Type`, `TypeParameter.Numeric`, 
  and `TypeParameter.Variable` instead. ({issue}`27574`)
* Remove support for `NamedType`, `NamedTypeSignature` and `NamedTypeParameter`. Use 
  `TypeParameter.Type` instead. ({issue}`27574`)
* Deprecate `MaterializedViewFreshness#getLastFreshTime`.
  Use `getLastKnownFreshTime` instead. ({issue}`27803`)
* Change `ColumnMetadata.comment` and `ColumnMetadata.extraInfo` to `Optional<String>`. ({issue}`28151`)
