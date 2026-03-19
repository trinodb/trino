# Release 480 (xxx Mar 2026)

## General

* Add `number` type. ({issue}`28319`)
* Add coordinator and worker counts to `/metrics` endpoint. ({issue}`27408`)
* Allow configuring the maximum amount of memory to use while writing tables
  through the `task.scale-writers.max-writer-memory-percentage` configuration
  property. ({issue}`27874`)
* Add variant of {func}`array_first` for finding the first element that matches
  a predicate. ({issue}`27706`)
* Add support for casting `varbinary` to `json` with base64. ({issue}`28234`)
* Add {doc}`/functions/datasketches`. ({issue}`27563`)
* {{breaking}} Remove `enable-large-dynamic-filters` configuration property and the 
  corresponding system session property `enable_large_dynamic_filters`. ({issue}`27637`)
* {{breaking}} Remove the `dynamic-filtering.small*` and `dynamic-filtering.large-broadcast*` 
  configuration properties. ({issue}`27637`)
* {{breaking}} Remove `deprecated.http-server.authentication.oauth2.groups-field`
  configuration property. ({issue}`28646`)
* Improve performance for remote data exchanges on newer CPU architectures and Graviton
  4 CPUs. ({issue}`27586`)
* Improve performance of queries with remote data exchanges or aggregations. ({issue}`27657`)
* Reduce out-of-memory errors for queries involving window functions when spilling is enabled. ({issue}`27873`)
* Fix incorrect results when using {func}`localtimestamp` with precision 3. ({issue}`27806`)
* Fix {func}`localtimestamp` failure for precisions 7 and 8. ({issue}`27807`)
* Fix spurious query failures when querying the `system` catalog during catalog
  drop operations. ({issue}`28017`)
* Fix failure when executing {func}`date_add` function with a value greater than
  `Integer.MAX_VALUE`. ({issue}`27899`)
* Fix incorrect results when the result of casting `json`, `time`, `boolean` or
  `interval` values to `varchar(n)` doesn't fit in the target type. ({issue}`552`)

## Web UI

* Add cluster status info to the header in the preview UI. ({issue}`27712`)
* Sort stages in the query details page numerically rather than alphabetically. ({issue}`27655`)

## JDBC driver

* Fix incorrect result of `ResultSetMetaData.getColumnClassName` for
  `map`, `row`, `time with time zone`, `timestamp with time zone`, `varbinary`
  and `null` values. ({issue}`28314`)

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
* Fix Azure Storage connectivity issues. ({issue}`28058`)
* Fix failure when the file path contains `#` in GCS. ({issue}`28292`)
* Fix NPE when loading parquet column index with non stats supported column. ({issue}`28560`)

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
* Fix Azure Storage connectivity issues. ({issue}`28058`)
* Fix incorrect memory accounting for `INSERT` queries targeting bucketed and sorted tables. ({issue}`28315`)

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
* Fix Azure Storage connectivity issues. ({issue}`28058`)
* Fix failure when the file path contains `#` in GCS. ({issue}`28292`)

## Iceberg connector

* Add support for the BigLake metastore in Iceberg REST catalog. ({issue}`26219`)
* Add `delete_after_commit_enabled` and `max_previous_versions` table properties. ({issue}`14128`)
* Add support for column default values in Iceberg v3 tables. ({issue}`27837`)
* Add support for creating, writing to or deleting from Iceberg v3 tables. ({issue}`27786`, {issue}`27788`)
* Add `content` column to `$manifests` and `$all_manifests` metadata tables. ({issue}`27975`)
* Add support for changing `map` and `array` nested types through `ALTER ... SET DATA TYPE`. ({issue}`27998`)
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
* Improve `optimize` to clean up partition scoped equality delete files when a partition filter is used. ({issue}`28371`)
* Enhance `optimize_manifests` to cluster manifests by partition, improving read
  performance for queries that apply partition filters. ({issue}`27358`)
* Reduce planning time of queries on tables containing delete files. ({issue}`27955`)
* Reduce planning time for queries involving simple `FROM` and `WHERE` clauses. ({issue}`27973`)
* Reduce query planning time on large tables. ({issue}`28068`)
* Fix failure when reading `$files` metadata tables when
  scheme involving `bucket` or `truncate` changes. ({issue}`26109`)
* Fix failure when reading `$file_modified_time` metadata column on tables with equality
  deletes. ({issue}`27850`)
* Avoid large footers in Parquet files from certain rare string inputs. ({issue}`27903`)
* Fix failures for queries with joins on metadata columns. ({issue}`27984`)
* Fix Azure Storage connectivity issues. ({issue}`28058`)
* Fix incorrect memory accounting for `INSERT` queries targeting bucketed and sorted tables. ({issue}`28315`)
* Fix an issue where using `ALTER TABLE ... SET PROPERTIES` to set partition spec
  unintentionally removed existing partition columns from the partition spec. ({issue}`26492`)
* Fix failures when reading from tables with `write.parquet.compression-codec` property set to `LZ4`. ({issue}`28291`)
* Fix value of `compression-codec` table property written by Trino to be compliant with Iceberg spec. ({issue}`28293`)
* Fix failure when the file path contains `#` in GCS. ({issue}`28292`)
* Fix failure when reading tables with `iceberg.jdbc-catalog.schema-version=V0`. ({issue}`28419`)
* Avoid worker crashes when reading from tables with a larger number of equality deletes. ({issue}`28468`)

## Ignite connector

* Fix failure when creating a table if a prior `CREATE TABLE ... AS SELECT`
  operation for the same table failed. ({issue}`27702`)

## Lakehouse connector

* Improved performance and memory usage when [Equality Delete](https://iceberg.apache.org/spec/#equality-delete-files) files are used ({issue}`28507`)
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

* Add support for reading PostgreSQL `NUMERIC(p, s)` columns which cannot be mapped to Trino `decimal`.
  These columns are mapped to Trino `number` type. ({issue}`28141`)
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
