# Release 482 (25 Jun 2026)

## General

* Add support for the `AT LOCAL` operator. ({issue}`29644`)
* Add support for named arguments in function calls using the `name => value`
  syntax. ({issue}`29530`)
* Add support for casting `char` values to numeric, `boolean`, `varbinary`, and
  temporal types. ({issue}`29374`)
* Add support for the `MATCH` predicate. ({issue}`29810`)
* Add support for the `UNIQUE` predicate. ({issue}`29811`)
* Add support for the `SYMMETRIC` and `ASYMMETRIC` keywords in the `BETWEEN`
  predicate. ({issue}`29981`)
* Add support for predicates such as `<`, `BETWEEN`, and `IS NULL` in the `WHEN`
  clauses of a simple `CASE` expression. ({issue}`29446`)
* Add support for the `IS [NOT] TRUE`, `IS [NOT] FALSE`, and `IS [NOT] UNKNOWN`
  predicates. ({issue}`29998`)
* Add support for the `like_regex` predicate in SQL/JSON path expressions.
  ({issue}`29592`)
* Add the {func}`ends_with` function. ({issue}`29847`)
* Add support for the `OVERLAY` string function. ({issue}`30026`)
* Add the `ROW::fields` function to return the field names of a `row` value.
  ({issue}`29425`)
* Add support for invoking string functions as methods on `varchar` and `char`
  values, such as `value.length()` and `varchar::chr(65)`. ({issue}`29978`)
* Allow `INSERT`, `UPDATE`, and `MERGE` to assign values to `array`, `map`, and
  `row` columns containing nested character types when the value is assignable
  to the column type. ({issue}`29953`)
* Allow comparing and ordering `row` and `array` values that contain null
  elements, for example in `ORDER BY`, `DISTINCT`, {func}`min`, {func}`max`, and
  range comparisons. ({issue}`29891`)
* {{breaking}} Remove support for Alluxio-backed exchange storage for
  fault-tolerant execution. ({issue}`29596`)
* {{breaking}} Reverse the implicit coercion between `char` and `varchar`: a
  `char` value coerces to `varchar` with its trailing spaces removed, and
  comparisons between `char` and `varchar` follow `varchar` semantics with no
  blank padding, so trailing spaces are significant. The previous behavior can be
  restored by setting the `deprecated.legacy-varchar-to-char-coercion`
  configuration property to `true`. ({issue}`29939`)
* Disallow `OMIT QUOTES` in `JSON_QUERY` when the returned type is `json`.
  ({issue}`29587`)
* Reduce memory usage that accumulates over time on long-running clusters that
  execute many queries with aggregations. ({issue}`29523`, {issue}`29524`)
* Improve performance of queries with a {func}`starts_with` filter by enabling
  predicate pushdown. ({issue}`29712`)
* Fix incorrect results for queries on `number` columns containing `NaN` values.
  ({issue}`29497`)
* Fix incorrect results when casting `decimal` values with precision smaller
  than 19 to `double`. ({issue}`29831`)
* Fix `DISTINCT` and `GROUP BY` treating two `NaN` values in `number` columns as
  distinct. ({issue}`29920`)
* Fix incorrect results for `DISTINCT`, `GROUP BY`, and `=` comparisons on
  `number` values whose magnitude exceeds the supported precision.
  ({issue}`29962`)
* Fix incorrect ordering, grouping, and aggregation of `row` values with more
  than 64 fields. ({issue}`30013`)
* Fix incorrect column lineage for recursive common table expressions with
  column aliases. ({issue}`29304`)
* Fix query failure when dereferencing a field of a `null` row value inside a
  lambda expression. ({issue}`29504`)
* Fix failure when executing a SQL user-defined function that declares a
  variable with a structural default value such as an array. ({issue}`29533`)
* Fix failure when a table function returns large pass-through columns.
  ({issue}`29561`)
* Fix incorrect query progress reported through the `/v1/query` endpoint and the
  Web UI. ({issue}`29793`)
* Fix query progress reported as `NaN` under fault-tolerant execution.
  ({issue}`29791`)
* Fix failure when serializing custom data types provided by connectors over the
  spooling protocol. ({issue}`29711`)
* Fix {func}`round` returning `NaN` for `double` and `real` values when rounding
  to a number of decimal places that causes underflow. ({issue}`29808`)
* Fix `DESCRIBE OUTPUT` returning the `number` type to clients that do not
  support it. ({issue}`29671`)

## Security

* Add a `User-Agent` header to JWKS requests used for JWT authentication and
  OAuth 2.0 token validation. ({issue}`29669`)

## Web UI

* Add buttons to the preview UI to cancel or preempt a running query.
  ({issue}`29315`)
* Reduce CPU usage of the preview UI query list page. ({issue}`29865`)
* Fix error in the preview UI splits timeline view for a failed query with no
  stages. ({issue}`29537`)

## JDBC driver

* Fix connection leak when retrieval of a spooled result segment fails.
  ({issue}`30048`)

## CLI

* Fix connection leak when retrieval of a spooled result segment fails.
  ({issue}`30048`)

## Cassandra connector

* Fix query failure when a predicate on a Cassandra table results in an empty
  `WHERE` clause. ({issue}`30027`)

## Delta Lake connector

* Add the table location to the `$properties` metadata table. ({issue}`29521`)
* Add support for limiting the number of rows in Parquet writer row groups with
  the `parquet.writer.block-row-count` configuration property or the
  `parquet_writer_block_row_count` session property. ({issue}`29673`)
* Disallow creating tables partitioned by `varbinary` columns. ({issue}`24155`)
* {{breaking}} Remove support for the Alluxio file system. This does not affect
  support for the Alluxio file system cache. ({issue}`29596`)
* Improve compression and read performance for high-cardinality string columns
  in Parquet files. This can be disabled by setting the
  `parquet.writer.delta-length-byte-array-encoding-enabled` configuration
  property to `false`. ({issue}`29246`)
* Improve performance of queries that access nested fields inside lambda
  expressions. ({issue}`29532`)
* Improve performance of reads from Google Cloud Storage when using static
  credentials. ({issue}`29776`)
* Fix data loss caused by cleanup of a failed write deleting active data files
  when deletion vectors are enabled. ({issue}`29361`)
* Fix failure when accessing Azure Storage with a hierarchical namespace using
  vended credentials scoped to a subdirectory. ({issue}`29293`)
* Fix sporadic failures when reading data from S3. ({issue}`29419`)

## Druid connector

* Improve performance of queries involving aggregations by pushing the
  computation to Druid. ({issue}`29863`)

## Exasol connector

* Add support for the `timestamp` type. ({issue}`26963`)

## Hive connector

* Add support for limiting the number of rows in Parquet writer row groups with
  the `parquet.writer.block-row-count` configuration property or the
  `parquet_writer_block_row_count` session property. ({issue}`29673`)
* Add the `hive.parquet.max-split-size` configuration property to control the
  maximum size of splits for Parquet files, while `hive.max-split-size` applies
  only to other file formats. ({issue}`29698`)
* {{breaking}} Remove the `hive.max-initial-splits` and
  `hive.max-initial-split-size` configuration properties. ({issue}`29698`)
* {{breaking}} Remove support for the Alluxio file system. This does not affect
  support for the Alluxio file system cache. ({issue}`29596`)
* Improve compression and read performance for high-cardinality string columns
  in Parquet files. This can be disabled by setting the
  `parquet.writer.delta-length-byte-array-encoding-enabled` configuration
  property to `false`. ({issue}`29246`)
* Improve performance of queries that access nested fields inside lambda
  expressions. ({issue}`29532`)
* Improve performance of reads from Google Cloud Storage when using static
  credentials. ({issue}`29776`)
* Improve performance when a configured Hive metastore is unavailable by backing
  off before retrying the connection. ({issue}`29653`)
* Fix sporadic failures when reading data from S3. ({issue}`29419`)

## Hudi connector

* {{breaking}} Remove support for the Alluxio file system. This does not affect
  support for the Alluxio file system cache. ({issue}`29596`)
* Improve performance of reads from Google Cloud Storage when using static
  credentials. ({issue}`29776`)
* Fix sporadic failures when reading data from S3. ({issue}`29419`)

## Iceberg connector

* Allow configuring the `data_location` table property without enabling
  `object_store_layout_enabled`. ({issue}`28887`)
* Add support for limiting the number of rows in Parquet writer row groups with
  the `parquet.writer.block-row-count` configuration property or the
  `parquet_writer_block_row_count` session property. ({issue}`29673`)
* Add support for caching Parquet file footers, configurable with the
  `iceberg.parquet-footer-cache.type` configuration property. ({issue}`29684`)
* Add support for the `write.object-storage.partitioned-paths` table property.
  ({issue}`27633`)
* Add support for prefixed path storage credentials in the Iceberg REST
  catalog. ({issue}`29641`)
* Add support for reading and writing `geometry` and `geography` values in
  Iceberg v3 tables. ({issue}`27893`)
* Add support for the `location` property when creating views in the REST and
  JDBC catalogs. ({issue}`29724`)
* Add the `iceberg.max-split-size` configuration property and `max_split_size`
  session property to control the maximum size of splits. This replaces the
  experimental `experimental_split_size` session property. ({issue}`29894`)
* Add support for the `target_max_file_size` and `parquet_writer_row_group_size`
  table properties, persisted as the Iceberg-native `write.target-file-size-bytes`
  and `write.parquet.row-group-size-bytes` properties. ({issue}`28250`)
* {{breaking}} Change the `lower_bounds` and `upper_bounds` columns of the
  `$files` metadata table from `map(integer, varchar)` to typed `row` values.
  Queries using `lower_bounds[1]` or casting these columns to `json` must be
  updated. ({issue}`23147`)
* {{breaking}} Remove the `target_max_file_size` and `parquet_writer_row_group_size`
  session properties, including the deprecated `parquet_writer_block_size` alias.
  Use the equivalent table properties instead. ({issue}`28250`)
* {{breaking}} Remove support for the Alluxio file system. This does not affect
  support for the Alluxio file system cache. ({issue}`29596`)
* Improve performance of queries on the `$partitions` metadata table.
  ({issue}`23147`)
* Improve performance of queries on the `iceberg_tables` system table that
  filter the table name with an `IN` predicate. ({issue}`29964`)
* Improve compression and read performance for high-cardinality string columns
  in Parquet files. This can be disabled by setting the
  `parquet.writer.delta-length-byte-array-encoding-enabled` configuration
  property to `false`. ({issue}`29246`, {issue}`29616`)
* Improve performance of queries that access nested fields inside lambda
  expressions. ({issue}`29532`)
* Reduce memory usage when reading tables with many equality delete files.
  ({issue}`29643`)
* Improve performance of reads from Google Cloud Storage when using static
  credentials. ({issue}`29776`)
* Increase the maximum supported size of `variant` metadata from 16MB to 128MB.
  ({issue}`29423`)
* Fix loss of materialized view data when running `CREATE OR REPLACE
  MATERIALIZED VIEW` for a view with a fixed storage location. ({issue}`29481`)
* Fix loss of the custom storage location of a view when running `CREATE OR
  REPLACE VIEW` in the REST and JDBC catalogs. ({issue}`25425`, {issue}`29755`)
* Fix query failure when reading tables with delete files that reference nested
  fields. ({issue}`29763`)
* Fix failure when reading statistics for a column whose type was changed.
  ({issue}`29802`)
* Fix excessive token requests when listing nested namespaces in a REST catalog
  using OAuth 2.0 with `iceberg.rest-catalog.session` set to `NONE`.
  ({issue}`29722`)
* Fix creation of multiple delete files per data file when updating tables
  partitioned only with identity transforms. ({issue}`28276`)
* Fix failure when listing schemas in an Iceberg REST catalog and a nested
  namespace is dropped concurrently. ({issue}`29500`)
* Fix failure when listing columns or comments for tables in the Glue catalog
  when access to a table is denied. ({issue}`29995`)
* Fix failure when accessing Azure Storage with a hierarchical namespace using
  vended credentials scoped to a subdirectory. ({issue}`29293`)
* Fix sporadic failures when reading data from S3. ({issue}`29419`)

## Lakehouse connector

* Add support for limiting the number of rows in Parquet writer row groups with
  the `parquet.writer.block-row-count` configuration property or the
  `parquet_writer_block_row_count` session property. ({issue}`29673`)
* Improve performance of reads from Google Cloud Storage when using static
  credentials. ({issue}`29776`)
* Fix sporadic failures when reading data from S3. ({issue}`29419`)

## MySQL connector

* Fix incorrect results when filtering a `char` or `varchar` column with an
  equality or `IN` predicate where stored values differ only in trailing spaces.
  Inequality and range predicates on character columns are no longer pushed down.
  ({issue}`29952`, {issue}`29939`)

## Oracle connector

* Fix incorrect results when a `CAST` from `char` to `varchar` is pushed down to
  Oracle, which previously retained the blank padding instead of trimming it.
  ({issue}`29939`)

## PostgreSQL connector

* Add support for reading and writing the PostgreSQL `point` type as the Trino
  `geometry` type. ({issue}`29568`)
* Fix failure when reading tables with PostGIS `geometry` columns.
  ({issue}`29505`)

## Redis connector

* Add support for TLS connections to Redis, configurable with the `redis.tls.*`
  configuration properties. ({issue}`8091`)

## SQL Server connector

* {{breaking}} Fix incorrect results when filtering a `char` or `varchar` column
  with an equality or `IN` predicate where stored values differ only in trailing
  spaces. Inequality and range predicates on character columns are no longer
  pushed down, so a `DELETE` or `UPDATE` that relies on pushing down such a
  predicate fails. ({issue}`29952`, {issue}`29939`)

## TPC-DS connector

* Update the TPC-DS data generator to version 1.7, which changes some generated
  data and table statistics. ({issue}`30022`)

## SPI

* Add the `@StaticMethod` annotation to register a scalar function as a static
  method of a named type, invocable via `T::method(args)`. ({issue}`29385`)
* Add the `@InstanceMethod` annotation to register a scalar function as a method
  of its receiver type, invocable via `expr.method(args)`. ({issue}`29399`)
* Add the `@Name` annotation to declare names for the arguments of functions
  implemented in Java. ({issue}`29530`)
* Add support for lambda expressions in connector expression pushdown.
  ({issue}`29532`)
* Add support for an optional description on connector table functions.
  ({issue}`29830`)
* Add support for connector page sources to report memory usage through a
  `MemoryContext` provided by the engine. ({issue}`29843`)
* {{breaking}} Replace the `DynamicFilter` parameter of
  `ConnectorSplitManager.getSplits` with the set of dynamic filter columns, and
  pass the current predicate to `ConnectorSplitSource.getNextBatch` as a
  `DynamicFilterSnapshot`. ({issue}`29206`)
* {{breaking}} Remove the predicate from `Constraint` and add the
  `ConnectorExpressionEvaluator` interface for evaluating expressions during
  partition and split pruning. ({issue}`29289`)
* {{breaking}} Remove deprecated methods from `ConnectorPageSourceProvider` and
  `ConnectorPageSinkProvider`. ({issue}`29562`)
* {{breaking}} Remove a deprecated method from `TableFunctionProcessorProvider`.
  ({issue}`29618`)
