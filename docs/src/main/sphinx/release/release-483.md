# Release 483 (17 Jul 2026)

## General

* Add support for the {doc}`/sql/pivot` clause to turn row values into columns.
  ({issue}`29404`)
* Add support for reading values from a `json` column using the
  {ref}`JSON simplified accessor <json-simplified-accessor>` syntax, with dotted
  and subscripted notation such as `SELECT j.customer.name FROM orders` or
  `SELECT j.items[0].price.decimal(18,2) FROM orders`. ({issue}`29565`)
* Add support for `SELECT j.*` over a `json` column to return its top-level
  members as a single JSON array. ({issue}`29565`)
* Add support for the `OVERLAPS` predicate to test whether two time periods share
  any instant. See {doc}`/functions/datetime`. ({issue}`29652`)
* Add support for the `datetime()` method in SQL/JSON path expressions.
  ({issue}`29603`)
* Add the {func}`title_case` function. ({issue}`2942`)
* Add the {func}`ST_Transform` and {func}`ST_TransformXY` functions for
  transforming `geometry` values between coordinate reference systems.
  ({issue}`29737`)
* Add support for constructing and inspecting three-dimensional points with the
  new three- and four-argument {func}`ST_Point` overloads and the {func}`ST_Z`,
  {func}`ST_Force2D`, and {func}`ST_Force3D` functions. ({issue}`29737`)
* Add the {func}`ST_GeomFromEWKT` function for parsing extended well-known text.
  ({issue}`29737`)
* Add the {func}`ST_PointOnSurface`, {func}`ST_MakePolygon`, {func}`ST_Multi`,
  {func}`ST_LineMerge`, {func}`ST_Normalize`, {func}`ST_ReducePrecision`,
  {func}`ST_VoronoiPolygons`, {func}`ST_MinimumBoundingCircle`, and
  {func}`ST_OrientedEnvelope` functions. ({issue}`29737`)
* Add the {func}`ST_Collect`, {func}`ST_MakeLine`, and {func}`ST_Polygonize`
  functions and the {func}`geometry_collect_agg` aggregate function for combining
  `geometry` values. ({issue}`29737`)
* Add support for tuning the HTTP connection pool used for Azure exchange
  spooling storage with the `exchange.azure.max-connections`,
  `exchange.azure.pending-acquire-max-count`, and
  `exchange.azure.connection-acquisition-timeout` configuration properties.
  ({issue}`30083`)
* Preserve `geometry` SRID and Z-coordinate metadata across serialization, format
  conversions, and supported geospatial operations. ({issue}`29737`)
* Improve performance of queries with a `BETWEEN` predicate that compares a cast
  expression against a range of literals, such as `CAST(ts AS date) BETWEEN DATE
  '2020-01-01' AND DATE '2020-01-02'`. ({issue}`14648`)
* Improve performance of joins whose condition contains an equality shared by
  every branch of an `IF` or `CASE` expression, such as
  `IF(k IS NULL, a = b, a = b AND c = d)`. ({issue}`30288`)
* Reduce worker memory usage for queries that use
  [dynamic filtering](/admin/dynamic-filtering) on `bigint`, `integer`, or `date`
  join keys. ({issue}`30321`)
* Fix incorrect results for `NOT BETWEEN` predicates that compare a cast
  expression against a range of literals. ({issue}`30073`)
* Fix potential incorrect results when comparing a non-deterministic expression
  cast to `date` against a `date` literal, such as
  `CAST(f() AS date) = DATE '2020-01-01'`. ({issue}`30286`)
* Fix incorrect results and `MERGE` failures for queries containing a right or
  full outer join. ({issue}`30278`)
* Fix query failure when a filter contains a row constructor with more than 64
  fields referencing input columns. ({issue}`30279`)
* Fix query failure when spooling query results that contain rows with highly
  variable sizes. ({issue}`30112`)
* Fix query failure during fault-tolerant execution when processing small data
  pages. ({issue}`23531`)
* Fix query failure caused by an optimizer timeout when comparing a cast or a
  date and time function against a literal. ({issue}`30347`)
* Fix failure of `SHOW SESSION` and similar metadata queries when a catalog fails
  to load. ({issue}`30086`)
* Fix query failure when calling {func}`theta_sketch_union` with explicit nominal
  entries and seed arguments. ({issue}`29725`)

## Security

* Add support for restricting OAuth 2.0 single sign-on to accounts from a specific
  domain, configurable with the `http-server.authentication.oauth2.domain-hint`
  configuration property. ({issue}`30272`)

## Web UI

* Add support for sorting the query list by progress. ({issue}`30350`)
* {{breaking}} Make the redesigned Web UI, previously served at `/ui/preview`, the
  default at `/ui`, and move the previous Web UI to `/ui/legacy`. The previous Web
  UI is deactivated by default and can be restored by setting
  `web-ui.legacy.enabled` to `true`. ({issue}`22697`)
* Fix query state filter checkboxes not toggling when clicking the item text in
  the preview UI. ({issue}`30078`)

## JDBC driver

* Prevent JDBC queries from hanging indefinitely when an error occurs while
  reading query results. ({issue}`30007`)

## CLI

* Add support for themed, colorized output in the CLI with the new `--theme`
  command-line option. ({issue}`6190`, {issue}`30265`)

## Delta Lake connector

* Add support for configuring the S3 authentication type with the `s3.auth-type`
  configuration property. Use `ANONYMOUS` for reading public buckets.
  ({issue}`27512`)
* {{breaking}} Require `s3.auth-type=IAM_ROLE` when `s3.iam-role` is set.
  ({issue}`27512`)
* {{breaking}} Remove the `s3.use-web-identity-token-credentials-provider`
  configuration property. Use `s3.auth-type=WEB_IDENTITY` instead. ({issue}`27512`)
* Reduce memory usage when writing Parquet files. ({issue}`30372`)
* Prevent orphaned deletion vector files after a failed write. ({issue}`30342`)
* Prevent deletion vectors written by Trino from being rejected by other Delta
  Lake readers. ({issue}`30355`)

## Druid connector

* Add support for canceling Druid queries that exceed a configurable timeout, set
  with the `druid.execution-timeout` configuration property. ({issue}`27150`)
* Improve performance of queries with complex filters by pushing more expressions
  down to Druid. ({issue}`4109`)

## Hive connector

* Add support for configuring the S3 authentication type with the `s3.auth-type`
  configuration property. Use `ANONYMOUS` for reading public buckets.
  ({issue}`27512`)
* {{breaking}} Require `s3.auth-type=IAM_ROLE` when `s3.iam-role` is set.
  ({issue}`27512`)
* {{breaking}} Remove the `s3.use-web-identity-token-credentials-provider`
  configuration property. Use `s3.auth-type=WEB_IDENTITY` instead. ({issue}`27512`)
* Improve performance of reading TEXTFILE and CSV data. ({issue}`30291`)
* Reduce memory usage when writing Parquet files. ({issue}`30372`)
* Fix incorrect results for queries with a filter on `char` columns in ORC files.
  ({issue}`30187`)

## Hudi connector

* Add support for limiting the maximum size of splits when reading Hudi tables
  with the `hudi.max-split-size` configuration property. ({issue}`29842`)
* Add support for configuring the S3 authentication type with the `s3.auth-type`
  configuration property. Use `ANONYMOUS` for reading public buckets.
  ({issue}`27512`)
* {{breaking}} Require `s3.auth-type=IAM_ROLE` when `s3.iam-role` is set.
  ({issue}`27512`)
* {{breaking}} Remove the `s3.use-web-identity-token-credentials-provider`
  configuration property. Use `s3.auth-type=WEB_IDENTITY` instead. ({issue}`27512`)
* Improve performance of reading Hudi tables. ({issue}`29842`)

## Iceberg connector

* Add support for reading Iceberg tables with encrypted Parquet files.
  ({issue}`28204`)
* Add support for querying metadata tables, such as `$files`, `$snapshots`, and
  `$partitions`, for materialized views. ({issue}`30218`)
* Add support for configuring the S3 authentication type with the `s3.auth-type`
  configuration property. Use `ANONYMOUS` for reading public buckets.
  ({issue}`27512`)
* Allow using Application Default Credentials with the Iceberg REST catalog by
  making the `gcs.json-key-file-path` configuration property optional when
  `iceberg.rest-catalog.security` is set to `GOOGLE`. ({issue}`29084`)
* {{breaking}} Require `s3.auth-type=IAM_ROLE` when `s3.iam-role` is set.
  ({issue}`27512`)
* {{breaking}} Remove the `s3.use-web-identity-token-credentials-provider`
  configuration property. Use `s3.auth-type=WEB_IDENTITY` instead. ({issue}`27512`)
* Reduce memory usage when writing position delete files in Iceberg tables.
  ({issue}`30081`)
* Reduce memory usage when writing Parquet files. ({issue}`30372`)
* Fix incorrect results for queries with a filter on `char` columns in ORC files.
  ({issue}`30187`)
* Prevent table corruption by failing `OPTIMIZE` when a `NOT NULL` column contains
  `NULL` values. ({issue}`30062`)
* Fix potential loss of table metadata when a `CREATE TABLE` commit to the Hive
  metastore catalog fails after the table was created. ({issue}`29973`)
* Fix incorrect listing of views from unrelated schemas when using an Iceberg
  JDBC catalog. ({issue}`29738`)
* Fix `CREATE TABLE` failure when using a catalog that generates the table
  location, such as an Iceberg REST catalog. ({issue}`26742`)
* Fix failure when setting a column comment containing characters rejected by the
  Glue catalog, such as newlines. ({issue}`30353`)
* Fix query failure when using an Iceberg REST catalog with SigV4 request signing.
  ({issue}`30163`)

## Ignite connector

* {{breaking}} Require the additional `--add-opens=java.base/java.util=ALL-UNNAMED`
  JVM configuration option when using the Ignite connector. ({issue}`30255`)

## Lakehouse connector

* Add support for reading Iceberg tables with encrypted Parquet files.
  ({issue}`28204`)
* Add support for configuring the S3 authentication type with the `s3.auth-type`
  configuration property. Use `ANONYMOUS` for reading public buckets.
  ({issue}`27512`)
* {{breaking}} Require `s3.auth-type=IAM_ROLE` when `s3.iam-role` is set.
  ({issue}`27512`)
* {{breaking}} Remove the `s3.use-web-identity-token-credentials-provider`
  configuration property. Use `s3.auth-type=WEB_IDENTITY` instead. ({issue}`27512`)
* Reduce memory usage when writing Parquet files. ({issue}`30372`)
* Fix incorrect results for queries with a filter on `char` columns in ORC files.
  ({issue}`30187`)

## SQL Server connector

* Fix query failure when reading a table with hidden columns using a `LIMIT`.
  ({issue}`30084`)

## SPI

* Consistently expose raw array and offset accessors on all block types.
  ({issue}`30128`)
* {{breaking}} Change the block null representation to bit-packed validity bitmaps
  instead of boolean null arrays. Plugins that construct blocks directly instead
  of through the block builder API must be updated. ({issue}`30185`)
