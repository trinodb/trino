# Release 484 (dd MMM 2026)

## General

* Add support for a file system cache shared by all catalogs, configured with
  the `cache-manager.config-files` configuration property. ({issue}`29184`)
* Suggest similar column names in the error message when a column cannot be
  resolved. ({issue}`18649`)
* Add support for running table procedures on materialized views with
  {doc}`/sql/alter-materialized-view` `EXECUTE`. ({issue}`30546`)
* Add support for returning execution metrics from procedures invoked with
  {doc}`/sql/call`. ({issue}`30749`)
* Add the `legacy_varchar_to_char_coercion` session property to restore the
  previous implicit coercion from `varchar` to `char`. ({issue}`30708`)
* Allow a `NULL` value in the `environment` column of database resource groups
  and selectors to match all environments. ({issue}`30873`)
* Add support for limiting the size of output data produced by a query with the
  `query.max-output-data-size` configuration property. ({issue}`30615`)
* Add support for the `EXCLUDE` clause in window frames. ({issue}`30226`)
* Add support for the `COMMENT ON MATERIALIZED VIEW` statement. ({issue}`31279`)
* Set the Linux process name of the Trino server to `trino-server`.
  ({issue}`30403`)
* {{breaking}} Preserve the original case of user and role names in `GRANT`,
  `REVOKE`, and `SET AUTHORIZATION` statements and in table and schema owners.
  ({issue}`30567`)
* Report connector failures while listing schemas, tables, or columns as
  external errors instead of internal errors. ({issue}`31206`)
* Report an `AMBIGUOUS_COLUMN_NAME` external error instead of an internal error
  when a connector returns columns with names that differ only in case.
  ({issue}`31401`)
* Fail queries with fault-tolerant execution immediately when spooled exchange
  data is lost. ({issue}`31336`)
* Limit the `optimizer.max-reordered-joins` configuration property and the
  `max_reordered_joins` session property to a maximum value of `62`.
  ({issue}`31261`)
* Improve performance of queries using `boolean` values. ({issue}`30299`)
* Improve performance of queries that compare the result of {func}`at_timezone`
  with a constant time zone against a value. ({issue}`30597`)
* Reduce memory usage of queries that compute both {func}`avg` and {func}`sum`
  of the same `decimal` column. ({issue}`30595`)
* Improve performance of queries with filters on `date_trunc` with the `week` or
  `quarter` unit. ({issue}`30192`)
* Improve performance of {func}`sum` over `decimal` values in window functions
  with a sliding window frame. ({issue}`30630`)
* Improve performance of queries with uncorrelated `EXISTS` subqueries.
  ({issue}`30224`)
* Improve performance of filters and projections on dictionary-encoded data.
  ({issue}`30877`)
* Improve performance of queries with filters that combine conditions with `AND`
  by reading only the columns needed to evaluate them. ({issue}`30909`)
* Improve performance of workers processing many concurrent splits.
  ({issue}`30961`)
* Improve performance of cross joins with an input that uses a `LIMIT` clause.
  ({issue}`31128`)
* Improve performance of queries that compare a cast between `char` and
  `varchar` with a constant. ({issue}`31089`)
* Improve performance of queries with an `IN` predicate that compares a cast
  expression against a list of literals, such as `CAST(ts AS date) IN (DATE
  '2020-01-01', DATE '2020-01-02')`. ({issue}`31182`)
* Improve performance of filters on columns with large dictionaries.
  ({issue}`31200`)
* Improve performance of `ORDER BY` queries with the `QUERY` retry policy.
  ({issue}`30966`)
* Improve performance of queries that repeat the same shape with different
  literal values by reusing generated code. ({issue}`30465`)
* Reduce worker memory usage for queries with many operators. ({issue}`31301`)
* Reduce worker memory usage for queries that use [dynamic
  filtering](/admin/dynamic-filtering) on join keys of types such as `varchar`.
  ({issue}`31303`)
* Improve performance of queries that filter on {func}`at_timezone` over
  `timestamp with time zone` columns by enabling partition pruning.
  ({issue}`30551`)
* Improve query planning performance for queries with many joins.
  ({issue}`31261`)
* Fix `Invalid function name` failure when applying an item method, such as
  `integer()`, after two or more nested members in a JSON simplified accessor.
  ({issue}`30379`)
* Fix query failure when using {func}`try` with `NULLIF` or `BETWEEN`
  expressions. ({issue}`30399`)
* Fix rare query failure with fault-tolerant execution. ({issue}`30427`)
* Fix incorrect results for `CASE` and `IF` expressions with a repeated
  non-deterministic condition in a single branch. ({issue}`30509`)
* Fix internal error instead of a `QUERY_EXCEEDED_COMPILER_LIMIT` or
  `COMPILER_ERROR` failure when a join is too complex to compile.
  ({issue}`30529`)
* Fix incorrect results when {func}`sum` over `bigint` values overflows in a
  window function. The query now fails instead. ({issue}`30600`)
* Fix incorrect results for `time AT TIME ZONE` with an `interval` offset
  outside the `[-14:00, 14:00]` range. Such offsets now cause a query failure.
  ({issue}`9288`)
* Fix potential worker unavailability under high exchange concurrency.
  ({issue}`30627`)
* Fix incorrect results for joins on a `char` column cast to `varchar`.
  ({issue}`30686`)
* Fix worker out-of-memory failures caused by untracked memory usage of right
  and full outer joins. ({issue}`30790`)
* Prevent queries that need no memory, such as metadata queries, from being
  blocked on a cluster with exhausted memory when using fault-tolerant
  execution. ({issue}`30678`)
* Fix incorrect results when casting some short `decimal` values to `real`.
  ({issue}`30130`)
* Fix failure when using `USE` in a prepared statement. ({issue}`30859`)
* Prevent planning failures for queries with a large number of `OR` predicates.
  ({issue}`30709`)
* Fix query information being retained indefinitely and query completion events
  not being emitted when a worker restarts while the query is running.
  ({issue}`30755`)
* Fix query failure when a `BETWEEN` predicate is applied to an expression in a
  query with more than one `DISTINCT` aggregation. ({issue}`30608`)
* Fix excessive data reads for joins with filters that may fail when the
  `optimizer.allow-unsafe-pushdown` configuration property is enabled.
  ({issue}`30917`)
* Fix intermittent query failure with `No committed attempts found under sink
  output path` during fault-tolerant execution. ({issue}`30411`)
* Fix internal error when geometry functions such as {func}`ST_Union` receive an
  invalid geometry. Set the `trino.geospatial.legacy-lenient-overlay` JVM system
  property to `true` to repair invalid inputs as in earlier releases.
  ({issue}`30584`)
* Fix incorrect results or query failure due to overflow when computing
  {func}`avg` of high-precision `decimal` values. ({issue}`30991`)
* Prevent worker out-of-memory errors for queries using `row_number` or `rank`
  with a limit on wide rows. ({issue}`31014`)
* Fix {func}`date_add` and addition of intervals to `timestamp` values returning
  an incorrect result instead of failing when the result is out of range.
  ({issue}`31002`)
* Fix incorrect results when subtracting `timestamp` values with a very large
  difference. ({issue}`31002`)
* Fix incorrect results for aggregations over outer joins without equality join
  conditions when the inner side is empty. ({issue}`30989`)
* Fix query failure when `WITH SESSION` overrides a catalog session property
  that is already set in the session. ({issue}`31045`)
* Fix {func}`try` not returning `NULL` when {func}`mod` is called with a zero
  divisor. ({issue}`31054`)
* Fix failure of SQL user-defined functions that end in an `IF` or `CASE`
  statement returning a value from every branch. ({issue}`30582`)
* Fix incorrect results for filters with `IF`, `CASE`, or `NULLIF` expressions
  that contain a non-deterministic condition. ({issue}`30993`)
* Fix incorrect results for `IN` predicates with `real` `NaN` values, or with
  `time with time zone` and `timestamp with time zone` values in different time
  zones. ({issue}`31069`, {issue}`31080`)
* Prevent worker out-of-memory errors for ordered aggregations, such as
  `array_agg(DISTINCT x ORDER BY x)`. ({issue}`31039`)
* Prevent worker out-of-memory errors when partitioning output of wide rows that
  use dictionary encoding. ({issue}`31056`)
* Fix workers becoming unresponsive under load when using the thread-per-driver
  task executor. ({issue}`21512`)
* Fix `MERGE` failing with a false `MERGE_TARGET_ROW_MULTIPLE_MATCHES` error
  when the join is colocated with the partitioning of the target table, such as
  with bucketed Iceberg tables. ({issue}`30639`)
* Fix incorrect results for aggregations over non-deterministic `CASE`
  expressions. ({issue}`31121`)
* Fix incorrect results for comparisons of {func}`year` or {func}`date_trunc`
  with a non-deterministic argument. ({issue}`31086`)
* Fix incorrect results for `IS NOT DISTINCT FROM` predicates that compare a
  cast expression with a literal. ({issue}`31084`)
* Fix incorrect results for `CASE` expressions that compare the operand with
  itself, or a constant operand with an equal constant. ({issue}`31065`,
  {issue}`31081`)
* Fix `system.jdbc.columns` reporting the `JAVA_OBJECT` data type instead of
  `OTHER` for `number` columns. ({issue}`31184`)
* Fix incorrect results for `OR` and `IN` predicates with non-deterministic
  expressions. ({issue}`31149`)
* Fix failure of recursive queries with `WITH RECURSIVE` when the connector
  plans the base relation as a table function. ({issue}`31053`)
* Fix {func}`try` not suppressing errors raised in the base of a row field
  reference, such as an out-of-bounds array subscript in
  `TRY(array[index].field)`. ({issue}`31008`)
* Fix incorrect results for queries with multiple distinct aggregations over a
  non-deterministic filter or projection. ({issue}`31151`)
* Fix query failure or incorrect results when comparing a `varchar` value cast
  to `char` with a constant. ({issue}`31187`, {issue}`31189`)
* Fix incorrect results when casting a `char` value to a shorter `char` type or
  using {func}`reverse` with `char` values. ({issue}`31190`)
* Fix incorrect results for `IN` and `NOT IN` predicates with a list of
  consecutive values that includes `NULL`. ({issue}`31068`)
* Fix incorrect results for some `FULL OUTER JOIN` queries where an input has a
  single row and no rows match the join condition. ({issue}`30988`)
* Fix {func}`trim`, {func}`ltrim`, and {func}`rtrim` removing trailing spaces
  that are part of a `char` value when trimming a custom set of characters.
  ({issue}`31004`)
* Fix worker memory leak when flushing task output fails, such as when a task
  with fault-tolerant execution is aborted. ({issue}`31334`)
* Fix incorrect results or failure when casting a `timestamp` value before 1970
  with fractional seconds to `time`. ({issue}`31344`)
* Fix {func}`hamming_distance` hanging or failing when an input string contains
  a NUL (`U+0000`) character. ({issue}`31247`)
* Fix incorrect results and query failure for {func}`max_by` and {func}`min_by`
  with a count argument and `NULL` values. ({issue}`31193`)
* Fix missing rows with a `NULL` grouping key in queries with multiple
  `DISTINCT` aggregations when the `split_to_subqueries` distinct aggregation
  strategy is used. ({issue}`29095`)
* Fix incorrect results when filtering on a simple `CASE` expression with a
  `NULL` operand and a predicate in a `WHEN` clause. ({issue}`30992`)

## Security

* Add support for the `{user}` placeholder in the catalog, schema, and table
  fields of file-based access control rules. ({issue}`31205`)
* Add support for `COMMENT ON MATERIALIZED VIEW` to file-based, OPA, and Ranger
  access control. ({issue}`31279`)
* Show only the denied columns in Ranger access control error messages.
  ({issue}`30435`)
* Show only the denied columns in file-based access control error messages.
  ({issue}`30479`)
* Improve performance of column access checks with Ranger access control.
  ({issue}`30493`)
* Fix authorization failures in Ranger access control when using Kerberos
  authentication. ({issue}`29342`)
* Fix access denied failures for session property defaults set by the session
  property manager when the user is not allowed to set the property.
  ({issue}`31281`)

## Web UI

* Add support for worker thread snapshots. ({issue}`30388`)
* Use the full screen width for the query list. ({issue}`30352`)
* Fix sorting of finished and failed queries by progress. ({issue}`30528`)
* Fix overlapping text in the collapsed navigation menu. ({issue}`31232`)
* Fix failure to load the query details page when worker addresses are IPv6
  addresses with a zone ID. ({issue}`30905`)
* Fix missing query progress in the query list for queries with `retry-policy`
  set to `TASK` until all stages are scheduled. ({issue}`31336`)

## JDBC driver

* Throw `SQLException` instead of `IllegalArgumentException` when calling
  `getTime()` or `getTimestamp()` on a column of an incompatible type.
  ({issue}`5315`)
* Improve performance of reading query results. ({issue}`30375`)

## Docker image

* {{breaking}} Remove the ppc64le Docker image. ({issue}`30422`)
* Use a Red Hat Hardened Image as the base image. ({issue}`30397`)
* {{breaking}} Remove the `run-trino` script. Use `/usr/lib/trino/bin/launcher
  run --etc-dir /etc/trino` to start Trino with a custom command.
  ({issue}`30397`)
* Configure a `memory` cache manager in the default configuration.
  ({issue}`29184`)

## CLI

* Improve performance of reading query results. ({issue}`30375`)
* Fix the pager requiring `q` to be pressed twice to exit after a query shows
  progress updates. ({issue}`30965`)

## BigQuery connector

* Add support for the `location` schema property. ({issue}`30366`)
* Add support for retrying writes that exceed BigQuery quotas, configurable with
  the `bigquery.write-retry-max-attempts`, `bigquery.write-retry-initial-delay`,
  and `bigquery.write-retry-max-delay` configuration properties.
  ({issue}`30927`)
* Fix query failures caused by read timeouts by restoring the default BigQuery
  read timeout of 60 seconds. ({issue}`30840`)
* Fix failure when a table or dataset with a name that differs only in case was
  dropped or renamed and `bigquery.case-insensitive-name-matching` is enabled.
  ({issue}`31332`)

## Blackhole connector

## Cassandra connector

## ClickHouse connector

* Fix duplicated rows when inserting into `Distributed` tables and failure when
  inserting into replicated tables. ({issue}`7600`, {issue}`7601`)
* Prevent temporary tables from being left behind when a write query is canceled
  or fails while it starts. ({issue}`31333`)

## Delta Lake connector

* Add support for disabling the HTTP `Expect: 100-continue` handshake for S3
  requests with the `s3.expect-continue-enabled` configuration property.
  ({issue}`30534`)
* Add support for writing data files under hash-based directory prefixes with
  the `delta.object-store-layout.enabled` configuration property and the
  `object_store_layout_enabled` table property. ({issue}`24199`)
* Add support for `VACUUM` on tables with deletion vectors. ({issue}`22809`)
* {{breaking}} Move the `fs.cache.*` configuration properties, except
  `fs.cache.enabled`, from the catalog properties file to an `alluxio` cache
  manager configuration file. ({issue}`29184`)
* Skip writing min and max statistics for `real` and `double` columns with NaN
  values in Parquet files. ({issue}`30906`)
* Disable page pruning for `real` and `double` columns in Parquet files written
  without NaN counts. ({issue}`30906`)
* Disallow dropping UniForm tables. ({issue}`31083`)
* Improve performance of reading and writing `boolean` columns in Parquet files.
  ({issue}`30299`)
* Improve performance of writing Parquet files. ({issue}`30502`)
* Improve performance of writes when `retry-policy` is set to `TASK`. Files
  left behind by failed task attempts are removed by the `vacuum` procedure.
  ({issue}`31408`)
* Improve performance of queries with filters on `date_trunc` with the `week` or
  `quarter` unit on `timestamp with time zone` columns. ({issue}`30192`)
* Improve accuracy of memory usage tracking for `MERGE`, `UPDATE`, and `DELETE`
  statements. ({issue}`29956`)
* Improve performance of reading tables with deletion vectors and of `MERGE`,
  `UPDATE`, and `DELETE` statements when Parquet column indexes are enabled.
  ({issue}`30976`)
* Improve performance of selective queries on Parquet files, configurable with
  the `parquet.selected-positions-pushdown-enabled` configuration property.
  ({issue}`30303`)
* Reduce memory usage when reading `row` columns from Parquet files.
  ({issue}`31381`)
* Reduce coordinator memory usage when writing checkpoints for tables with many
  files. ({issue}`31354`)
* Fix incorrect results for `FOR TIMESTAMP AS OF` queries that could return data
  from an earlier table version. ({issue}`31057`)
* Fix incorrect results after `DELETE`, `CREATE OR REPLACE TABLE`, or `CREATE OR
  REPLACE TABLE AS` on tables with deletion vectors. ({issue}`30343`)
* Fix writing Parquet files smaller than the target file size for
  low-cardinality data. ({issue}`31291`)
* Fix rows silently lost from tables with deletion vectors written by other
  engines when Trino writes a checkpoint. ({issue}`31252`)
* Fix failure when writing checkpoints for tables with statistics that contain
  `Infinity` or `NaN` values, decimal values stored as JSON numbers, or
  timestamps without a zone offset. ({issue}`24029`, {issue}`28532`)
* Fix rows silently lost when Trino writes checkpoints for tables with deletion
  vectors that another engine restored to an earlier version. ({issue}`30985`)
* Fix duplicate rows returned by other Delta Lake readers after `OPTIMIZE` on
  tables with deletion vectors. ({issue}`31253`)

## Druid connector

* Improve performance of queries with `ORDER BY` on the `__time` column and
  `LIMIT` by pushing them down to Druid. ({issue}`31277`)

## DuckDB connector

* Prevent temporary tables from being left behind when a write query is canceled
  or fails while it starts. ({issue}`31333`)

## Elasticsearch connector

* Fix failure when listing columns with `information_schema.columns` if an index
  contains unsupported metadata. ({issue}`30852`)

## Exasol connector

## Faker connector

* Fix the `min` and `max` column properties being ignored for `timestamp` and
  `timestamp with time zone` columns with a precision higher than microseconds.
  ({issue}`31037`)

## Google Sheets connector

## Hive connector

* Add support for disabling the HTTP `Expect: 100-continue` handshake for S3
  requests with the `s3.expect-continue-enabled` configuration property.
  ({issue}`30534`)
* Add the `last_column_takes_rest` table property to store the remaining fields
  of a delimited line in the last column. ({issue}`30938`)
* {{breaking}} Move the `fs.cache.*` configuration properties, except
  `fs.cache.enabled`, from the catalog properties file to an `alluxio` cache
  manager configuration file. ({issue}`29184`)
* {{breaking}} Change the default value of the `hive.storage-format`
  configuration property to `PARQUET`. Set the property to `ORC` to continue
  creating new tables in ORC format by default. ({issue}`30818`)
* Skip writing min and max statistics for `real` and `double` columns with NaN
  values in Parquet files. ({issue}`30906`)
* Disable page pruning for `real` and `double` columns in Parquet files written
  without NaN counts. ({issue}`30906`)
* Return values of the `$file_modified_time` hidden column in the UTC time zone
  instead of the JVM time zone. ({issue}`31239`)
* Improve performance of reading and writing `boolean` columns in ORC and
  Parquet files. ({issue}`30299`)
* Improve performance of writing Parquet files. ({issue}`30502`)
* Improve performance of selective queries on Parquet files, configurable with
  the `parquet.selected-positions-pushdown-enabled` configuration property.
  ({issue}`30303`)
* Reduce memory usage when a write to a sorted table fails. ({issue}`31018`)
* Improve performance of writes into many partitions by updating partition
  statistics in bulk. ({issue}`31020`)
* Reduce memory usage when reading `row` columns from Parquet files.
  ({issue}`31381`)
* Fix worker out-of-memory failures when writing ORC files with many open
  writers, such as when writing to many partitions. ({issue}`30771`)
* Fix failure when reading Avro tables from AWS Glue that define their schema
  with `avro.schema.url` or `avro.schema.literal` and whose stored columns
  differ from the Avro schema. ({issue}`30599`)
* Fix loss of column statistics and slow rollback when a write into existing
  partitions fails during commit. ({issue}`31020`)
* Fix `Seek past end of stream` failure when reading ORC files or writing to
  sorted tables. ({issue}`10113`, {issue}`20164`, {issue}`28636`)
* Fix writing Parquet files smaller than the target file size for
  low-cardinality data. ({issue}`31291`)
* Fix incorrect results when filtering on Parquet `timestamp` columns written
  with `isAdjustedToUTC=true` and `hive.parquet.time-zone` set to a zone other
  than UTC. ({issue}`31050`)

## Hudi connector

* Add support for disabling the HTTP `Expect: 100-continue` handshake for S3
  requests with the `s3.expect-continue-enabled` configuration property.
  ({issue}`30534`)
* Disable page pruning for `real` and `double` columns in Parquet files written
  without NaN counts. ({issue}`30906`)
* Improve performance of reading `boolean` columns in Parquet files.
  ({issue}`30299`)
* Improve performance of selective queries on Parquet files, configurable with
  the `parquet.selected-positions-pushdown-enabled` configuration property.
  ({issue}`30303`)
* Reduce memory usage when reading `row` columns from Parquet files.
  ({issue}`31381`)
* Fix failure when reading tables with pending clustering operations.
  ({issue}`30855`)
* Fix incorrect results when filtering on Parquet `timestamp` columns written
  with `isAdjustedToUTC=true` when the JVM time zone is not UTC.
  ({issue}`31050`)

## Iceberg connector

* Add support for configuring access to AWS KMS for table encryption with the
  `aws.kms.*` configuration properties. ({issue}`30371`)
* Add support for disabling the HTTP `Expect: 100-continue` handshake for S3
  requests with the `s3.expect-continue-enabled` configuration property.
  ({issue}`30534`)
* Return execution metrics while running the `migrate` procedure.
  ({issue}`30749`)
* Return the number of removed statistics files when running the
  `drop_extended_stats` command. ({issue}`30768`)
* Add support for SigV4 authentication with the AWS default credentials provider
  chain when using the Iceberg REST catalog. ({issue}`25257`)
* Add support for the `gcs.json-key` configuration property for Google
  authentication with the Iceberg REST catalog. ({issue}`30770`)
* Add the `iceberg.rest-catalog.case-insensitive-name-matching.cache-max-size`
  configuration property to limit the size of the case-insensitive name mapping
  caches for the Iceberg REST catalog. ({issue}`30856`)
* Add support for sorting by nested `row` fields with the `sorted_by` table
  property. ({issue}`19620`)
* Add support for `ALTER VIEW ... REFRESH`. ({issue}`30954`)
* Add support for Parquet column indexes, configurable with the
  `parquet.use-column-index` configuration property or the
  `parquet_use_column_index` catalog session property. ({issue}`11000`)
* Add support for configuring the number of retries for requests to the Iceberg
  REST catalog with the `iceberg.rest-catalog.max-retries` configuration
  property. ({issue}`31073`)
* Add support for disabling metrics reporting to the Iceberg REST catalog with
  the `iceberg.rest-catalog.metrics-reporting-enabled` configuration property.
  ({issue}`31075`)
* Add support for running table procedures on materialized views with
  {doc}`/sql/alter-materialized-view` `EXECUTE`. ({issue}`30936`)
* Add the `iceberg.domain-compaction-threshold` configuration property to
  control the compaction of predicates pushed into the Parquet and ORC readers.
  ({issue}`31175`)
* Add support for registering tables with metadata files outside the default
  metadata directory with the `metadata_location` parameter of the
  `register_table` procedure. ({issue}`31164`)
* Add support for the `COMMENT ON MATERIALIZED VIEW` statement. ({issue}`31279`)
* Add the `gc_enabled` table property to control whether snapshot expiration,
  orphan file removal, and `DROP TABLE` delete data files. ({issue}`31218`)
* {{breaking}} Move the `fs.cache.*` configuration properties, except
  `fs.cache.enabled`, from the catalog properties file to an `alluxio` cache
  manager configuration file. ({issue}`29184`)
* {{breaking}} Remove the `fs.memory-cache.*` configuration properties. Use a
  `memory` cache manager instead. ({issue}`29184`)
* Skip writing min and max statistics for `real` and `double` columns with NaN
  values in Parquet files. ({issue}`30906`)
* Disable page pruning for `real` and `double` columns in Parquet files written
  without NaN counts. ({issue}`30906`)
* Fail at catalog startup when `iceberg.rest-catalog.vended-credentials-enabled`
  is set to `true` without native file system support for the storage.
  ({issue}`30634`)
* {{breaking}} Rename the `gcs.json-key-file-path` configuration property for
  Iceberg REST catalog authentication with `GOOGLE` security to
  `iceberg.rest-catalog.google-json-key-file-path`. ({issue}`31240`)
* {{breaking}} Remove the `iceberg.equality-deletes-blocks-hash-enabled`
  configuration property. ({issue}`31286`)
* {{breaking}} Return `timestamp with time zone` values in UTC instead of the
  session time zone in the `$snapshots`, `$history`, and `$metadata_log_entries`
  system tables. ({issue}`31356`)
* Improve performance of reading and writing `boolean` columns in ORC and
  Parquet files. ({issue}`30299`)
* Reduce coordinator memory usage when writing a large number of files.
  ({issue}`30433`)
* Improve performance of writing Parquet files. ({issue}`30502`)
* Reduce memory usage when using the Iceberg REST catalog. ({issue}`30624`)
* Improve performance of queries with filters on `date_trunc` with the `week` or
  `quarter` unit on `timestamp with time zone` columns. ({issue}`30192`)
* Improve performance of listing views when using an Iceberg JDBC catalog.
  ({issue}`29751`)
* Reduce coordinator memory usage when collecting table statistics for tables
  with many data files. ({issue}`30675`)
* Improve success rate of concurrent `DELETE` statements on the same table.
  ({issue}`30850`)
* Reduce the number of OAuth 2.0 token requests to the Iceberg REST catalog when
  `iceberg.rest-catalog.session` is set to `NONE`. ({issue}`30816`)
* Improve performance of selective queries on Parquet files, configurable with
  the `parquet.selected-positions-pushdown-enabled` configuration property.
  ({issue}`30303`)
* Reduce memory usage when a write to a sorted table fails. ({issue}`31018`)
* Improve performance of case-insensitive name matching with the Iceberg REST
  catalog by caching namespace listings. ({issue}`29382`)
* Improve performance of reading tables with format version 3 after `DELETE`,
  `UPDATE`, or `MERGE` statements remove all rows from a data file.
  ({issue}`31165`)
* Reduce memory usage when reading `row` columns from Parquet files.
  ({issue}`31381`)
* Fix failure when querying the `$partitions` table of tables with many columns.
  ({issue}`30311`)
* Fix failure when creating tables in BigLake metastore with the Iceberg REST
  catalog. ({issue}`30438`)
* Fix failure when querying the `$partitions` metadata table of tables with
  columns named after SQL reserved words. ({issue}`30488`)
* Fix failure when reading the `$files`, `$partitions`, `$entries`, or
  `$all_entries` metadata table of a table with a dropped partition field.
  ({issue}`30247`)
* Fix failure when reading the `$files` or `$partitions` metadata table of a
  table without snapshots. ({issue}`30501`)
* Fix `SHOW CREATE SCHEMA` failure for schemas with namespace properties unknown
  to Trino when using an Iceberg JDBC catalog. ({issue}`29769`)
* Fix worker out-of-memory failures when writing ORC files with many open
  writers, such as when writing to many partitions. ({issue}`30771`)
* Fix failure when reading `NULL` values of `row` or `map` columns that contain
  a `variant` field in Parquet files. ({issue}`30613`)
* Fix incorrect resolution of tables and views when case-insensitive name
  matching is enabled for the Iceberg REST catalog. ({issue}`30747`)
* Fix incorrect error message when a table metadata file or manifest file is
  missing in S3. ({issue}`30653`)
* Fix excessive memory usage accounting for queries on tables with equality
  delete files. ({issue}`29955`)
* Fix incorrect snapshots and commit times in the `$history` metadata table.
  ({issue}`31102`)
* Fix failure of `DROP TABLE` and other operations that access the table
  location when using vended credentials with an Iceberg REST catalog.
  ({issue}`31217`)
* Fix query failure when the `optimize_metadata_queries` session property is
  enabled and the query filters on a hidden column such as `$path`.
  ({issue}`31103`)
* Fix incorrect or duplicate rows when refreshing a materialized view while its
  source tables are modified, or when the view reads a source table with `FOR
  VERSION AS OF`. ({issue}`30990`)
* Fix excessive memory usage and missing memory accounting for `UPDATE`,
  `DELETE`, and `MERGE` statements on tables with many data files.
  ({issue}`31258`)
* Fix incorrect results when refreshing a materialized view concurrently.
  ({issue}`31257`)
* Fix `Seek past end of stream` failure when reading ORC files or writing to
  sorted tables. ({issue}`10113`, {issue}`20164`, {issue}`28636`)
* Fix failure when selecting a field of a `row` column that is an equality
  delete key. ({issue}`25720`)
* Fix writing Parquet files smaller than the target file size for
  low-cardinality data. ({issue}`31291`)
* Fix missing columns in `information_schema.columns` when a table in the schema
  fails to load with the Glue catalog. ({issue}`30926`)
* Fix duplicate rows when the commit of a write is retried with `retry-policy`
  set to `TASK`. ({issue}`31246`)
* Fix incorrect results when an equality delete file uses a key field nested in
  a `row` column. ({issue}`31376`)

## Ignite connector

* Fix query failure when pushing down a join with an `IS NOT DISTINCT FROM`
  condition and the `complex_join_pushdown_enabled` catalog session property is
  disabled. ({issue}`31097`)
* Prevent temporary tables from being left behind when a write query is canceled
  or fails while it starts. ({issue}`31333`)

## JMX connector

## Kafka connector

## Lakehouse connector

* Add support for configuring access to AWS KMS for table encryption with the
  `aws.kms.*` configuration properties. ({issue}`30371`)
* Add support for disabling the HTTP `Expect: 100-continue` handshake for S3
  requests with the `s3.expect-continue-enabled` configuration property.
  ({issue}`30534`)
* Return the number of removed statistics files when running the
  `drop_extended_stats` command. ({issue}`30768`)
* Add support for sorting by nested `row` fields with the `sorted_by` table
  property. ({issue}`19620`)
* Add the `last_column_takes_rest` table property to store the remaining fields
  of a delimited line in the last column. ({issue}`30938`)
* Add support for `ALTER VIEW ... REFRESH`. ({issue}`30954`)
* Add support for Parquet column indexes, configurable with the
  `parquet.use-column-index` configuration property or the
  `parquet_use_column_index` catalog session property. ({issue}`11000`)
* Add support for configuring the number of retries for requests to the Iceberg
  REST catalog with the `iceberg.rest-catalog.max-retries` configuration
  property. ({issue}`31073`)
* Add support for disabling metrics reporting to the Iceberg REST catalog with
  the `iceberg.rest-catalog.metrics-reporting-enabled` configuration property.
  ({issue}`31075`)
* Add support for procedures, such as `system.sync_partition_metadata`,
  `system.rollback_to_snapshot`, and `system.vacuum`, and for table procedures,
  such as `optimize` and `expire_snapshots`. ({issue}`26753`, {issue}`26754`)
* Add support for running table procedures on materialized views with
  {doc}`/sql/alter-materialized-view` `EXECUTE`. ({issue}`30936`)
* Add the `iceberg.domain-compaction-threshold` configuration property to
  control the compaction of predicates pushed into the Parquet and ORC readers.
  ({issue}`31175`)
* Add support for registering tables with metadata files outside the default
  metadata directory with the `metadata_location` parameter of the
  `register_table` procedure. ({issue}`31164`)
* Add support for the `COMMENT ON MATERIALIZED VIEW` statement. ({issue}`31279`)
* Add the `gc_enabled` table property to control whether snapshot expiration,
  orphan file removal, and `DROP TABLE` delete data files. ({issue}`31218`)
* Add support for writing data files under hash-based directory prefixes with
  the `delta.object-store-layout.enabled` configuration property and the
  `object_store_layout_enabled` table property. ({issue}`24199`)
* Add support for `VACUUM` on tables with deletion vectors. ({issue}`22809`)
* {{breaking}} Move the `fs.cache.*` configuration properties, except
  `fs.cache.enabled`, from the catalog properties file to an `alluxio` cache
  manager configuration file. ({issue}`29184`)
* {{breaking}} Remove the `fs.memory-cache.*` configuration properties. Use a
  `memory` cache manager instead. ({issue}`29184`)
* {{breaking}} Change the default value of the `hive.storage-format`
  configuration property to `PARQUET`. Set the property to `ORC` to continue
  creating new tables in ORC format by default. ({issue}`30818`)
* Skip writing min and max statistics for `real` and `double` columns with NaN
  values in Parquet files. ({issue}`30906`)
* Disable page pruning for `real` and `double` columns in Parquet files written
  without NaN counts. ({issue}`30906`)
* Fail at catalog startup when `iceberg.rest-catalog.vended-credentials-enabled`
  is set to `true` without native file system support for the storage.
  ({issue}`30634`)
* Disallow dropping UniForm tables. ({issue}`31083`)
* {{breaking}} Rename the `gcs.json-key-file-path` configuration property for
  Iceberg REST catalog authentication with `GOOGLE` security to
  `iceberg.rest-catalog.google-json-key-file-path`. ({issue}`31240`)
* Return values of the `$file_modified_time` hidden column in the UTC time zone
  instead of the JVM time zone. ({issue}`31239`)
* {{breaking}} Remove the `iceberg.equality-deletes-blocks-hash-enabled`
  configuration property. ({issue}`31286`)
* {{breaking}} Return `timestamp with time zone` values in UTC instead of the
  session time zone in the `$snapshots`, `$history`, and `$metadata_log_entries`
  system tables. ({issue}`31356`)
* Improve performance of reading and writing `boolean` columns in ORC and
  Parquet files. ({issue}`30299`)
* Improve performance of writing Parquet files. ({issue}`30502`)
* Reduce memory usage when using the Iceberg REST catalog. ({issue}`30624`)
* Improve performance of queries with filters on `date_trunc` with the `week` or
  `quarter` unit on `timestamp with time zone` columns. ({issue}`30192`)
* Reduce coordinator memory usage when collecting table statistics for tables
  with many data files. ({issue}`30675`)
* Improve performance of reading tables with deletion vectors and of `MERGE`,
  `UPDATE`, and `DELETE` statements when Parquet column indexes are enabled.
  ({issue}`30976`)
* Improve performance of selective queries on Parquet files, configurable with
  the `parquet.selected-positions-pushdown-enabled` configuration property.
  ({issue}`30303`)
* Reduce memory usage when a write to a sorted table fails. ({issue}`31018`)
* Improve performance of writes into many partitions by updating partition
  statistics in bulk. ({issue}`31020`)
* Improve performance of writes when `retry-policy` is set to `TASK`. Files
  left behind by failed task attempts are removed by the `vacuum` procedure.
  ({issue}`31408`)
* Improve performance of case-insensitive name matching with the Iceberg REST
  catalog by caching namespace listings. ({issue}`29382`)
* Improve performance of reading tables with format version 3 after `DELETE`,
  `UPDATE`, or `MERGE` statements remove all rows from a data file.
  ({issue}`31165`)
* Reduce memory usage when reading `row` columns from Parquet files.
  ({issue}`31381`)
* Reduce coordinator memory usage when writing checkpoints for tables with many
  files. ({issue}`31354`)
* Fix worker out-of-memory failures when writing ORC files with many open
  writers, such as when writing to many partitions. ({issue}`30771`)
* Fix failure when reading tables with pending clustering operations.
  ({issue}`30855`)
* Fix excessive memory usage accounting for queries on tables with equality
  delete files. ({issue}`29955`)
* Fix incorrect results for `FOR TIMESTAMP AS OF` queries that could return data
  from an earlier table version. ({issue}`31057`)
* Fix failure when reading Avro tables from AWS Glue that define their schema
  with `avro.schema.url` or `avro.schema.literal` and whose stored columns
  differ from the Avro schema. ({issue}`30599`)
* Fix loss of column statistics and slow rollback when a write into existing
  partitions fails during commit. ({issue}`31020`)
* Fix incorrect snapshots and commit times in the `$history` metadata table.
  ({issue}`31102`)
* Fix failure of `DROP TABLE` and other operations that access the table
  location when using vended credentials with an Iceberg REST catalog.
  ({issue}`31217`)
* Fix query failure when the `optimize_metadata_queries` session property is
  enabled and the query filters on a hidden column such as `$path`.
  ({issue}`31103`)
* Fix incorrect results after `DELETE`, `CREATE OR REPLACE TABLE`, or `CREATE OR
  REPLACE TABLE AS` on tables with deletion vectors. ({issue}`30343`)
* Fix incorrect or duplicate rows when refreshing a materialized view while its
  source tables are modified, or when the view reads a source table with `FOR
  VERSION AS OF`. ({issue}`30990`)
* Fix excessive memory usage and missing memory accounting for `UPDATE`,
  `DELETE`, and `MERGE` statements on tables with many data files.
  ({issue}`31258`)
* Fix incorrect results when refreshing a materialized view concurrently.
  ({issue}`31257`)
* Fix `Seek past end of stream` failure when reading ORC files or writing to
  sorted tables. ({issue}`10113`, {issue}`20164`, {issue}`28636`)
* Fix failure when creating a view with the `extra_properties` view property.
  ({issue}`31265`)
* Fix failure when selecting a field of a `row` column that is an equality
  delete key. ({issue}`25720`)
* Fix writing Parquet files smaller than the target file size for
  low-cardinality data. ({issue}`31291`)
* Fix missing columns in `information_schema.columns` when a table in the schema
  fails to load with the Glue catalog. ({issue}`30926`)
* Fix rows silently lost from tables with deletion vectors written by other
  engines when Trino writes a checkpoint. ({issue}`31252`)
* Fix failure when writing checkpoints for tables with statistics that contain
  `Infinity` or `NaN` values, decimal values stored as JSON numbers, or
  timestamps without a zone offset. ({issue}`24029`, {issue}`28532`)
* Fix duplicate rows when the commit of a write is retried with `retry-policy`
  set to `TASK`. ({issue}`31246`)
* Fix incorrect results when filtering on Parquet `timestamp` columns written
  with `isAdjustedToUTC=true` and `hive.parquet.time-zone` set to a zone other
  than UTC. ({issue}`31050`)
* Fix rows silently lost when Trino writes checkpoints for tables with deletion
  vectors that another engine restored to an earlier version. ({issue}`30985`)
* Fix duplicate rows returned by other Delta Lake readers after `OPTIMIZE` on
  tables with deletion vectors. ({issue}`31253`)
* Fix incorrect results when an equality delete file uses a key field nested in
  a `row` column. ({issue}`31376`)

## Loki connector

## MariaDB connector

* Prevent temporary tables from being left behind when a write query is canceled
  or fails while it starts. ({issue}`31333`)

## Memory connector

* Fix incorrect results for queries with multiple distinct aggregations over a
  table with `TABLESAMPLE`. ({issue}`31203`)

## MongoDB connector

* Fix failure when querying a collection with a name that is not lowercase and
  the `mongodb.case-insensitive-name-matching` configuration property is
  disabled. ({issue}`12901`)

## MySQL connector

* Improve performance of queries with an `IS NOT NULL` predicate on `char` or
  `varchar` columns by pushing it down to the data source. ({issue}`31060`)
* Improve performance of retrieving column metadata for tables with many rows.
  ({issue}`31283`)
* Prevent temporary tables from being left behind when a write query is canceled
  or fails while it starts. ({issue}`31333`)

## OpenSearch connector

* Add support for OpenSearch 3.x. ({issue}`30469`)
* Fix failure when listing columns with `information_schema.columns` if an index
  contains unsupported metadata. ({issue}`30852`)

## Oracle connector

* Fix failure when reading a `NUMBER` column without a declared precision, such
  as an aggregation result in a view or in the `query` table function.
  ({issue}`30467`, {issue}`30517`)
* Fix incorrect results for queries that cast `char` to `varchar` when the
  `deprecated.legacy-varchar-to-char-coercion` configuration property is
  enabled. ({issue}`30683`)
* Prevent temporary tables from being left behind when a write query is canceled
  or fails while it starts. ({issue}`31333`)

## Pinot connector

## PostgreSQL connector

* Fix incorrect results for queries that cast `char` to `varchar` when the
  `deprecated.legacy-varchar-to-char-coercion` configuration property is
  enabled. ({issue}`30683`)
* Fix query failure when pushing down a join with an `IS NOT DISTINCT FROM`
  condition and the `complex_join_pushdown_enabled` catalog session property is
  disabled. ({issue}`31097`)
* Prevent temporary tables from being left behind when a write query is canceled
  or fails while it starts. ({issue}`31333`)

## Prometheus connector

## Redis connector

## Redshift connector

* Fix query failure when dynamic filtering is applied to a table read with
  `UNLOAD`. ({issue}`30209`)
* Fix incorrect results for queries that cast `char` to `varchar` when the
  `deprecated.legacy-varchar-to-char-coercion` configuration property is
  enabled. ({issue}`30683`)
* Fix incorrect type mapping of `TEXT` columns in views, which are now mapped to
  `varchar(256)`. ({issue}`31201`)
* Prevent temporary tables from being left behind when a write query is canceled
  or fails while it starts. ({issue}`31333`)

## SingleStore connector

* Prevent temporary tables from being left behind when a write query is canceled
  or fails while it starts. ({issue}`31333`)

## Snowflake connector

* Prevent temporary tables from being left behind when a write query is canceled
  or fails while it starts. ({issue}`31333`)

## SQL Server connector

* Improve performance of queries with an `IS NOT NULL` predicate on `char` or
  `varchar` columns by pushing it down to the data source. ({issue}`31060`)
* Fix queries hanging indefinitely when connecting to an unresponsive SQL
  Server. ({issue}`30786`)
* Prevent temporary tables from being left behind when a write query is canceled
  or fails while it starts. ({issue}`31333`)

## TPC-DS connector

## TPC-H connector

## SPI

* Add a blob cache SPI for plugins that provide cache managers. ({issue}`29184`)
* Add `ConnectorMetadata.getMaterializedViewTableHandleForExecute()` to allow
  connectors to support `ALTER MATERIALIZED VIEW ... EXECUTE`. ({issue}`30868`)
* Add the `SourcePage.trySelectPositions` method to allow connectors to skip
  loading data for positions rejected by filters. ({issue}`30303`)
* Add the `setMaterializedViewComment` method to `ConnectorMetadata` and the
  `checkCanSetMaterializedViewComment` method to `SystemAccessControl` and
  `ConnectorAccessControl`. ({issue}`31279`)
* {{breaking}} Change `BooleanType` to store values in `BitArrayBlock` instead
  of `ByteArrayBlock`. Plugins that access the contents of `boolean` blocks
  directly must be updated. ({issue}`30299`)
* Reject time zone offsets outside the `[-14:00, 14:00]` range in
  `DateTimeEncoding.packTimeWithTimeZone` and the `LongTimeWithTimeZone`
  constructor. ({issue}`9288`)
* Deprecate the `void` return type for procedures. Return `Map<String, Long>`
  with execution metrics instead. ({issue}`30749`)
* {{breaking}} Change `DictionaryBlock.compact()` and
  `DictionaryBlock.compactRelatedBlocks()` to return `Block` instead of
  `DictionaryBlock`. Plugins must not assume that results of `DictionaryBlock`
  operations are dictionary blocks. ({issue}`30536`)
* Deprecate `TrinoPrincipal.getName()` in favor of
  `TrinoPrincipal.getPrincipalName()`, which preserves the original case of the
  principal name. ({issue}`30567`)
* {{breaking}} Remove `ConnectorPageSourceProvider.getMemoryUsage()` and add a
  memory context parameter to
  `ConnectorPageSourceProviderFactory.createPageSourceProvider()`. Connectors
  must report memory usage of state shared across page sources with the provided
  memory context. ({issue}`29955`)
* {{breaking}} Add a `MemoryContext` parameter to
  `ConnectorPageSinkProvider.createMergeSink()` and remove the overload without
  it. ({issue}`29956`, {issue}`29957`)
* {{breaking}} Remove the Jackson annotations from `TrinoPrincipal` and
  `RoleGrant`. Plugins that serialize these classes to JSON must use their own
  representation. ({issue}`30610`)
* {{breaking}} Change the return type of the `ConnectorMetadata.finishMerge`
  method to `Optional<ConnectorOutputMetadata>`. ({issue}`30934`)
* {{breaking}} Add frame exclusion parameters to `WindowFunction.processRow`.
  Custom window functions must be updated to the new signature. ({issue}`30226`)
