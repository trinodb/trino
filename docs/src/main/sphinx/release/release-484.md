# Release 484 (dd MMM 2026)

## General

* Add support for the [JSON value
  constructor](json-value-constructor). ({issue}`30341`)
* Add support for the [JSON_SERIALIZE](json-serialize)
  function. ({issue}`30341`)
* Add the {func}`json_scalar` function. ({issue}`30341`)
* Add support for `json` input to [JSON_EXISTS](json-exists),
  [JSON_VALUE](json-value), [JSON_QUERY](json-query), and
  [JSON_TABLE](json-table). ({issue}`30341`)
* Add support for datetime values in the [JSON type](json-data-type) and casts
  from `json` to `date` and `time`. ({issue}`30341`)
* Add support for numeric values in [JSON_VALUE](json-value) with `RETURNING`,
  [JSON path](json-path-language) arithmetic, and JSON path array
  subscripts. ({issue}`30341`)
* Allow `FORMAT JSON` arguments in [JSON_OBJECT](json-object) with `WITH UNIQUE
  KEYS`, enforcing key uniqueness in nested objects. ({issue}`30341`)
* Add support for interval types with explicit [leading-field
  precision](interval-leading-precision) and up to 12 [fractional-second
  digits](interval-fractional-seconds-precision), such as `INTERVAL DAY(4) TO
  SECOND(9)`. ({issue}`6754`)
* Add support for specifying the interval field range and precision when
  [subtracting datetime values](/functions/datetime), such as `(end_time -
  start_time) DAY(3)`. ({issue}`6754`)
* Add support for the `EXCLUDE` clause in [window frame](window-clause)
  specifications. ({issue}`30226`)
* Add support for a file system cache shared by all catalogs on a node. Cache
  managers are configured with the `cache-manager.config-files` configuration
  property. See [](/object-storage/file-system-cache). ({issue}`29184`)
* Add support for `IF NOT EXISTS` in [](/sql/create-view). ({issue}`28076`)
* Add support for adding comments to materialized views with
  [](/sql/comment). ({issue}`31279`)
* Add support for limiting the size of the output data produced by a query with
  the `query.max-output-data-size` configuration property. ({issue}`30615`)
* Add support for returning metrics from procedures invoked with
  [](/sql/call). ({issue}`30749`)
* Allow resource groups and selectors with a `NULL` environment in the [database
  resource group manager](db-resource-group-manager) to match every
  environment. ({issue}`30873`)
* Allow restoring the legacy implicit coercion from `varchar` to `char` for a
  session with the `legacy_varchar_to_char_coercion` session
  property. ({issue}`30708`)
* Allow restoring the legacy repair of topologically invalid input geometries in
  {func}`ST_Intersection`, {func}`ST_Difference`, {func}`ST_SymDifference`,
  {func}`ST_Union`, {func}`geometry_union`, and {func}`geometry_union_agg` with
  the `trino.geospatial.legacy-lenient-overlay` JVM system
  property. ({issue}`30584`)
* Preserve the original case of user names in `GRANT`, `REVOKE`, and `SET
  AUTHORIZATION` statements, and when recording the owner of tables, views, and
  schemas. ({issue}`30567`)
* Fail queries immediately instead of retrying tasks when spooled exchange data
  is lost with `retry-policy` set to `TASK`. ({issue}`31336`)
* {{breaking}} Retain numeric arguments as JSON numbers in `JSON_ARRAY`,
  `JSON_OBJECT`, and `PASSING` clauses instead of converting them to JSON
  strings. ({issue}`30341`)
* {{breaking}} Retain numeric types and decimal scale when casting `array`,
  `map`, and `row` values to `json`. ({issue}`30341`)
* {{breaking}} Retain decimal notation when casting decimal-form JSON numbers to
  `varchar`. ({issue}`30341`)
* {{breaking}} Retain object member order and duplicate keys in
  {func}`json_parse` and JSON literals. ({issue}`30341`)
* {{breaking}} Disallow construction of JSON values with more than 1,024 nested
  arrays and objects. ({issue}`30341`)
* {{breaking}} Disallow JSON input containing overflowing exponent-form numbers
  or unpaired Unicode surrogate escapes. ({issue}`30341`)
* {{breaking}} Remove conversion of `json` arguments to JSON strings in
  `JSON_ARRAY`, `JSON_OBJECT`, and `PASSING` clauses. The JSON values are used
  directly without requiring `FORMAT JSON`. ({issue}`30341`)
* {{breaking}} Require quoting identifiers named `JSON_SERIALIZE`, which is a
  reserved keyword. ({issue}`30341`)
* {{breaking}} Require qualifying calls written as `json(x)` to invoke a
  function instead of the JSON constructor. ({issue}`30341`)
* {{breaking}} Limit the number of elements returned by {func}`ngrams` to
  1,000,000. ({issue}`30806`)
* {{breaking}} Reduce the maximum duration of day-time intervals to
  approximately 292,000 years. Subtracting timestamps that are further apart
  fails with an overflow error. ({issue}`6754`)
* {{breaking}} Disallow extracting fields that are absent from an interval's
  qualifier. ({issue}`6754`)
* {{breaking}} Return the full leading field when extracting a value from an
  interval, such as `36` for `hour(INTERVAL '36' HOUR)`. ({issue}`6754`)
* {{breaking}} Truncate interval multiplication and division results to the
  interval's field range, so `INTERVAL '1' DAY / 2` returns zero
  days. ({issue}`6754`)
* Improve performance of queries involving `boolean` values, especially when
  reading or writing ORC and Parquet files. ({issue}`30299`)
* Improve performance of queries that compare the result of {func}`at_timezone`
  with a constant time zone against a `timestamp with time zone`
  value. ({issue}`30597`)
* Reduce memory usage for queries that compute both {func}`avg` and {func}`sum`
  over the same `decimal` column. ({issue}`30595`)
* Improve performance of queries with predicates on {func}`date_trunc` with the
  `week` or `quarter` unit. ({issue}`30192`)
* Improve performance of window functions that compute {func}`sum` over
  `decimal` values with a moving window frame. ({issue}`30630`)
* Improve scheduling of tasks that require no memory, such as tasks reading only
  metadata, when cluster memory is exhausted with fault-tolerant
  execution. ({issue}`30678`)
* Improve performance of queries with uncorrelated `EXISTS`
  subqueries. ({issue}`30224`)
* Improve performance of queries that filter or project dictionary-encoded
  data. ({issue}`30877`)
* Reduce the amount of data read for queries with filters that combine multiple
  conditions with `AND`. ({issue}`30909`)
* Improve performance of queries with joins and filters that can fail when
  `optimizer.allow-unsafe-pushdown` is enabled. ({issue}`30917`)
* Reduce memory usage of queries that redistribute dictionary-encoded data
  across workers. ({issue}`31056`)
* Improve performance of cross joins where one side is a subquery with `LIMIT`
  or `ORDER BY ... LIMIT`. ({issue}`31128`)
* Improve performance of queries with range comparisons between `char` columns
  and `varchar` values. ({issue}`31089`)
* Improve performance of queries with an `IN` predicate that compares a cast
  expression against a list of literals, such as `CAST(ts AS date) IN (DATE
  '2020-01-01', DATE '2020-01-02')`. ({issue}`31182`)
* Improve performance of filters on dictionary-encoded columns when each page
  uses only a few entries of a large dictionary. ({issue}`31200`)
* Improve performance of queries with `ORDER BY` when `retry-policy` is set to
  `QUERY`. ({issue}`30966`)
* Improve performance of queries that differ only in literal values from
  previously run queries. ({issue}`30465`)
* Reduce worker memory usage for queries with many operators. ({issue}`31301`)
* Reduce memory usage of [dynamic filtering](/admin/dynamic-filtering) on
  `varchar` columns. ({issue}`31303`)
* Improve performance of queries with filters on {func}`at_timezone` over
  `timestamp with time zone` columns, including filters on views that convert
  such columns to a per-row time zone. ({issue}`30551`)
* Improve query planning performance for queries with many joins. The
  `optimizer.max-reordered-joins` configuration property and the
  `max_reordered_joins` session property are limited to a maximum of
  62. ({issue}`31261`)
* Improve performance of `BETWEEN` and comparison predicates on numeric values
  that are implicitly cast to a wider type. ({issue}`31400`)
* Improve performance of casting between `decimal` values and `double` or `real`
  values. ({issue}`31475`, {issue}`31481`, {issue}`31502`)
* Improve performance of queries with conditional expressions, such as `CASE` or
  `IF`, that reference columns only needed when the condition is
  met. ({issue}`31473`)
* Improve performance of nested string concatenations. ({issue}`31465`)
* Fix failure when a JSON simplified accessor applies an item method, such as
  `integer()`, after two or more nested member accesses, for example
  `j.a.b.integer()`. ({issue}`30379`)
* Fix query failure when using {func}`try` with an argument containing
  {func}`nullif` or `BETWEEN`. ({issue}`30399`)
* Set the process name to `trino-server` when running the `trino-server-core`
  distribution. ({issue}`30403`)
* Fix potential worker memory accounting leak when tasks finish, which can cause
  queries to block or fail with out-of-memory errors. ({issue}`30472`)
* Fix incorrect results for `IF` and `CASE` expressions that repeat a
  non-deterministic condition within a branch, such as `random() = 0.5 AND
  random() = 0.5`. ({issue}`30509`)
* Fix incorrect results when {func}`sum` over `bigint` values overflows in a
  window function. The query fails instead. ({issue}`30600`)
* Fix incorrect results when using `AT TIME ZONE` with an `interval` offset
  outside the range of `-14:00` to `+14:00` on `time` values. The query fails
  instead. ({issue}`9288`)
* Fix potential worker unavailability under highly concurrent
  workloads. ({issue}`30627`)
* Fix incorrect results for joins on `char` columns cast to `varchar` when
  dynamic filtering is enabled. ({issue}`30686`)
* Fix worker out-of-memory errors due to untracked memory usage for queries with
  large right or full outer joins. ({issue}`30790`)
* Fix incorrect results when casting some `decimal` values with precision of 18
  or less to `real`. ({issue}`30130`)
* Fix failure when executing a `USE` statement as a prepared
  statement. ({issue}`30859`)
* Fix query failure when planning queries with a very large number of `OR`
  predicates. ({issue}`30709`)
* Fix missing query completion events and a potential coordinator memory leak
  when a worker restarts while running a query. ({issue}`30755`)
* Fix query failure for queries with a `BETWEEN` predicate on an expression and
  multiple aggregations with `DISTINCT`. ({issue}`30608`)
* Fix intermittent query failure with a `No committed attempts found under sink
  output path` error when using fault-tolerant execution. ({issue}`30411`)
* Fix failure of {func}`convex_hull_agg` when an input geometry is topologically
  invalid. ({issue}`30584`)
* Fix incorrect results or query failure for {func}`avg` over `decimal` values
  when the sum of the values exceeds the range of the type. ({issue}`30991`)
* Fix potential worker out-of-memory errors for queries with `ORDER BY ...
  LIMIT` or filters on {func}`row_number` or {func}`rank` that process wide
  rows. ({issue}`31014`)
* Fix incorrect results instead of a failure when {func}`date_add` or adding an
  interval to a `timestamp` value produces a value outside the supported
  range. ({issue}`31002`)
* Fix incorrect results when subtracting `timestamp` values with a very large
  difference. ({issue}`31002`)
* Fix incorrect results for aggregations over outer joins without equality
  conditions, such as `LEFT JOIN ... ON true`, when the joined side produces no
  rows. ({issue}`30989`)
* Fix query failure instead of returning `NULL` when calling {func}`mod` with a
  zero divisor inside {func}`try`. ({issue}`31054`)
* Fix failure when using `WITH SESSION` to set a catalog session property for a
  catalog that already has session properties set. ({issue}`31045`)
* Fix failure when a SQL user-defined function ends with an `IF` or `CASE`
  statement that returns a value from every branch. ({issue}`30582`)
* Fix incorrect results when filtering with `IF`, `CASE`, or `NULLIF`
  expressions that contain non-deterministic conditions. ({issue}`30993`)
* Fix incorrect results for `IN` predicates with `real` `NaN` values, or with
  `time with time zone` and `timestamp with time zone` values in different time
  zones. ({issue}`31069`, {issue}`31080`)
* Fix incorrect memory accounting for aggregations with both `DISTINCT` and
  `ORDER BY` clauses, which could cause workers to run out of
  memory. ({issue}`31039`)
* Fix workers becoming unresponsive and dropping out of the cluster when running
  queries with a large number of stages and tasks. ({issue}`21512`)
* Fix `MERGE` failing with `MERGE_TARGET_ROW_MULTIPLE_MATCHES` when the target
  table is bucketed and the query uses a partitioned join
  distribution. ({issue}`30639`)
* Fix incorrect results for aggregations over `CASE` expressions that contain
  non-deterministic functions. ({issue}`31121`)
* Fix incorrect results for comparisons involving {func}`year` or
  {func}`date_trunc` with a non-deterministic argument. ({issue}`31086`)
* Fix incorrect results for `IS NOT DISTINCT FROM` comparisons between a cast
  expression and a constant. ({issue}`31084`)
* Fix incorrect results for simple `CASE` expressions when the operand is `NULL`
  or `NaN`, or when comparing values with time
  zones. ({issue}`31065`, {issue}`31081`)
* Fix incorrect `data_type` for `number` columns in `system.jdbc.columns` and in
  JDBC `DatabaseMetaData.getColumns`. ({issue}`31184`)
* Fix incorrect results for `OR` and `IN` predicates over non-deterministic
  expressions. ({issue}`31149`)
* Fix failure of recursive `WITH` queries that use a table
  function. ({issue}`31053`)
* Fix incorrect results for queries with multiple aggregations containing a
  `DISTINCT` clause over non-deterministic expressions. ({issue}`31151`)
* Fix `TRY` not suppressing errors when accessing a field of a row produced by a
  failing expression, such as `TRY(array[index].field)` with an out-of-bounds
  index. ({issue}`31008`)
* Fix incorrect results for `char` values ending with a space produced by
  {func}`reverse` or by casting to a shorter `char` type. ({issue}`31190`)
* Fix query failure when comparing a `varchar` value cast to `char` with a
  `char` value. ({issue}`31187`)
* Fix incorrect results for `IN` and `NOT IN` predicates with a list of
  consecutive values that contains `NULL`. ({issue}`31068`)
* Fix incorrect results for full outer joins with a join condition when an input
  produces a single row. ({issue}`30988`)
* Fix incorrect results for {func}`trim`, {func}`ltrim`, and {func}`rtrim` on
  `char` values when trimming a set of characters that does not include a
  space. ({issue}`31004`)
* Fix potential worker memory leak when a task fails while writing its output,
  such as when a fault-tolerant task is
  aborted. ({issue}`31334`, {issue}`31353`)
* Fix incorrect results and failures when casting a `timestamp` value before
  1970 with fractional seconds to `time`. ({issue}`31344`)
* Fix query hang or failure when calling {func}`hamming_distance` on strings
  containing a NUL character. ({issue}`31247`)
* Fix incorrect results and query failures when {func}`max_by` or {func}`min_by`
  with a count argument returns `row` values with null fields. ({issue}`31193`)
* Fix incorrect results for queries with a `GROUP BY` clause and multiple
  aggregations containing a `DISTINCT` clause when a grouping key is
  `NULL`. ({issue}`29095`)
* Fix incorrect `NULL` results from {func}`max_by` and {func}`min_by` with a
  count argument when the input contains `NULL` values. ({issue}`31383`)
* Fix incorrect results when filtering on a simple `CASE` expression with a
  `NULL` operand and a predicate in a `WHEN` clause. ({issue}`30992`)
* Fix incorrect results for window functions with a `ROWS` frame whose offsets
  vary between rows. ({issue}`31420`)
* {{breaking}} Fix missing padding in SQL/JSON output for `char`
  values. ({issue}`30341`)
* {{breaking}} Fix precision loss when casting decimal-form JSON numbers to
  numeric types. ({issue}`30341`)
* {{breaking}} Fix invalid JSON returned by {func}`json_array_get` when
  extracting string elements. ({issue}`28861`)
* Fix clients polling for results in a tight loop after a query
  finishes. ({issue}`31431`)
* Fix query failure when using `BETWEEN SYMMETRIC` with nested fields of `row`
  columns. ({issue}`31487`)
* Fix incorrect formatting of `interval` values when the JVM default locale uses
  non-ASCII digits. ({issue}`31433`)
* Fix duplicate rows in `information_schema.tables` when the `table_name` filter
  includes a table that does not exist. ({issue}`31488`)

## Security

* Add support for clients using the [OAuth 2.0](/security/oauth2) client
  credentials flow by including the token endpoint and scope in the
  `WWW-Authenticate` authentication challenge. ({issue}`15836`)
* Add support for the `{user}` placeholder in the `catalog`, `schema`, and
  `table` patterns of [file-based access
  control](/security/file-system-access-control) rules. ({issue}`31205`)
* Add support for forwarding selected extra credentials to [Open Policy
  Agent](/security/opa-access-control) with the
  `opa.identity.extra-credentials-keys` configuration property. ({issue}`29302`)
* Fix authorization failures with Ranger access control when the Kerberos
  principal differs from the mapped user name. ({issue}`29342`)
* Fix query failure when a session property manager sets a default for a session
  property that the user is not allowed to set. ({issue}`31281`)

## Web UI

* Add support for capturing worker thread snapshots on the worker status
  page. ({issue}`30388`)
* Add line numbers to the query text on the query details page. ({issue}`31424`)
* Use the full width of the browser window for the query list. ({issue}`30352`)
* Fix sorting of finished and failed queries when sorting the query list by
  progress. ({issue}`30528`)
* Fix overlapping text in the navigation menu when it is
  collapsed. ({issue}`31232`)
* Fix failure to load the query details page when worker addresses are IPv6
  addresses with a zone ID. ({issue}`30905`)
* Fix missing query progress in the query list for queries with `retry-policy`
  set to `TASK` until all stages are scheduled. ({issue}`31336`)

## JDBC driver

* Add support for the OAuth 2.0 client credentials flow with the
  `oauth2ClientId`, `oauth2ClientSecret`, `oauth2TokenEndpoint`, and
  `oauth2Scope` [connection
  parameters](jdbc-parameter-reference). ({issue}`15836`)
* Add support for reading `interval` values with up to 12 fractional-second
  digits and reporting their field ranges and precision. ({issue}`6754`)
* Throw `SQLException` instead of `IllegalArgumentException` when calling
  `getTime` or `getTimestamp` on a column of an incompatible
  type. ({issue}`5315`)
* Improve performance of reading query results that use the JSON
  encoding. ({issue}`30375`)
* Fix failure when reading `interval` values when the JVM default locale uses
  non-ASCII digits, such as Arabic or Persian. ({issue}`31433`)

## Docker image

* {{breaking}} Remove the ppc64le Docker image. ({issue}`30422`)
* {{breaking}} Use the Red Hat Hardened core runtime image as the base of the
  Docker image. The image does not include utilities such as `grep`, `dnf`, or
  `useradd`. ({issue}`30397`)
* {{breaking}} Remove the `run-trino` script from the Docker image. Custom
  `node.properties` files must set `node.id=${ENV:HOSTNAME}` to keep using the
  container hostname as the node identifier. ({issue}`30397`)

## CLI

* Add support for [OAuth 2.0 client credentials
  authentication](cli-oauth2-client-credentials-auth) with the
  `--oauth2-client-id`, `--oauth2-client-secret`, `--oauth2-token-endpoint`, and
  `--oauth2-scope` options. ({issue}`15836`)
* Improve performance of reading query results that use the JSON
  encoding. ({issue}`30375`)

## BigQuery connector

* Add support for setting the location of a dataset with the `location` schema
  property in [](/sql/create-schema). ({issue}`30366`)
* Add support for retrying writes that fail due to exceeded BigQuery quotas,
  configurable with the `bigquery.write-retry-max-attempts`,
  `bigquery.write-retry-initial-delay`, and `bigquery.write-retry-max-delay`
  configuration properties. ({issue}`30927`)
* Fix query failures due to read timeouts when BigQuery takes longer than 20
  seconds to respond. ({issue}`30840`)
* Fix potential connection leak when reading from BigQuery
  fails. ({issue}`30834`)
* Fix failure due to ambiguous names when querying a table or dataset after
  another table or dataset with the same name in different case is dropped or
  renamed, when `bigquery.case-insensitive-name-matching` is
  enabled. ({issue}`31332`)

## ClickHouse connector

* Add support for creating, altering, and dropping tables and schemas on all
  nodes of a [ClickHouse cluster](clickhouse-cluster-mode) with the
  `clickhouse.cluster-name` configuration property. ({issue}`17307`)
* Fix duplicate rows when inserting into `Distributed` tables, and failure when
  inserting into replicated tables. ({issue}`7600`, {issue}`7601`)
* Fix leak of temporary tables in the remote database when a query writing to a
  table fails or is canceled. ({issue}`31333`)

## Delta Lake connector

* Add support for Databricks 18 LTS. ({issue}`30935`)
* Add support for the [vacuum](delta-lake-vacuum) procedure on tables with
  deletion vectors. ({issue}`22809`)
* Add support for writing data files under hash-based directory prefixes with
  the `delta.object-store-layout.enabled` configuration property and the
  `object_store_layout_enabled` table property. ({issue}`24199`)
* Add support for reporting execution metrics in the output of the
  [optimize](delta-lake-alter-table-execute) table procedure. ({issue}`28999`)
* Add support for using `CRC32`, `CRC32C`, `SHA1`, or `SHA256` checksums when
  writing to S3 with the `s3.checksum-algorithm` configuration
  property. ({issue}`31523`)
* Add support for writing to S3-compatible storage that rejects the HTTP
  expect-continue handshake by setting the `s3.expect-continue-enabled`
  configuration property to `false`. ({issue}`30534`)
* {{breaking}} Require setting the `fs.cache.directories`, `fs.cache.max-sizes`,
  `fs.cache.max-disk-usage-percentages`, `fs.cache.ttl`, and
  `fs.cache.page-size` properties in the `alluxio` cache manager of the
  cluster-wide [file system cache](/object-storage/file-system-cache) instead of
  in catalog properties files. Catalogs only set
  `fs.cache.enabled`. ({issue}`29184`)
* Disallow dropping tables with
  [UniForm](https://docs.delta.io/latest/delta-uniform.html)
  enabled. ({issue}`31083`)
* Improve performance of writing data files. ({issue}`30502`)
* Improve performance of `MERGE`, `UPDATE`, and `DELETE` statements and of
  reading tables with deletion vectors. ({issue}`30976`)
* Improve performance of queries with selective filters. This can be disabled
  with the `parquet.selected-positions-pushdown-enabled` configuration
  property. ({issue}`30303`)
* Reduce memory usage when reading `row` columns from Parquet
  files. ({issue}`31381`)
* Reduce coordinator memory usage when writing checkpoints for tables with many
  files. ({issue}`31354`)
* Improve performance of writes when task retries are enabled. ({issue}`31408`)
* Fix incorrect memory accounting for `MERGE`, `UPDATE`, and `DELETE`
  statements. ({issue}`29956`)
* Fix incorrect results for queries using `FOR TIMESTAMP AS OF` that could read
  an older version of the table. ({issue}`31057`)
* Fix deleted rows remaining visible after `DELETE` of whole files or `CREATE OR
  REPLACE TABLE` on tables with deletion vectors. ({issue}`30343`)
* Fix writing Parquet files smaller than the target file size when writing
  low-cardinality data. ({issue}`31291`)
* Fix data loss when Trino writes a checkpoint for tables with deletion vectors
  written by other engines. ({issue}`31252`)
* Fix failure when writing checkpoints, including during `OPTIMIZE`, for tables
  whose statistics contain `NaN` or infinite `double` or `real` values, decimal
  values stored as JSON numbers, or timestamps without a zone
  offset. ({issue}`24029`, {issue}`28532`)
* Fix potential data loss when Trino writes a checkpoint for a table with
  deletion vectors that another engine restored to an earlier
  version. ({issue}`30985`)
* Fix duplicate rows returned by other Delta Lake readers after running
  `OPTIMIZE` on tables with deletion vectors. ({issue}`31253`)
* Fix incorrect results when filtering on `timestamp with time zone` columns
  after Trino writes a checkpoint for a table with sub-millisecond statistics
  from other writers. ({issue}`31231`)
* Fix `OPTIMIZE` skipping a file with deletion vectors when it is the only file
  smaller than `file_size_threshold` in its partition. ({issue}`29041`)
* Fix incorrect results when reading or modifying tables with deletion vectors
  when `delta.enableDeletionVectors` is
  disabled. ({issue}`30685`, {issue}`31396`)
* Fix failure when reading change data with the `table_changes` function for
  partitions with values that require URI encoding, such as values containing a
  space. ({issue}`30952`)
* Fix failure when reading nested arrays from Parquet files that use the legacy
  list encoding. ({issue}`27766`)

## Druid connector

* Improve performance of queries with `ORDER BY` on the `__time` column and
  `LIMIT`. ({issue}`31277`)

## Elasticsearch connector

* Fix failure when listing columns if an index contains fields with unsupported
  metadata. ({issue}`30852`)

## Faker connector

* Fix incorrect values generated for `timestamp(12)` and `timestamp(12) with
  time zone` columns when the `min` and `max` column properties are
  set. ({issue}`31037`)

## Hive connector

* Add support for reading the remainder of a line into the last column of
  TEXTFILE, RCTEXT, and SEQUENCEFILE tables with the `last_column_takes_rest`
  [table property](hive-table-properties). ({issue}`30938`)
* Add support for using `CRC32`, `CRC32C`, `SHA1`, or `SHA256` checksums when
  writing to S3 with the `s3.checksum-algorithm` configuration
  property. ({issue}`31523`)
* Add support for writing to S3-compatible storage that rejects the HTTP
  expect-continue handshake by setting the `s3.expect-continue-enabled`
  configuration property to `false`. ({issue}`30534`)
* {{breaking}} Require setting the `fs.cache.directories`, `fs.cache.max-sizes`,
  `fs.cache.max-disk-usage-percentages`, `fs.cache.ttl`, and
  `fs.cache.page-size` properties in the `alluxio` cache manager of the
  cluster-wide [file system cache](/object-storage/file-system-cache) instead of
  in catalog properties files. Catalogs only set
  `fs.cache.enabled`. ({issue}`29184`)
* {{breaking}} Set the default value of the `hive.storage-format` configuration
  property to `PARQUET`. The previous behavior can be restored by setting
  `hive.storage-format` to `ORC`. ({issue}`30818`)
* {{breaking}} Return values of the `$file_modified_time` hidden column in UTC
  instead of the JVM default time zone. ({issue}`31239`)
* Improve performance of writing Parquet files. ({issue}`30502`)
* Improve performance of queries with selective filters on Parquet files. This
  can be disabled with the `parquet.selected-positions-pushdown-enabled`
  configuration property. ({issue}`30303`)
* Improve performance of writes into many existing partitions when using a
  Thrift metastore. ({issue}`31020`)
* Reduce memory usage when reading `row` columns from Parquet
  files. ({issue}`31381`)
* Improve performance of the `flush_metadata_cache` procedure for partitioned
  tables. ({issue}`31486`)
* Fix worker out-of-memory errors due to under-accounted memory usage when
  writing ORC files. ({issue}`30771`)
* Fix potential worker out-of-memory errors when writes to sorted tables
  fail. ({issue}`31018`)
* Fix failure when reading Avro tables in the Glue catalog whose columns differ
  from the schema defined by the `avro.schema.url` or `avro.schema.literal`
  table property. ({issue}`30599`)
* Fix loss of column statistics and slow rollback when a write into existing
  partitions fails. ({issue}`31020`)
* Fix failure when reading ORC files where a large `varchar` or `varbinary`
  value is followed only by null or empty values until the end of a
  stripe. ({issue}`10113`)
* Fix writing Parquet files smaller than the target file size when writing
  low-cardinality data. ({issue}`31291`)
* Fix incorrect results when filtering on `timestamp` columns in Parquet files
  with UTC-adjusted timestamps when `hive.parquet.time-zone` is set to a time
  zone other than UTC. ({issue}`31050`)
* Fix incorrect translation of Hive views containing an `IN` predicate in a
  `JOIN` condition. ({issue}`31298`)
* Fix failure when reading nested arrays from Parquet files that use the legacy
  list encoding. ({issue}`27766`)
* Fix failure when reading JSON tables with duplicate top-level keys in a JSON
  object. ({issue}`29983`)

## Hudi connector

* Improve performance of queries with selective filters on Parquet files. This
  can be disabled with the `parquet.selected-positions-pushdown-enabled`
  configuration property. ({issue}`30303`)
* Reduce memory usage when reading `row` columns from Parquet
  files. ({issue}`31381`)
* Fix query failure when reading copy-on-write tables with pending clustering
  operations. ({issue}`30855`)
* Fix incorrect results when filtering on `timestamp` columns in Parquet files
  with UTC-adjusted timestamps when the JVM time zone is not
  UTC. ({issue}`31050`)
* Fix failure when reading nested arrays from Parquet files that use the legacy
  list encoding. ({issue}`27766`)

## Iceberg connector

* Add support for running table procedures such as `optimize` and
  `expire_snapshots` on materialized views with `ALTER MATERIALIZED VIEW ...
  EXECUTE`. See [](/sql/alter-materialized-view). ({issue}`21797`)
* Add support for refreshing view definitions with `ALTER VIEW ... REFRESH`. See
  [](/sql/alter-view). ({issue}`30954`)
* Add support for adding comments to materialized views with
  [](/sql/comment). ({issue}`31279`)
* Add support for sorting data files with a custom sort order using the
  `sorted_by` parameter of the [optimize](iceberg-alter-table-execute) table
  procedure. ({issue}`31274`)
* Add support for [sorting](iceberg-sorted-files) by nested fields with the
  `sorted_by` table property. ({issue}`19620`)
* Add support for preventing snapshot expiration, orphan file removal, and `DROP
  TABLE` from deleting table files with the `gc_enabled` [table
  property](iceberg-table-properties). ({issue}`31218`)
* Add support for registering tables whose metadata file is outside the default
  metadata directory with the `metadata_location` parameter of the
  [register_table](iceberg-register-table) procedure. ({issue}`31164`)
* Add support for creating tables with the BigLake metastore through the Iceberg
  REST catalog with the
  `iceberg.rest-catalog.server-assigned-table-location-enabled` configuration
  property. ({issue}`30438`)
* Add support for configuring the AWS KMS connection used to read tables with
  encrypted Parquet files with the `aws.kms.region`, `aws.kms.endpoint`,
  `aws.kms.iam-role`, `aws.kms.access-key`, `aws.kms.secret-key`, and related
  configuration properties. ({issue}`30371`)
* Add support for using `CRC32`, `CRC32C`, `SHA1`, or `SHA256` checksums when
  writing to S3 with the `s3.checksum-algorithm` configuration
  property. ({issue}`31523`)
* Add support for writing to S3-compatible storage that rejects the HTTP
  expect-continue handshake by setting the `s3.expect-continue-enabled`
  configuration property to `false`. ({issue}`30534`)
* Add support for using the AWS default credentials provider chain for SigV4
  request signing with the Iceberg REST catalog when no IAM role or access keys
  are configured. ({issue}`25257`)
* Add support for configuring the maximum number of retries for Iceberg REST
  catalog requests with the `iceberg.rest-catalog.max-retries` configuration
  property. ({issue}`31073`)
* Add support for disabling reporting of metrics to the Iceberg REST catalog
  with the `iceberg.rest-catalog.metrics-reporting-enabled` configuration
  property. ({issue}`31075`)
* Add support for limiting the size of the case-insensitive name mapping cache
  of the Iceberg REST catalog with the
  `iceberg.rest-catalog.case-insensitive-name-matching.cache-max-size`
  configuration property. ({issue}`30856`)
* Add support for skipping more data when filtering on predicates with more than
  1,000 discrete values, such as [dynamic filters](/admin/dynamic-filtering)
  from selective joins, with the `iceberg.domain-compaction-threshold`
  configuration property. ({issue}`31175`)
* Add support for reporting the number of added data files in the output of the
  `migrate` procedure. ({issue}`30749`)
* Add support for reporting the number of removed statistics files in the output
  of [drop_extended_stats](drop-extended-stats). ({issue}`30768`)
* Include the commit summary in the output metadata reported to event listeners
  for `MERGE`, `UPDATE`, and `DELETE` statements. ({issue}`30934`)
* {{breaking}} Require setting the `fs.cache.directories`, `fs.cache.max-sizes`,
  `fs.cache.max-disk-usage-percentages`, `fs.cache.ttl`, and
  `fs.cache.page-size` properties in the `alluxio` cache manager of the
  cluster-wide [file system cache](/object-storage/file-system-cache) instead of
  in catalog properties files. Catalogs only set
  `fs.cache.enabled`. ({issue}`29184`)
* Require the native file system for the warehouse location, such as
  `fs.s3.enabled`, to be enabled when
  `iceberg.rest-catalog.vended-credentials-enabled` is set to
  `true`. ({issue}`30634`)
* {{breaking}} Require the `iceberg.rest-catalog.google-json-key-file-path` or
  `iceberg.rest-catalog.google-json-key` configuration property instead of
  `gcs.json-key-file-path` to authenticate with an Iceberg REST catalog using
  `GOOGLE` security. ({issue}`31240`)
* {{breaking}} Remove the `iceberg.equality-deletes-blocks-hash-enabled`
  configuration property. ({issue}`31286`)
* {{breaking}} Return `timestamp with time zone` values in UTC instead of the
  session time zone in the `$snapshots`, `$history`, and `$metadata_log_entries`
  metadata tables. ({issue}`31356`)
* Reduce coordinator memory usage when committing writes that produce a large
  number of files. ({issue}`30433`)
* Improve performance of writing Parquet files. ({issue}`30502`)
* Improve performance of listing views, such as querying
  `information_schema.views`, when using an Iceberg JDBC
  catalog. ({issue}`29751`)
* Reduce coordinator memory usage when planning queries on tables with many
  columns and data files. ({issue}`30675`)
* Reduce query failures when running concurrent `DELETE` statements on the same
  table. ({issue}`30850`)
* Reduce the number of OAuth 2.0 token requests made to the Iceberg REST catalog
  when `iceberg.rest-catalog.session` is set to `NONE`. ({issue}`30816`)
* Improve performance of queries with filters on Parquet files that contain
  column indexes. This can be disabled with the `parquet.use-column-index`
  configuration property or the `parquet_use_column_index` session
  property. ({issue}`11000`)
* Improve performance of queries with selective filters on Parquet files. This
  can be disabled with the `parquet.selected-positions-pushdown-enabled`
  configuration property. ({issue}`30303`)
* Improve performance of case-insensitive name matching with the Iceberg REST
  catalog. ({issue}`29382`)
* Improve performance of queries with range predicates when reading ORC files
  with Bloom filters. ({issue}`31175`)
* Improve performance of reading Iceberg v3 tables after `DELETE`, `UPDATE`, or
  `MERGE` removes all rows of a data file, by removing the file instead of
  writing a deletion vector. ({issue}`31165`)
* Reduce memory usage when reading `row` columns from Parquet
  files. ({issue}`31381`)
* Fix query failure when reading the `$partitions` metadata table of tables with
  many columns. ({issue}`30311`)
* Fix failure when reading the `$partitions` metadata table of tables with
  columns named after SQL reserved words, such as `group` or
  `order`. ({issue}`30488`)
* Fix query failure when reading the `$files`, `$partitions`, `$entries`, or
  `$all_entries` metadata tables of tables with a dropped partition
  field. ({issue}`30247`)
* Fix query failure when reading the `$files` or `$partitions` metadata tables
  of tables without snapshots. ({issue}`30501`)
* Fix failure of `SHOW CREATE SCHEMA` when using an Iceberg JDBC catalog and the
  schema has namespace properties not supported by Trino. ({issue}`29769`)
* Fix worker out-of-memory errors due to under-accounted memory usage when
  writing ORC files. ({issue}`30771`)
* Fix failure when reading null `row` or `map` values that contain `variant`
  fields from Parquet files. ({issue}`30613`)
* Fix incorrect resolution of tables and views in an Iceberg REST catalog when
  case-insensitive name matching is enabled. ({issue}`30747`)
* Fix incorrect memory accounting for queries reading tables with equality
  delete files. ({issue}`29955`)
* Fix incorrect memory accounting when reading tables with deletion
  vectors. ({issue}`30876`)
* Fix potential worker out-of-memory errors when writes to sorted tables
  fail. ({issue}`31018`)
* Fix incorrect results in `$history` metadata tables, which listed snapshots
  that never became current and reported the commit time instead of the time the
  snapshot became current. ({issue}`31102`)
* Fix failure when dropping a table or otherwise accessing the table location
  with vended credentials from an Iceberg REST catalog, such as
  BigLake. ({issue}`31217`)
* Fix query failure when the `optimize_metadata_queries` session property is
  enabled and the query filters on the `$path`, `$partition`, or
  `$file_modified_time` hidden columns. ({issue}`31103`)
* Fix duplicate rows in materialized views after a refresh when source tables
  are modified during the refresh, or when the view reads a source table with
  `FOR VERSION AS OF`. ({issue}`30990`)
* Fix excessive memory usage and out-of-memory errors during `UPDATE`, `DELETE`,
  and `MERGE` on tables with many data files. ({issue}`31258`)
* Fix duplicate or lost rows in materialized views when refreshes run
  concurrently. ({issue}`31257`)
* Fix failure when reading ORC files where a large `varchar` or `varbinary`
  value is followed only by null or empty values until the end of a
  stripe. ({issue}`10113`)
* Fix failure when running `OPTIMIZE` or writing to sorted tables that contain
  large `varchar` or `varbinary` values. ({issue}`20164`, {issue}`28636`)
* Fix failure when selecting a field of a `row` column that is used as an
  equality delete key. ({issue}`25720`)
* Fix writing Parquet files smaller than the target file size when writing
  low-cardinality data. ({issue}`31291`)
* Fix missing columns and comments in `information_schema` and `system.metadata`
  listings of a schema when a table in the schema fails to load in the Glue
  catalog. ({issue}`30926`)
* Fix duplicate rows when the commit of a write is retried with `retry-policy`
  set to `TASK`. ({issue}`31246`)
* Fix incorrect results when reading tables with equality delete files whose key
  includes a field nested in a `row` column. ({issue}`31376`)
* Fix potential loss of table metadata when a `CREATE TABLE` commit to the Hive
  metastore or AWS Glue fails after the table was created. ({issue}`31211`)
* Fix failure when querying the `$files` or `$partitions` metadata tables of a
  table with `bucket` or `truncate` partition evolution on the same
  column. ({issue}`31435`)
* Fix failure when accessing tables using Azure vended credentials from REST
  catalogs that key SAS tokens by storage host. ({issue}`29526`)
* Fix failure when reading nested arrays from Parquet files that use the legacy
  list encoding. ({issue}`27766`)
* Fix incorrect results for joins on tables partitioned with the `bucket`
  transform on a `decimal` column. ({issue}`31457`)
* Fix failure of concurrent writes to tables in a Hive metastore when
  `iceberg.hive-catalog.locking-enabled` is set to `false`. ({issue}`27942`)
* Fix failure of writes to tables in AWS Glue when Glue returns an error for an
  update it already applied. ({issue}`31213`)

## Ignite connector

* Fix query failure for joins with an `IS NOT DISTINCT FROM` condition when
  complex join pushdown is disabled. ({issue}`31097`)
* Fix leak of temporary tables in the remote database when a query writing to a
  table fails or is canceled. ({issue}`31333`)

## Lakehouse connector

* Add support for procedures such as `system.sync_partition_metadata` and
  `system.vacuum`, and table procedures such as `optimize` and
  `expire_snapshots`. ({issue}`26753`, {issue}`26754`)
* Add support for sorting data files of Iceberg tables with a custom sort order
  using the `sorted_by` parameter of the `optimize` table
  procedure. ({issue}`31274`)
* Add support for writing Delta Lake data files under hash-based directory
  prefixes with the `object_store_layout_enabled` table
  property. ({issue}`24199`)
* Add support for configuring the AWS KMS connection used to read tables with
  encrypted Parquet files with the `aws.kms.region`, `aws.kms.endpoint`,
  `aws.kms.iam-role`, `aws.kms.access-key`, `aws.kms.secret-key`, and related
  configuration properties. ({issue}`30371`)
* Add support for using `CRC32`, `CRC32C`, `SHA1`, or `SHA256` checksums when
  writing to S3 with the `s3.checksum-algorithm` configuration
  property. ({issue}`31523`)
* Add support for writing to S3-compatible storage that rejects the HTTP
  expect-continue handshake by setting the `s3.expect-continue-enabled`
  configuration property to `false`. ({issue}`30534`)
* {{breaking}} Require setting the `fs.cache.directories`, `fs.cache.max-sizes`,
  `fs.cache.max-disk-usage-percentages`, `fs.cache.ttl`, and
  `fs.cache.page-size` properties in the `alluxio` cache manager of the
  cluster-wide [file system cache](/object-storage/file-system-cache) instead of
  in catalog properties files. Catalogs only set
  `fs.cache.enabled`. ({issue}`29184`)
* {{breaking}} Set the default value of the `hive.storage-format` configuration
  property to `PARQUET`. The previous behavior can be restored by setting
  `hive.storage-format` to `ORC`. ({issue}`30818`)
* {{breaking}} Require the `iceberg.rest-catalog.google-json-key-file-path` or
  `iceberg.rest-catalog.google-json-key` configuration property instead of
  `gcs.json-key-file-path` to authenticate with an Iceberg REST catalog using
  `GOOGLE` security. ({issue}`31240`)
* {{breaking}} Remove the `iceberg.equality-deletes-blocks-hash-enabled`
  configuration property. ({issue}`31286`)
* Improve performance of writing Parquet files. ({issue}`30502`)
* Improve performance of `MERGE`, `UPDATE`, and `DELETE` statements and of
  reading Delta Lake tables with deletion vectors. ({issue}`30976`)
* Improve performance of queries with filters on Iceberg tables with Parquet
  files that contain column indexes. ({issue}`11000`)
* Improve performance of queries with selective filters on Parquet files. This
  can be disabled with the `parquet.selected-positions-pushdown-enabled`
  configuration property. ({issue}`30303`)
* Reduce memory usage when reading `row` columns from Parquet
  files. ({issue}`31381`)
* Fix worker out-of-memory errors due to under-accounted memory usage when
  writing ORC files. ({issue}`30771`)
* Fix failure when reading null `row` or `map` values that contain `variant`
  fields from Parquet files. ({issue}`30613`)
* Fix incorrect memory accounting for queries reading Iceberg tables with
  equality delete files. ({issue}`29955`)
* Fix incorrect memory accounting for `MERGE`, `UPDATE`, and `DELETE` statements
  on Delta Lake tables. ({issue}`29956`)
* Fix incorrect memory accounting when reading Iceberg tables with deletion
  vectors. ({issue}`30876`)
* Fix potential worker out-of-memory errors when writes to sorted tables
  fail. ({issue}`31018`)
* Fix failure when reading ORC files where a large `varchar` or `varbinary`
  value is followed only by null or empty values until the end of a
  stripe. ({issue}`10113`)
* Fix failure when running `OPTIMIZE` or writing to sorted Iceberg tables that
  contain large `varchar` or `varbinary`
  values. ({issue}`20164`, {issue}`28636`)
* Fix failure when creating a view with the `extra_properties` view
  property. ({issue}`31265`)
* Fix writing Parquet files smaller than the target file size when writing
  low-cardinality data. ({issue}`31291`)
* Fix incorrect results when filtering on `timestamp` columns in Parquet files
  with UTC-adjusted timestamps when `hive.parquet.time-zone` is set to a time
  zone other than UTC. ({issue}`31050`)
* Fix failure when accessing tables using Azure vended credentials from REST
  catalogs that key SAS tokens by storage host. ({issue}`29526`)
* Fix failure when reading nested arrays from Parquet files that use the legacy
  list encoding. ({issue}`27766`)

## MariaDB connector

* Improve performance of queries with `IS NOT NULL` predicates on `char` and
  `varchar` columns. ({issue}`31060`)
* Fix leak of temporary tables in the remote database when a query writing to a
  table fails or is canceled. ({issue}`31333`)

## Memory connector

* Fix incorrect results for queries with multiple aggregations containing a
  `DISTINCT` clause over a table sampled with `TABLESAMPLE`. ({issue}`31203`)

## MySQL connector

* Improve performance of queries with `IS NOT NULL` predicates on `char` and
  `varchar` columns. ({issue}`31060`)
* Improve performance of retrieving column metadata for tables with many
  rows. ({issue}`31283`)
* Fix leak of temporary tables in the remote database when a query writing to a
  table fails or is canceled. ({issue}`31333`)

## OpenSearch connector

* Fix failure when listing columns if an index contains fields with unsupported
  metadata. ({issue}`30852`)

## Oracle connector

* Fix failure when reading `NUMBER` columns without a declared precision, such
  as results of `COUNT(*)` or `SUM`, with the `query` table
  function. ({issue}`30467`)
* Fix incorrect results when a cast from `char` to `varchar` is pushed down to
  Oracle and the `deprecated.legacy-varchar-to-char-coercion` configuration
  property or the `legacy_varchar_to_char_coercion` session property is
  enabled. ({issue}`30683`)
* Fix leak of temporary tables in the remote database when a query writing to a
  table fails or is canceled. ({issue}`31333`)

## PostgreSQL connector

* Fix query failure for joins with an `IS NOT DISTINCT FROM` condition when
  complex join pushdown is disabled. ({issue}`31097`)
* Fix leak of temporary tables in the remote database when a query writing to a
  table fails or is canceled. ({issue}`31333`)

## Redshift connector

* Fix query failure for joins with dynamic filtering when the
  `redshift.unload-location` configuration property is set. ({issue}`30209`)
* Fix leak of temporary tables in the remote database when a query writing to a
  table fails or is canceled. ({issue}`31333`)

## SingleStore connector

* Improve performance of queries with `IS NOT NULL` predicates on `char` and
  `varchar` columns. ({issue}`31060`)
* Fix leak of temporary tables in the remote database when a query writing to a
  table fails or is canceled. ({issue}`31333`)

## Snowflake connector

* Improve performance of queries with `IS NOT NULL` predicates on `char` and
  `varchar` columns. ({issue}`31060`)
* Fix leak of temporary tables in the remote database when a query writing to a
  table fails or is canceled. ({issue}`31333`)

## SQL Server connector

* Improve performance of queries with `IS NOT NULL` predicates on `char` and
  `varchar` columns. ({issue}`31060`)
* Fix worker threads hanging indefinitely when connecting to an unresponsive SQL
  Server. ({issue}`30786`)
* Fix leak of temporary tables in the remote database when a query writing to a
  table fails or is canceled. ({issue}`31333`)
* Fix incorrect results when comparing `char` or `varchar` values that differ
  only in case or trailing spaces. ({issue}`30480`)

## SPI

* Add the blob cache manager SPI for plugins that provide file caching to
  connectors. ({issue}`29184`)
* Add support for procedures that return metrics as `Map<String,
  Long>`. ({issue}`30749`)
* Add `ConnectorMetadata.getMaterializedViewTableHandleForExecute()` to support
  `ALTER MATERIALIZED VIEW ... EXECUTE` in connectors. ({issue}`30868`)
* Add a `ConnectorMetadata.createView` method that takes a `SaveMode`, and
  deprecate the method that takes a `replace` flag. ({issue}`28076`)
* {{breaking}} Change `BooleanType` to use the bit-packed `BitArrayBlock`
  instead of `ByteArrayBlock`. Plugins that construct or read `boolean` blocks
  directly must be updated. ({issue}`30299`)
* Disallow time zone offsets outside the range of `-14:00` to `+14:00` in
  `DateTimeEncoding.packTimeWithTimeZone` and the `LongTimeWithTimeZone`
  constructor. ({issue}`9288`)
* {{breaking}} Change the return type of `DictionaryBlock.compact()` and
  `DictionaryBlock.compactRelatedBlocks()` to `Block`, as compaction can return
  a simpler block representation. ({issue}`30536`)
* Deprecate `TrinoPrincipal.getName()`. Use `TrinoPrincipal.getPrincipalName()`
  instead, which preserves the case of user names. ({issue}`30567`)
* {{breaking}} Remove `ConnectorPageSourceProvider.getMemoryUsage()` and add a
  `MemoryContext` parameter to
  `ConnectorPageSourceProviderFactory.createPageSourceProvider()` for reporting
  memory shared across page sources. ({issue}`29955`)
* {{breaking}} Add a `MemoryContext` parameter to
  `ConnectorPageSinkProvider.createMergeSink()` and remove the overload without
  it. ({issue}`29956`)
* {{breaking}} Remove JSON serialization support from `TrinoPrincipal` and
  `RoleGrant`. ({issue}`30610`)
* {{breaking}} Change the return type of `ConnectorMetadata.finishMerge()` to
  `Optional<ConnectorOutputMetadata>`. ({issue}`30934`)
* {{breaking}} Add frame exclusion parameters to `WindowFunction.processRow`.
  Plugins that implement window functions must be updated. ({issue}`30226`)
* {{breaking}} Require plugins to use `io.trino.json.Json` instead of `Slice`
  for native `json` values. ({issue}`30341`)
* {{breaking}} Change the values of the `StandardTypes.INTERVAL_DAY_TO_SECOND`
  and `StandardTypes.INTERVAL_YEAR_TO_MONTH` constants and store day-time
  interval values in microseconds. Plugins must be recompiled. ({issue}`6754`)
