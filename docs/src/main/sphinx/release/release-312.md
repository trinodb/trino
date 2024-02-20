# Release 312 (29 May 2019)

## General

- Fix incorrect results for queries using `IS [NOT] DISTINCT FROM`. ({issue}`795`)
- Fix `array_distinct`, `array_intersect` semantics with respect to indeterminate
  values (i.e., `NULL` or structural types containing `NULL`). ({issue}`559`)
- Fix failure when the largest negative `BIGINT` value (`-9223372036854775808`) is used
  as a constant in a query. ({issue}`805`)
- Improve reliability for network errors when using Kerberos with
  {doc}`/security/internal-communication`. ({issue}`838`)
- Improve performance of `JOIN` queries involving inline tables (`VALUES`). ({issue}`743`)
- Improve performance of queries containing duplicate expressions. ({issue}`730`)
- Improve performance of queries involving comparisons between values of different types. ({issue}`731`)
- Improve performance of queries containing redundant `ORDER BY` clauses in subqueries. This may
  affect the semantics of queries that incorrectly rely on implementation-specific behavior. The
  old behavior can be restored via the `skip_redundant_sort` session property or the
  `optimizer.skip-redundant-sort` configuration property. ({issue}`818`)
- Improve performance of `IN` predicates that contain subqueries. ({issue}`767`)
- Improve support for correlated subqueries containing redundant `LIMIT` clauses. ({issue}`441`)
- Add a new {ref}`uuid-type` type to represent UUIDs. ({issue}`755`)
- Add {func}`uuid` function to generate random UUIDs. ({issue}`786`)
- Add {doc}`/connector/phoenix`. ({issue}`672`)
- Make semantic error name available in client protocol. ({issue}`790`)
- Report operator statistics when `experimental.work-processor-pipelines`
  is enabled. ({issue}`788`)

## Server

- Raise required Java version to 8u161. This version allows unlimited strength crypto. ({issue}`779`)
- Show JVM configuration hint when JMX agent fails to start on Java 9+. ({issue}`838`)
- Skip starting JMX agent on Java 9+ if it is already configured via JVM properties. ({issue}`838`)
- Support configuring TrustStore for {doc}`/security/internal-communication` using the
  `internal-communication.https.truststore.path` and `internal-communication.https.truststore.key`
  configuration properties. The path can point at a Java KeyStore or a PEM file. ({issue}`785`)
- Remove deprecated check for minimum number of workers before starting a coordinator.  Use the
  `query-manager.required-workers` and `query-manager.required-workers-max-wait` configuration
  properties instead. ({issue}`95`)

## Hive connector

- Fix `SHOW GRANTS` failure when metastore contains few tables. ({issue}`791`)
- Fix failure reading from `information_schema.table_privileges` table when metastore
  contains few tables. ({issue}`791`)
- Use Hive naming convention for file names when writing to bucketed tables. ({issue}`822`)
- Support new Hive bucketing conventions by allowing any number of files per bucket.
  This allows reading from partitions that were inserted into multiple times by Hive,
  or were written to by Hive on Tez (which does not create files for empty buckets).
- Allow disabling the creation of files for empty buckets when writing data.
  This behavior is enabled by  default for compatibility with previous versions of Presto,
  but can be disabled using the `hive.create-empty-bucket-files` configuration property
  or the `create_empty_bucket_files` session property. ({issue}`822`)

## MySQL connector

- Map MySQL `json` type to Presto `json` type. ({issue}`824`)

## PostgreSQL connector

- Add support for PostgreSQL's `TIMESTAMP WITH TIME ZONE` data type. ({issue}`640`)

## SPI

- Add support for pushing `TABLESAMPLE` into connectors via the
  `ConnectorMetadata.applySample()` method. ({issue}`753`)
