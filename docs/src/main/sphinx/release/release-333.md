# Release 333 (04 May 2020)

## General

- Fix planning failure when lambda expressions are repeated in a query. ({issue}`3218`)
- Fix failure when input to `TRY` is a constant `NULL`. ({issue}`3408`)
- Fix failure for {doc}`/sql/show-create-table` for tables with
  row types that contain special characters. ({issue}`3380`)
- Fix failure when using {func}`max_by` or {func}`min_by`
  where the second argument is of type `varchar`. ({issue}`3424`)
- Fix rare failure due to an invalid size estimation for T-Digests. ({issue}`3625`)
- Do not require coordinator to have spill paths setup when spill is enabled. ({issue}`3407`)
- Improve performance when dynamic filtering is enabled. ({issue}`3413`)
- Improve performance of queries involving constant scalar subqueries ({issue}`3432`)
- Allow overriding the count of available workers used for query cost
  estimation via the `cost_estimation_worker_count` session property. ({issue}`2705`)
- Add data integrity verification for Presto internal communication. This can be configured
  with the `exchange.data-integrity-verification` configuration property. ({issue}`3438`)
- Add support for `LIKE` predicate to {doc}`/sql/show-columns`. ({issue}`2997`)
- Add {doc}`/sql/show-create-schema`. ({issue}`3099`)
- Add {func}`starts_with` function. ({issue}`3392`)

## Server

- Require running on {ref}`Java 11 or above <requirements-java>`. ({issue}`2799`)

## Server RPM

- Reduce size of RPM and disk usage after installation. ({issue}`3595`)

## Security

- Allow configuring trust certificate for LDAP password authenticator. ({issue}`3523`)

## JDBC driver

- Fix hangs on JDK 8u252 when using secure connections. ({issue}`3444`)

## BigQuery connector

- Improve performance for queries that contain filters on table columns. ({issue}`3376`)
- Add support for partitioned tables. ({issue}`3376`)

## Cassandra connector

- Allow {doc}`/sql/insert` statement for table having hidden `id` column. ({issue}`3499`)
- Add support for {doc}`/sql/create-table` statement. ({issue}`3478`)

## Elasticsearch connector

- Fix failure when querying Elasticsearch 7.x clusters. ({issue}`3447`)

## Hive connector

- Fix incorrect query results when reading Parquet data with a `varchar` column predicate
  which is a comparison with a value containing non-ASCII characters. ({issue}`3517`)
- Ensure cleanup of resources (file descriptors, sockets, temporary files, etc.)
  when an error occurs while writing an ORC file. ({issue}`3390`)
- Generate multiple splits for files in bucketed tables. ({issue}`3455`)
- Make file system caching honor Hadoop properties from `hive.config.resources`. ({issue}`3557`)
- Disallow enabling file system caching together with S3 security mapping or GCS access tokens. ({issue}`3571`)
- Disable file system caching parallel warmup by default.
  It is currently broken and should not be enabled. ({issue}`3591`)
- Include metrics from S3 Select in the S3 JMX metrics. ({issue}`3429`)
- Report timings for request retries in S3 JMX metrics.
  Previously, only the first request was reported. ({issue}`3429`)
- Add S3 JMX metric for client retry pause time (how long the thread was asleep
  between request retries in the client itself). ({issue}`3429`)
- Add support for {doc}`/sql/show-create-schema`. ({issue}`3099`)
- Add `hive.projection-pushdown-enabled` configuration property and
  `projection_pushdown_enabled` session property. ({issue}`3490`)
- Add support for connecting to the Thrift metastore using TLS. ({issue}`3440`)

## MongoDB connector

- Skip unknown types in nested BSON object. ({issue}`2935`)
- Fix query failure when the user does not have access privileges for `system.views`. ({issue}`3355`)

## Other connectors

These changes apply to the MemSQL, MySQL, PostgreSQL, Redshift, and SQL Server connectors.

- Export JMX statistics for various connector operations. ({issue}`3479`).
