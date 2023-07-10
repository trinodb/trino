# Release 334 (29 May 2020)

## General

- Fix incorrect query results for certain queries involving comparisons of `real` and `double` types
  when values include negative zero. ({issue}`3745`)
- Fix failure when querying an empty table with late materialization enabled. ({issue}`3577`)
- Fix failure when the inputs to `UNNEST` are repeated. ({issue}`3587`)
- Fix failure when an aggregation is used in the arguments to {func}`format`. ({issue}`3829`)
- Fix {func}`localtime` and {func}`current_time` for session zones with DST or with historical offset changes
  in legacy (default) timestamp semantics. ({issue}`3846`, {issue}`3850`)
- Fix dynamic filter failures in complex spatial join queries. ({issue}`3694`)
- Improve performance of queries involving {func}`row_number`. ({issue}`3614`)
- Improve performance of queries containing `LIKE` predicate. ({issue}`3618`)
- Improve query performance when dynamic filtering is enabled. ({issue}`3632`)
- Improve performance for queries that read fields from nested structures. ({issue}`2672`)
- Add variant of {func}`random` function that produces a number in the provided range. ({issue}`1848`)
- Show distributed plan by default in {doc}`/sql/explain`. ({issue}`3724`)
- Add {doc}`/connector/oracle`. ({issue}`1959`)
- Add {doc}`/connector/pinot`. ({issue}`2028`)
- Add {doc}`/connector/prometheus`. ({issue}`2321`)
- Add support for standards compliant ({rfc}`7239`) HTTP forwarded headers. Processing of HTTP forwarded headers is now controlled by the
  `http-server.process-forwarded` configuration property, and the old `http-server.authentication.allow-forwarded-https` and
  `dispatcher.forwarded-header` configuration properties are no longer supported. ({issue}`3714`)
- Add pluggable {doc}`/develop/certificate-authenticator`. ({issue}`3804`)

## JDBC driver

- Implement `toString()` for `java.sql.Array` results. ({issue}`3803`)

## CLI

- Improve rendering of elapsed time for short queries. ({issue}`3311`)

## Web UI

- Add `fixed`, `certificate`, `JWT`, and `Kerberos` to UI authentication. ({issue}`3433`)
- Show join distribution type in Live Plan. ({issue}`1323`)

## JDBC driver

- Improve performance of `DatabaseMetaData.getColumns()` when the
  parameters contain unescaped `%` or `_`. ({issue}`1620`)

## Elasticsearch connector

- Fix failure when executing `SHOW CREATE TABLE`. ({issue}`3718`)
- Improve performance for `count(*)` queries. ({issue}`3512`)
- Add support for raw Elasticsearch queries. ({issue}`3735`)

## Hive connector

- Fix matching bucket filenames without leading zeros. ({issue}`3702`)
- Fix creation of external tables using `CREATE TABLE AS`. Previously, the
  tables were created as managed and with the default location. ({issue}`3755`)
- Fix incorrect table statistics for newly created external tables. ({issue}`3819`)
- Prevent Presto from starting when cache fails to initialize. ({issue}`3749`)
- Fix race condition that could cause caching to be permanently disabled. ({issue}`3729`, {issue}`3810`)
- Fix malformed reads when asynchronous read mode for caching is enabled. ({issue}`3772`)
- Fix eviction of cached data while still under size eviction threshold. ({issue}`3772`)
- Improve performance when creating unpartitioned external tables over large data sets. ({issue}`3624`)
- Leverage Parquet file statistics when reading decimal columns. ({issue}`3581`)
- Change type of `$file_modified_time` hidden column from `bigint` to `timestamp with timezone type`. ({issue}`3611`)
- Add caching support for HDFS and Azure file systems. ({issue}`3772`)
- Fix S3 connection pool depletion when asynchronous read mode for caching is enabled. ({issue}`3772`)
- Disable caching on coordinator by default. ({issue}`3820`)
- Use asynchronous read mode for caching by default. ({issue}`3799`)
- Cache delegation token for Hive thrift metastore. This can be configured with
  the `hive.metastore.thrift.delegation-token.cache-ttl` and `hive.metastore.thrift.delegation-token.cache-maximum-size`
  configuration properties. ({issue}`3771`)

## MemSQL connector

- Include {doc}`/connector/singlestore` in the server tarball and RPM. ({issue}`3743`)

## MongoDB connector

- Support case insensitive database and collection names. This can be enabled with the
  `mongodb.case-insensitive-name-matching` configuration property. ({issue}`3453`)

## SPI

- Allow a `SystemAccessControl` to provide an `EventListener`. ({issue}`3629`).
