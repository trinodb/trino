# Release 335 (14 Jun 2020)

## General

- Fix failure when {func}`reduce_agg` is used as a window function. ({issue}`3883`)
- Fix incorrect cast from `TIMESTAMP` (without time zone) to `TIME` type. ({issue}`3848`)
- Fix incorrect query results when converting very large `TIMESTAMP` values into
  `TIMESTAMP WITH TIME ZONE`, or when parsing very large
  `TIMESTAMP WITH TIME ZONE` values. ({issue}`3956`)
- Return `VARCHAR` type when {func}`substr` argument is `CHAR` type. ({issue}`3599`, {issue}`3456`)
- Improve optimized local scheduling with regard to non-uniform data distribution. ({issue}`3922`)
- Add support for variable-precision `TIMESTAMP` (without time zone) type. ({issue}`3783`)
- Add a variant of {func}`substring` that takes a `CHAR` argument. ({issue}`3949`)
- Add  `information_schema.role_authorization_descriptors` table that returns information about the roles
  granted to principals. ({issue}`3535`)

## Security

- Add schema access rules to {doc}`/security/file-system-access-control`. ({issue}`3766`)

## Web UI

- Fix the value displayed in the worker memory pools bar. ({issue}`3920`)

## Accumulo connector

- The server-side iterators are now in a JAR file named `presto-accumulo-iterators`. ({issue}`3673`)

## Hive connector

- Collect column statistics for inserts into empty tables. ({issue}`2469`)
- Add support for `information_schema.role_authorization_descriptors` table when using the `sql-standard`
  security mode. ({issue}`3535`)
- Allow non-lowercase column names in {ref}`system.sync_partition_metadata<hive-procedures>` procedure. This can be enabled
  by passing `case_sensitive=false` when invoking the procedure. ({issue}`3431`)
- Support caching with secured coordinator. ({issue}`3874`)
- Prevent caching from becoming disabled due to intermittent network failures. ({issue}`3874`)
- Ensure HDFS impersonation is not enabled when caching is enabled. ({issue}`3913`)
- Add `hive.cache.ttl` and `hive.cache.disk-usage-percentage` cache properties. ({issue}`3840`)
- Improve query performance when caching is enabled by scheduling work on nodes with cached data. ({issue}`3922`)
- Add support for `UNIONTYPE`.  This is mapped to `ROW` containing a `tag` field and a field for each data type in the union. For
  example, `UNIONTYPE<INT, DOUBLE>` is mapped to `ROW(tag INTEGER, field0 INTEGER, field1 DOUBLE)`. ({issue}`3483`)
- Make `partition_values` argument to `drop_stats` procedure optional. ({issue}`3937`)
- Add support for dynamic partition pruning to improve performance of complex queries
  over partitioned data. ({issue}`1072`)

## Phoenix connector

- Allow configuring whether `DROP TABLE` is allowed. This is controlled by the new `allow-drop-table`
  catalog configuration property and defaults to `true`, compatible with the previous behavior. ({issue}`3953`)

## SPI

- Add support for aggregation pushdown into connectors via the
  `ConnectorMetadata.applyAggregation()` method. ({issue}`3697`)
