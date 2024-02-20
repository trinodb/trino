# Release 327 (20 Dec 2019)

## General

- Fix join query failure when late materialization is enabled. ({issue}`2144`)
- Fix failure of {func}`word_stem` for certain inputs. ({issue}`2145`)
- Fix query failure when using `transform_values()` inside `try()` and the transformation fails
  for one of the rows. ({issue}`2315`)
- Fix potential incorrect results for aggregations involving `FILTER (WHERE ...)`
  when the condition is a reference to a table column. ({issue}`2267`)
- Allow renaming views with {doc}`/sql/alter-view`. ({issue}`1060`)
- Add `error_type` and `error_code` columns to `system.runtime.queries`. ({issue}`2249`)
- Rename `experimental.work-processor-pipelines` configuration property to `experimental.late-materialization.enabled`
  and rename `work_processor_pipelines` session property to `late_materialization`. ({issue}`2275`)

## Security

- Allow using multiple system access controls. ({issue}`2178`)
- Add {doc}`/security/password-file`. ({issue}`797`)

## Hive connector

- Fix incorrect query results when reading `timestamp` values from ORC files written by
  Hive 3.1 or later. ({issue}`2099`)
- Fix a CDH 5.x metastore compatibility issue resulting in failure when analyzing or inserting
  into a table with `date` columns. ({issue}`556`)
- Reduce number of metastore calls when fetching partitions. ({issue}`1921`)
- Support reading from insert-only transactional tables. ({issue}`576`)
- Deprecate `parquet.fail-on-corrupted-statistics` (previously known as `hive.parquet.fail-on-corrupted-statistics`).
  Setting this configuration property to `false` may hide correctness issues, leading to incorrect query results.
  Session property `parquet_fail_with_corrupted_statistics` is deprecated as well.
  Both configuration and session properties will be removed in a future version. ({issue}`2129`)
- Improve concurrency when updating table or partition statistics. ({issue}`2154`)
- Add support for renaming views. ({issue}`2189`)
- Allow configuring the `hive.orc.use-column-names` config property on a per-session
  basis using the `orc_use_column_names` session property. ({issue}`2248`)

## Kudu connector

- Support predicate pushdown for the `decimal` type. ({issue}`2131`)
- Fix column position swap for delete operations that may result in deletion of the wrong records. ({issue}`2252`)
- Improve predicate pushdown for queries that match a column against
  multiple values (typically using the `IN` operator). ({issue}`2253`)

## MongoDB connector

- Add support for reading from views. ({issue}`2156`)

## PostgreSQL connector

- Allow converting unsupported types to `VARCHAR` by setting the session property
  `unsupported_type_handling` or configuration property `unsupported-type-handling`
  to `CONVERT_TO_VARCHAR`. ({issue}`1182`)

## MySQL connector

- Fix `INSERT` query failure when `GTID` mode is enabled. ({issue}`2251`)

## Elasticsearch connector

- Improve performance for queries involving equality and range filters
  over table columns. ({issue}`2310`)

## Google Sheets connector

- Fix incorrect results when listing tables in `information_schema`. ({issue}`2118`)

## SPI

- Add `executionTime` to `QueryStatistics` for event listeners. ({issue}`2247`)
