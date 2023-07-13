# Release 352 (9 Feb 2021)

## General

* Add support for [`WINDOW` clause](window-clause). ({issue}`651`)
* Add support for {doc}`/sql/update`. ({issue}`5861`)
* Add {func}`version` function. ({issue}`4627`)
* Allow prepared statement parameters for `SHOW STATS`. ({issue}`6582`)
* Update tzdata version to 2020d. As a result, queries can no longer reference the 
  `US/Pacific-New` zone, as it has been removed. ({issue}`6660`)
* Add `plan-with-table-node-partitioning` feature config that corresponds to
  existing `plan_with_table_node_partitioning` session property. ({issue}`6811`)
* Improve performance of queries using {func}`rank()` window function. ({issue}`6333`)
* Improve performance of {func}`sum` and {func}`avg` for `decimal` types. ({issue}`6951`)
* Improve join performance. ({issue}`5981`)
* Improve query planning time for queries using range predicates or large `IN` lists. ({issue}`6544`)
* Fix window and streaming aggregation semantics regarding peer rows. Now peer rows are
  grouped using `IS NOT DISTINCT FROM` instead of the `=` operator. ({issue}`6472`) 
* Fix query failure when using an element of `array(timestamp(p))` in a complex expression 
  for `p` greater than 6. ({issue}`6350`)
* Fix failure when using geospatial functions in a join clause and `spatial_partitioning_table_name` is set. ({issue}`6587`)
* Fix `CREATE TABLE AS` failure when source table has hidden columns. ({issue}`6835`)

## Security

* Allow configuring HTTP client used for OAuth2 authentication. ({issue}`6600`)
* Add token polling client API for OAuth2 authentication. ({issue}`6625`)
* Support JWK with certificate chain for OAuth2 authorization. ({issue}`6428`)
* Add scopes to OAuth2 configuration. ({issue}`6580`)
* Optionally verify JWT audience (`aud`) field for OAuth2 authentication. ({issue}`6501`)
* Guard against replay attacks in OAuth2 by using `nonce` cookie when `openid` scope is requested. ({issue}`6580`)

## JDBC driver

* Add OAuth2 authentication. ({issue}`6576`)
* Support user impersonation when using password-based authentication
  using the new `sessionUser` parameter. ({issue}`6549`)

## Docker image

* Remove support for configuration directory `/usr/lib/trino/etc`. The configuration 
  should be provided in `/etc/trino`. ({issue}`6497`)

## CLI

* Support user impersonation when using password-based authentication using the
  `--session-user` command line option. ({issue}`6567`)

## BigQuery connector

* Add a `view_definition` system table which exposes BigQuery view definitions. ({issue}`3687`)
* Fix query failure when calculating `count(*)` aggregation on a view more than once, 
  without any filter. ({issue}`6706`).

## Hive connector

* Add `UPDATE` support for ACID tables. ({issue}`5861`)
* Match columns by index rather than by name by default for ORC ACID tables. ({issue}`6479`)
* Match columns by name rather than by index by default for Parquet files.
  This can be changed using `hive.parquet.use-column-names` configuration property and `parquet_use_column_names`
  session property. ({issue}`6479`)
* Remove the `hive.partition-use-column-names` configuration property and the
  `partition_use_column_names ` session property. This is now determined automatically. ({issue}`6479`)
* Support timestamps with microsecond or nanosecond precision (as configured with
  `hive.timestamp-precision` property) nested within `array`, `map` or `struct` data types. ({issue}`5195`)
* Support reading from table in Sequencefile format that uses LZO compression. ({issue}`6452`)
* Expose AWS HTTP Client stats via JMX. ({issue}`6503`)
* Allow specifying S3 KMS Key ID used for client side encryption via security mapping 
  config and extra credentials. ({issue}`6802`)
* Fix writing incorrect `timestamp` values within `row`, `array` or `map` when using Parquet file format. ({issue}`6760`)
* Fix possible S3 connection leak on query failure. ({issue}`6849`)

## Iceberg connector

* Add `iceberg.max-partitions-per-writer` config property to allow configuring the limit on partitions per writer. ({issue}`6650`)
* Optimize cardinality-insensitive aggregations ({func}`max`, {func}`min`, {func}`distinct`, {func}`approx_distinct`) 
  over identity partition columns with `optimizer.optimize-metadata-queries` config property 
  or `optimize_metadata_queries` session property. ({issue}`5199`)
* Provide `use_file_size_from_metadata` catalog session property and `iceberg.use-file-size-from-metadata` 
  config property to fix query failures on tables with wrong file sizes stored in the metadata. ({issue}`6369`)
* Fix the mapping of nested fields between table metadata and ORC file metadata. This 
  enables evolution of `row` typed columns for Iceberg tables stored in ORC. ({issue}`6520`)

## Kinesis connector

* Support GZIP message compression. ({issue}`6442`)

## MySQL connector

* Improve performance for certain complex queries involving aggregation and predicates (e.g. `HAVING` clause)
  by pushing the aggregation and predicates computation into the remote database. ({issue}`6667`)
* Improve performance for certain queries using `stddev_pop`, `stddev_samp`, `var_pop`, `var_samp` aggregation 
  functions by pushing the aggregation and predicates computation into the remote database. ({issue}`6673`)

## PostgreSQL connector

* Improve performance for certain complex queries involving aggregation and predicates (e.g. `HAVING` clause)
  by pushing the aggregation and predicates computation into the remote database. ({issue}`6667`)
* Improve performance for certain queries using `stddev_pop`, `stddev_samp`, `var_pop`, `var_samp`,
  `covar_pop`, `covar_samp`, `corr`, `regr_intercept`, `regr_slope` aggregation functions
  by pushing the aggregation and predicates computation into the remote database. ({issue}`6731`)

## Redshift connector

* Use the Redshift JDBC driver to access Redshift. As a result, `connection-url` in catalog 
  configuration files needs to be updated from `jdbc:postgresql:...` to `jdbc:redshift:...`. ({issue}`6465`)

## SQL Server connector

* Avoid query failures due to transaction deadlocks in SQL Server by using transaction snapshot isolation. ({issue}`6274`)
* Honor precision of SQL Server's `datetime2` type . ({issue}`6654`)
* Add support for Trino `timestamp` type in `CREATE TABLE` statement, by mapping it to SQL Server's `datetime2` type.
  Previously, it was incorrectly mapped to SQL Server's `timestamp` type. ({issue}`6654`)
* Add support for the `time` type. ({issue}`6654`)
* Improve performance for certain complex queries involving aggregation and predicates (e.g. `HAVING` clause)
  by pushing the aggregation and predicates computation into the remote database. ({issue}`6667`)
* Fix failure when querying tables having indexes and constraints. ({issue}`6464`)

## SPI

* Add support for join pushdown via the `ConnectorMetadata.applyJoin()` method. ({issue}`6752`)
