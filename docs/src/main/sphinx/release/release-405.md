# Release 405 (28 Dec 2022)

## General

* Add Trino version to the output of `EXPLAIN`. ({issue}`15317`)
* Add task input/output size distribution to the output of
  `EXPLAIN ANALYZE VERBOSE`. ({issue}`15286`)
* Add stage skewness warnings to the output of `EXPLAIN ANALYZE`. ({issue}`15286`)
* Add support for `ALTER COLUMN ... SET DATA TYPE` statement. ({issue}`11608`)
* Allow configuring a refresh interval for the database resource group manager
  with the `resource-groups.refresh-interval` configuration property. ({issue}`14514`)
* Improve performance of queries that compare `date` columns with
  `timestamp(n) with time zone` literals. ({issue}`5798`)
* Improve performance and resource utilization when inserting into tables. ({issue}`14718`, {issue}`14874`)
* Improve performance for `INSERT` queries when fault-tolerant execution is
  enabled. ({issue}`14735`)
* Improve planning performance for queries with many `GROUP BY` clauses. ({issue}`15292`)
* Improve query performance for large clusters and skewed queries. ({issue}`15369`)
* Rename the `node-scheduler.max-pending-splits-per-task` configuration property
  to `node-scheduler.min-pending-splits-per-task`. ({issue}`15168`)
* Ensure that the configured number of task retries is not larger than 126. ({issue}`14459`)
* Fix incorrect rounding of `time(n)` and `time(n) with time zone` values near
  the top of the range of allowed values. ({issue}`15138`)
* Fix incorrect results for queries involving window functions without a
  `PARTITION BY` clause followed by the evaluation of window functions with a
  `PARTITION BY` and `ORDER BY` clause. ({issue}`15203`)
* Fix incorrect results when adding or subtracting an `interval` from a
  `timestamp with time zone`. ({issue}`15103`)
* Fix potential incorrect results when joining tables on indexed and non-indexed
  columns at the same time. ({issue}`15334`)
* Fix potential failure of queries involving `MATCH_RECOGNIZE`. ({issue}`15343`)
* Fix incorrect reporting of `Projection CPU time` in the output of `EXPLAIN
  ANALYZE VERBOSE`. ({issue}`15364`)
* Fix `SET TIME ZONE LOCAL` to correctly reset to the initial time zone of the
  client session. ({issue}`15314`)

## Security

* Add support for string replacement as part of
  [impersonation rules](system-file-auth-impersonation-rules). ({issue}`14962`)
* Add support for fetching access control rules via HTTPS. ({issue}`14008`)
* Fix some `system.metadata` tables improperly showing the names of catalogs
  which the user cannot access. ({issue}`14000`)
* Fix `USE` statement improperly disclosing the names of catalogs and schemas
  which the user cannot access. ({issue}`14208`)
* Fix improper HTTP redirect after OAuth 2.0 token refresh. ({issue}`15336`)

## Web UI

* Display operator CPU time in the "Stage Performance" tab. ({issue}`15339`)

## JDBC driver

* Return correct values in `NULLABLE` columns of the
  `DatabaseMetaData.getColumns` result. ({issue}`15214`)

## BigQuery connector

* Improve read performance with experimental support for [Apache Arrow](https://arrow.apache.org/docs/)
  serialization when reading from BigQuery. This can be enabled with the
  `bigquery.experimental.arrow-serialization.enabled` catalog configuration
  property. ({issue}`14972`)
* Fix queries incorrectly executing with the project ID specified in the
  credentials instead of the project ID specified in the `bigquery.project-id`
  catalog property. ({issue}`14083`)

## Delta Lake connector

* Add support for views. ({issue}`11609`)
* Add support for configuring batch size for reads on Parquet files using the
  `parquet.max-read-block-row-count` configuration property or the
  `parquet_max_read_block_row_count` session property. ({issue}`15474`)
* Improve performance and reduce storage requirements when running the `vacuum`
  procedure on S3-compatible storage. ({issue}`15072`)
* Improve memory accounting for `INSERT`, `MERGE`, and
  `CREATE TABLE ... AS SELECT` queries. ({issue}`14407`)
* Improve performance of reading Parquet files for `boolean`, `tinyint`,
  `short`, `int`, `long`, `float`, `double`, `short decimal`, `UUID`, `time`,
  `decimal`, `varchar`, and `char` data types. This optimization can be disabled
  with the `parquet.optimized-reader.enabled` catalog configuration property. ({issue}`14423`, {issue}`14667`)
* Improve query performance when the `nulls fraction` statistic is not available
  for some columns. ({issue}`15132`)
* Improve performance when reading Parquet files. ({issue}`15257`, {issue}`15474`)
* Improve performance of reading Parquet files for queries with filters. ({issue}`15268`)
* Improve `DROP TABLE` performance for tables stored on AWS S3. ({issue}`13974`)
* Improve performance of reading Parquet files for `timestamp` and
  `timestamp with timezone` data types. ({issue}`15204`)
* Improve performance of queries that read a small number of columns and queries
  that process tables with large Parquet row groups or ORC stripes. ({issue}`15168`)
* Improve stability and reduce peak memory requirements when reading from
  Parquet files. ({issue}`15374`)
* Allow registering existing table files in the metastore with the new
  [`register_table` procedure](delta-lake-register-table). ({issue}`13568`)
* Deprecate creating a new table with existing table content. This can be
  re-enabled using the `delta.legacy-create-table-with-existing-location.enabled`
  configuration property or the
  `legacy_create_table_with_existing_location_enabled` session property. ({issue}`13568`)
* Fix query failure when reading Parquet files with large row groups. ({issue}`5729`)
* Fix `DROP TABLE` leaving files behind when using managed tables stored on S3
  and created by the Databricks runtime. ({issue}`13017`)
* Fix query failure when the path contains special characters. ({issue}`15183`)
* Fix potential `INSERT` failure for tables stored on S3. ({issue}`15476`)

## Google Sheets connector

* Add support for setting a read timeout with the `gsheets.read-timeout`
  configuration property. ({issue}`15322`)
* Add support for `base64`-encoded credentials using the
  `gsheets.credentials-key` configuration property. ({issue}`15477`)
* Rename the `credentials-path` configuration property to
  `gsheets.credentials-path`, `metadata-sheet-id` to
  `gsheets.metadata-sheet-id`, `sheets-data-max-cache-size` to
  `gsheets.max-data-cache-size`, and `sheets-data-expire-after-write` to
  `gsheets.data-cache-ttl`. ({issue}`15042`)

## Hive connector

* Add support for referencing nested fields in columns with the `UNIONTYPE` Hive
  type. ({issue}`15278`)
* Add support for configuring batch size for reads on Parquet files using the
  `parquet.max-read-block-row-count` configuration property or the
  `parquet_max_read_block_row_count` session property. ({issue}`15474`)
* Improve memory accounting for `INSERT`, `MERGE`, and `CREATE TABLE AS SELECT`
  queries. ({issue}`14407`)
* Improve performance of reading Parquet files for `boolean`, `tinyint`,
  `short`, `int`, `long`, `float`, `double`, `short decimal`, `UUID`, `time`,
  `decimal`, `varchar`, and `char` data types. This optimization can be disabled
  with the `parquet.optimized-reader.enabled` catalog configuration property. ({issue}`14423`, {issue}`14667`)
* Improve performance for queries which write data into multiple partitions. ({issue}`15241`, {issue}`15066`)
* Improve performance when reading Parquet files. ({issue}`15257`, {issue}`15474`)
* Improve performance of reading Parquet files for queries with filters. ({issue}`15268`)
* Improve `DROP TABLE` performance for tables stored on AWS S3. ({issue}`13974`)
* Improve performance of reading Parquet files for `timestamp` and
  `timestamp with timezone` data types. ({issue}`15204`)
* Improve performance of queries that read a small number of columns and queries
  that process tables with large Parquet row groups or ORC stripes. ({issue}`15168`)
* Improve stability and reduce peak memory requirements when reading from
  Parquet files. ({issue}`15374`)
* Disallow creating transactional tables when not using the Hive metastore. ({issue}`14673`)
* Fix query failure when reading Parquet files with large row groups. ({issue}`5729`)
* Fix incorrect `schema already exists` error caused by a client timeout when
  creating a new schema. ({issue}`15174`)
* Fix failure when an access denied exception happens while listing tables or
  views in a Glue metastore. ({issue}`14746`)
* Fix `INSERT` failure on ORC ACID tables when Apache Hive 3.1.2 is used as a
  metastore. ({issue}`7310`)
* Fix failure when reading Hive views with `char` types. ({issue}`15470`)
* Fix potential `INSERT` failure for tables stored on S3. ({issue}`15476`)

## Hudi connector

* Improve performance of reading Parquet files for `boolean`, `tinyint`,
  `short`, `int`, `long`, `float`, `double`, `short decimal`, `UUID`, `time`,
  `decimal`, `varchar`, and `char` data types. This optimization can be disabled
  with the `parquet.optimized-reader.enabled` catalog configuration property. ({issue}`14423`, {issue}`14667`)
* Improve performance of reading Parquet files for queries with filters. ({issue}`15268`)
* Improve performance of reading Parquet files for `timestamp` and
  `timestamp with timezone` data types. ({issue}`15204`)
* Improve performance of queries that read a small number of columns and queries
  that process tables with large Parquet row groups or ORC stripes. ({issue}`15168`)
* Improve stability and reduce peak memory requirements when reading from
  Parquet files. ({issue}`15374`)
* Fix query failure when reading Parquet files with large row groups. ({issue}`5729`)

## Iceberg connector

* Add support for configuring batch size for reads on Parquet files using the
  `parquet.max-read-block-row-count` configuration property or the
  `parquet_max_read_block_row_count` session property. ({issue}`15474`)
* Add support for the Iceberg REST catalog. ({issue}`13294`)
* Improve memory accounting for `INSERT`, `MERGE`, and `CREATE TABLE AS SELECT`
  queries. ({issue}`14407`)
* Improve performance of reading Parquet files for `boolean`, `tinyint`,
  `short`, `int`, `long`, `float`, `double`, `short decimal`, `UUID`, `time`,
  `decimal`, `varchar`, and `char` data types. This optimization can be disabled
  with the `parquet.optimized-reader.enabled` catalog configuration property. ({issue}`14423`, {issue}`14667`)
* Improve performance when reading Parquet files. ({issue}`15257`, {issue}`15474`)
* Improve performance of reading Parquet files for queries with filters. ({issue}`15268`)
* Improve `DROP TABLE` performance for tables stored on AWS S3. ({issue}`13974`)
* Improve performance of reading Parquet files for `timestamp` and
  `timestamp with timezone` data types. ({issue}`15204`)
* Improve performance of queries that read a small number of columns and queries
  that process tables with large Parquet row groups or ORC stripes. ({issue}`15168`)
* Improve stability and reduce peak memory requirements when reading from
  Parquet files. ({issue}`15374`)
* Fix incorrect results when predicates over `row` columns on Parquet files are
  pushed into the connector. ({issue}`15408`)
* Fix query failure when reading Parquet files with large row groups. ({issue}`5729`)
* Fix `REFRESH MATERIALIZED VIEW` failure when the materialized view is based on
  non-Iceberg tables. ({issue}`13131`)
* Fix failure when an access denied exception happens while listing tables or
  views in a Glue metastore. ({issue}`14971`)
* Fix potential `INSERT` failure for tables stored on S3. ({issue}`15476`)

## Kafka connector

* Add support for [Protobuf encoding](kafka-protobuf-encoding). ({issue}`14734`)

## MongoDB connector

* Add support for [fault-tolerant execution](/admin/fault-tolerant-execution). ({issue}`15062`)
* Add support for setting a file path and password for the truststore and
  keystore. ({issue}`15240`)
* Add support for case-insensitive name-matching in the `query` table function. ({issue}`15329`)
* Rename the `mongodb.ssl.enabled` configuration property to
  `mongodb.tls.enabled`. ({issue}`15240`)
* Upgrade minimum required MongoDB version to
  [4.2](https://www.mongodb.com/docs/manual/release-notes/4.2/). ({issue}`15062`)
* Delete a MongoDB field from collections when dropping a column.
  Previously, the connector deleted only metadata. ({issue}`15226`)
* Remove deprecated `mongodb.seeds` and `mongodb.credentials` configuration
  properties. ({issue}`15263`)
* Fix failure when an unauthorized exception happens while listing schemas or
  tables. ({issue}`1398`)
* Fix `NullPointerException` when a column name contains uppercase characters in
  the `query` table function. ({issue}`15294`)
* Fix potential incorrect results when the `objectid` function is used more than
  once within a single query. ({issue}`15426`)

## MySQL connector

* Fix failure when the `query` table function contains a `WITH` clause. ({issue}`15332`)

## PostgreSQL connector

* Fix query failure when a `FULL JOIN` is pushed down. ({issue}`14841`)

## Redshift connector

* Add support for aggregation, join, and `ORDER BY ... LIMIT` pushdown. ({issue}`15365`)
* Add support for `DELETE`. ({issue}`15365`)
* Add schema, table, and column name length checks. ({issue}`15365`)
* Add full type mapping for Redshift types. The previous behavior can be
  restored via the `redshift.use-legacy-type-mapping` configuration property. ({issue}`15365`)

## SPI

* Remove deprecated `ConnectorNodePartitioningProvider.getBucketNodeMap()`
  method. ({issue}`14067`)
* Use the `MERGE` APIs in the engine to execute `DELETE` and `UPDATE`.
  Require connectors to implement `beginMerge()` and related APIs.
  Deprecate `beginDelete()`, `beginUpdate()` and `UpdatablePageSource`, which
  are unused and do not need to be implemented. ({issue}`13926`)
