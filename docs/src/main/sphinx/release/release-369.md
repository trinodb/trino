# Release 369 (24 Jan 2022)

## General

* Add support for `Pacific/Kanton` time zone. ({issue}`10679`)
* Display `Physical input read time` using most succinct time unit in `EXPLAIN
  ANALYZE VERBOSE`. ({issue}`10576`)
* Fine tune request retry mechanism in HTTP event listener. ({issue}`10566`)
* Add support for using PostgreSQL and Oracle as backend database for resource
  groups. ({issue}`9812`)
* Remove unnecessary spilling configuration properties `spill-order-by` and
  `spill-window-operator`. ({issue}`10591`)
* Remove distinction between system and user memory to simplify
  cluster configuration. The configuration property
  `query.max-total-memory-per-node` is removed. Use `query.max-memory-per-node`
  instead. ({issue}`10574`)
* Use formatting specified in the SQL standard when casting `double` and `real`
  values to `varchar` type. ({issue}`552`)
* Add support for `ALTER MATERIALIZED VIEW ... SET PROPERTIES`. ({issue}`9613`)
* Add experimental implementation of task level retries. This can be enabled by
  setting the `retry-policy` configuration property or the `retry_policy`
  session property to `task`. ({issue}`9818`)
* Improve query wall time by splitting workload between nodes in a more balanced
  way. Previous workload balancing policy can be restored via
  `node-scheduler.splits-balancing-policy=node`. ({issue}`10660`)
* Prevent hanging query execution on failures with `phased` execution policy.
  ({issue}`10656`)
* Catch overflow in decimal multiplication. ({issue}`10732`)
* Fix `UnsupportedOperationException` in `max_by` and `min_by` aggregation.
  ({issue}`10599`)
* Fix incorrect results or failure when casting date to `varchar(n)` type.
  ({issue}`552`)
* Fix issue where the length of log file names grow indefinitely upon log
  rotation. ({issue}`10738`)

## Security

* Allow extracting groups from OAuth2 claims from
  ``http-server.authentication.oauth2.groups-field``. ({issue}`10262`)

## JDBC driver

* Fix memory leak when using `DatabaseMetaData`. ({issue}`10584`,
  {issue}`10632`)

## BigQuery connector

* Remove ``bigquery.case-insensitive-name-matching.cache-ttl`` configuration
  option. It was previously ignored. ({issue}`10697`)
* Fix query failure when reading columns with `numeric` or `bignumeric` type.
  ({issue}`10564`)

## ClickHouse connector

* Upgrade minimum required version to 21.3. ({issue}`10703`)
* Add support for [renaming schemas](/sql/alter-schema). ({issue}`10558`)
* Add support for setting [column comments](/sql/comment). ({issue}`10641`)
* Map ClickHouse `ipv4` and `ipv6` types to Trino `ipaddress` type.
  ({issue}`7098`)
* Allow mapping ClickHouse `fixedstring` or `string` as Trino `varchar` via the
  `map_string_as_varchar` session property. ({issue}`10601`)
* Disable `avg` pushdown on `decimal` types to avoid incorrect results.
  ({issue}`10650`)
* Fix spurious errors when metadata cache is enabled. ({issue}`10544`,
  {issue}`10512`)

## Druid connector

* Fix spurious errors when metadata cache is enabled. ({issue}`10544`,
  {issue}`10512`)

## Hive connector

* Add support for writing Bloom filters in ORC files. ({issue}`3939`)
* Allow flushing the metadata cache for specific schemas, tables, or partitions
  with the [flush_metadata_cache](hive_flush_metadata_cache) system procedure.
  ({issue}`10385`)
* Add support for long lived AWS Security Token Service (STS) credentials for
  authentication with Glue catalog. ({issue}`10735`)
* Ensure transaction locks in the Hive Metastore are released in case of query
  failure when querying Hive ACID tables.  ({issue}`10401`)
* Disallow reading from Iceberg tables when redirects from Hive to Iceberg are
  not enabled. ({issue}`8693`, {issue}`10441`)
* Improve performance of queries using range predicates when reading ORC files
  with Bloom filters. ({issue}`4108`)
* Support writing Parquet files greater than 2GB. ({issue}`10722`)
* Fix spurious errors when metadata cache is enabled. ({issue}`10646`,
  {issue}`10512`)
* Prevent data loss during `DROP SCHEMA` when the schema location contains files
  that are not part of existing tables. ({issue}`10485`)
* Fix inserting into transactional table when `task_writer_count` > 1.
  ({issue}`9149`)
* Fix possible data corruption when writing data to S3 with streaming enabled.
  ({issue}`10710 `)

## Iceberg connector

* Add `$properties` system table which can be queried to inspect Iceberg table
  properties. ({issue}`10480`)
* Add support for `ALTER TABLE .. EXECUTE OPTIMIZE` statement. ({issue}`10497`)
* Respect Iceberg column metrics mode when writing. ({issue}`9938`)
* Add support for long lived AWS Security Token Service (STS) credentials for
  authentication with Glue catalog. ({issue}`10735`)
* Improve performance of queries using range predicates when reading ORC files
  with Bloom filters. ({issue}`4108`)
* Improve select query planning performance after write operations from Trino.
  ({issue}`9340`)
* Ensure table statistics are accumulated in a deterministic way from Iceberg
  column metrics. ({issue}`9716`)
* Prevent data loss during `DROP SCHEMA` when the schema location contains files
  that are not part of existing tables. ({issue}`10485`)
* Support writing Parquet files greater than 2GB. ({issue}`10722`)
* Fix materialized view refresh when view a query references the same table
  multiple times. ({issue}`10570`)
 * Fix possible data corruption when writing data to S3 with streaming enabled.
  ({issue}`10710 `)

## MySQL connector

* Fix spurious errors when metadata cache is enabled. ({issue}`10544`,
  {issue}`10512`)

## Oracle connector

* Map Oracle `date` to Trino `timestamp(0)` type. ({issue}`10626`)
* Fix performance regression of predicate pushdown on indexed `date` columns.
  ({issue}`10626`)
* Fix spurious errors when metadata cache is enabled. ({issue}`10544`,
  {issue}`10512`)

## Phoenix connector

* Fix spurious errors when metadata cache is enabled. ({issue}`10544`,
  {issue}`10512`)

## Pinot connector

* Add support for basic authentication. ({issue}`9531`)

## PostgreSQL connector

* Add support for [renaming schemas](/sql/alter-schema). ({issue}`8939`)
* Fix spurious errors when metadata cache is enabled. ({issue}`10544`,
  {issue}`10512`)

## Redshift connector

* Add support for [renaming schemas](/sql/alter-schema). ({issue}`8939`)
* Fix spurious errors when metadata cache is enabled. ({issue}`10544`,
  {issue}`10512`)

## SingleStore (MemSQL) connector

* Fix spurious errors when metadata cache is enabled. ({issue}`10544`,
  {issue}`10512`)

## SQL Server connector

* Fix spurious errors when metadata cache is enabled. ({issue}`10544`,
  {issue}`10512`)

## SPI

* Remove support for the `ConnectorMetadata.getTableLayout()` API.
  ({issue}`781`)
