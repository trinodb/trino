# Release 329 (23 Jan 2020)

## General

- Fix incorrect result for {func}`last_day_of_month` function for first day of month. ({issue}`2452`)
- Fix incorrect results when handling `DOUBLE` or `REAL` types with `NaN` values. ({issue}`2582`)
- Fix query failure when coordinator hostname contains underscores. ({issue}`2571`)
- Fix `SHOW CREATE TABLE` failure when row types contain a field named after a
  reserved SQL keyword. ({issue}`2130`)
- Handle common disk failures during spill. When one disk fails but multiple
  spill locations are configured, the healthy disks will be used for future queries.
  ({issue}`2444`)
- Improve performance and reduce load on external systems when
  querying `information_schema`. ({issue}`2488`)
- Improve performance of queries containing redundant scalar subqueries. ({issue}`2456`)
- Limit broadcasted table size to `100MB` by default when using the `AUTOMATIC`
  join type selection strategy. This avoids query failures or excessive memory usage when joining two or
  more very large tables. ({issue}`2527`)
- Enable {doc}`cost based </optimizer/cost-based-optimizations>` join reordering and join type selection
  optimizations by default. The previous behavior can be restored by
  setting `optimizer.join-reordering-strategy` configuration property to `ELIMINATE_CROSS_JOINS`
  and `join-distribution-type` to `PARTITIONED`. ({issue}`2528`)
- Hide non-standard columns `comment` and `extra_info` in the standard
  `information_schema.columns` table. These columns can still be selected,
  but will no longer appear when describing the table. ({issue}`2306`)

## Security

- Add `ldap.bind-dn` and `ldap.bind-password` LDAP properties to allow LDAP authentication
  access LDAP server using service account. ({issue}`1917`)

## Hive connector

- Fix incorrect data returned when using S3 Select on uncompressed files. In our testing, S3 Select
  was apparently returning incorrect results when reading uncompressed files, so S3 Select is disabled
  for uncompressed files. ({issue}`2399`)
- Fix incorrect data returned when using S3 Select on a table with `skip.header.line.count` or
  `skip.footer.line.count` property. S3 Select API does not support skipping footers or more than one
  line of a header.  In our testing, S3 Select was apparently sometimes returning incorrect results when
  reading a compressed file with header skipping, so S3 Select is disabled when any of these table
  properties is set to non-zero value. ({issue}`2399`)
- Fix query failure for writes when one of the inserted `REAL` or `DOUBLE` values
  is infinite or `NaN`. ({issue}`2471`)
- Fix performance degradation reading from S3 when the Kinesis connector is installed. ({issue}`2496`)
- Allow reading data from Parquet files when the column type is declared as `INTEGER`
  in the table or partition, but is a `DECIMAL` type in the file. ({issue}`2451`)
- Validate the scale of decimal types when reading Parquet files. This prevents
  incorrect results when the decimal scale in the file does not match the declared
  type for the table or partition. ({issue}`2451`)
- Delete storage location when dropping an empty schema. ({issue}`2463`)
- Improve performance when deleting multiple partitions by executing these actions concurrently. ({issue}`1812`)
- Improve performance for queries containing `IN` predicates over bucketing columns. ({issue}`2277`)
- Add procedure `system.drop_stats()` to remove the column statistics
  for a table or selected partitions. ({issue}`2538`)

## Elasticsearch connector

- Add support for {ref}`elasticsearch-array-types`. ({issue}`2441`)
- Reduce load on Elasticsearch cluster and improve query performance. ({issue}`2561`)

## PostgreSQL connector

- Fix mapping between PostgreSQL's `TIME` and Presto's `TIME` data types.
  Previously the mapping was incorrect, shifting it by the relative offset between the session
  time zone and the Presto server's JVM time zone. ({issue}`2549`)
