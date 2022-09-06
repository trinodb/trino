# Release 392 (3 Aug 2022)

## General

* Add support for dynamic filtering when task-based fault-tolerant execution is enabled. ({issue}`9935`)
* Add support for correlated sub-queries in `DELETE` queries. ({issue}`9447`)
* Fix potential query failure in certain complex queries with multiple joins and
  aggregations. ({issue}`13315`)

## JDBC driver

* Add the `assumeLiteralUnderscoreInMetadataCallsForNonConformingClients`
  configuration property as a replacement for
  `assumeLiteralNamesInMetadataCallsForNonConformingClients`, which is
  deprecated and planned to be removed in a future release. ({issue}`12761`)

## ClickHouse connector

* Report the total time spent reading data from the data source. ({issue}`13132`)

## Delta Lake connector

* Add support for using a randomized location when creating a table, so that
  future table renames or drops do not interfere with new tables created with
  the same name. This can be disabled by setting the
  `delta.unique-table-location` configuration property to false. ({issue}`12980`)
* Add `delta.metadata.live-files.cache-ttl` configuration property for the
  caching duration of active data files. ({issue}`13316`)
* Retain metadata properties and column metadata after schema changes. ({issue}`13368`, {issue}`13418`)
* Prevent writing to a table with `NOT NULL` or
  [column invariants](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#column-invariants)
  columns. ({issue}`13353`)
* Fix incorrect min and max column statistics when writing `NULL` values. ({issue}`13389`)

## Druid connector

* Add support for `timestamp(p)` predicate pushdown. ({issue}`8404`)
* Report the total time spent reading data from the data source. ({issue}`13132`)
* Change mapping for the Druid `float` type to the Trino `real` type instead of
  the `double` type. ({issue}`13412`)

## Hive connector

* Add support for short timezone IDs when translating Hive views. For example,
  `JST` now works as an alias for `Asia/Tokyo`. ({issue}`13179`)
* Add support for Amazon S3 Select pushdown for JSON files. ({issue}`13354`)

## Iceberg connector

* Add support for hidden `$file_modified_time` columns. ({issue}`13082`)
* Add support for the Avro file format. ({issue}`12125`)
* Add support for filtering splits based on `$path` column predicates. ({issue}`12785`)
* Improve query performance for tables with updated or deleted rows. ({issue}`13092`)
* Improve performance of the `expire_snapshots` command for tables with many
  snapshots. ({issue}`13399`)
* Use unique table locations by default. This can be disabled by setting the
  `iceberg.unique-table-location` configuration property to false. ({issue}`12941`)
* Use the correct table schema when reading a past version of a table. ({issue}`12786`)
* Return the `$path` column without encoding when the path contains double
  slashes on S3. ({issue}`13012`)
* Fix failure when inserting into a Parquet table with columns that have
  quotation marks in their names. ({issue}`13074`)

## MariaDB connector

* Report the total time spent reading data from the data source. ({issue}`13132`)

## MySQL connector

* Report the total time spent reading data from the data source. ({issue}`13132`)
* Change mapping for the MySQL `enum` type to the Trino `varchar` type instead
  of the `char` type. ({issue}`13303`)
* Fix failure when reading table statistics while the
  `information_schema.column_statistics` table doesn't exist. ({issue}`13323`)

## Oracle connector

* Report the total time spent reading data from the data source. ({issue}`13132`)

## Phoenix connector

* Report the total time spent reading data from the data source. ({issue}`13132`)

## Pinot connector

* Redact the values of `pinot.grpc.tls.keystore-password` and
  `pinot.grpc.tls.truststore-password` in the server log. ({issue}`13422`)

## PostgreSQL connector

* Report the total time spent reading data from the data source. ({issue}`13132`)
* Improve performance of queries with an `IN` expression within a complex
  expression. ({issue}`13136`)

## Redshift connector

* Report the total time spent reading data from the data source. ({issue}`13132`)

## SingleStore (MemSQL) connector

* Report the total time spent reading data from the data source. ({issue}`13132`)

## SQL Server connector

* Report the total time spent reading data from the data source. ({issue}`13132`)
