# Release 361 (27 Aug 2021)

## General

* Add support for subqueries in `MATCH_RECOGNIZE` and `WINDOW` clause. ({issue}`8736`)
* Add `system.metadata.materialized_views` table that contains detailed information about materialized views. ({issue}`8796`)
* Support table redirection for `INSERT`, `UPDATE` and `DELETE` operations. ({issue}`8683`)
* Improve performance of {func}`sum` and {func}`avg` aggregations on `decimal` values. ({issue}`8878`)
* Improve performance for queries using `IN` predicate with moderate to large number of constants. ({issue}`8833`)
* Fix failures of specific queries accessing `row` columns with
  with field names that would require quoting when used as an identifier.  ({issue}`8845`)
* Fix incorrect results for queries with a comparison between a `varchar` column and a `char` constant. ({issue}`8984`)
* Fix invalid result when two decimals are added together. This happened in certain 
  queries where decimals had different precision. ({issue}`8973`)
* Prevent dropping or renaming objects with an incompatible SQL command. For example, `DROP TABLE` no longer allows dropping a view. ({issue}`8869`)

## Security

* Add support for OAuth2/OIDC opaque access tokens. The property
  `http-server.authentication.oauth2.audience` has been removed in favor of
  using `http-server.authentication.oauth2.client-id`, as expected by OIDC.
  The new property `http-server.authentication.oauth2.additional-audiences`
  supports audiences which are not the `client-id`. Additionally, the new
  property `http-server.authentication.oauth2.issuer` is now required;
  tokens which are not issued by this URL will be rejected. ({issue}`8641`)

## JDBC driver

* Implement the `PreparedStatement.getParameterMetaData()` method. ({issue}`2978`)
* Fix listing columns where table or schema name pattern contains an upper case value.
  Note that this fix is on the server, not in the JDBC driver. ({issue}`8978`)

## BigQuery connector

* Fix incorrect result when using BigQuery `time` type. ({issue}`8999`)

## Cassandra connector

* Add support for predicate pushdown of `smallint`, `tinyint` and `date` types on partition columns. ({issue}`3763`)
* Fix incorrect results for queries containing inequality predicates on a clustering key in the `WHERE` clause. ({issue}`401`)  

## ClickHouse connector

* Allow limiting the size of the metadata cache via the `metadata.cache-maximum-size` configuration property. ({issue}`8652`)
* Fix incorrect results for aggregation functions applied to columns of type `varchar` and `char`. ({issue}`7320`)

## Druid connector

* Allow limiting the size of the metadata cache via the `metadata.cache-maximum-size` configuration property. ({issue}`8652`)

## Elasticsearch connector

* Add support for reading fields as `json` values. ({issue}`7308`)

## Hive connector

* Expose `<view>$properties` system table for Trino and Hive views. ({issue}`8805`)
* Add support for translating Hive views which contain common table expressions. ({issue}`5977`)
* Add support for translating Hive views which contain outer parentheses. ({issue}`8789`)
* Add support for translating Hive views which use the `from_utc_timestamp` function. ({issue}`8502`)
* Add support for translating Hive views which use the `date` function. ({issue}`8789`)
* Add support for translating Hive views which use the `pmod` function. ({issue}`8935`)
* Prevent creating of tables that have column names containing commas, or leading or trailing spaces. ({issue}`8954`)
* Improve performance of updating Glue table statistics for partitioned tables. ({issue}`8839`)
* Change default Glue statistics read/write parallelism from 1 to 5. ({issue}`8839`)
* Improve performance of querying Parquet data for files containing column indexes. ({issue}`7349`)
* Fix query failure when inserting data into a Hive ACID table which is not explicitly bucketed. ({issue}`8899`)

## Iceberg connector

* Fix reading or writing Iceberg tables that previously contained a
  partition field that was later dropped. ({issue}`8730`)
* Allow reading from Iceberg tables which specify the Iceberg
  `write.object-storage.path` table property. ({issue}`8573`)
* Allow using randomized location when creating a table, so that future table
  renames or drops do not interfere with new tables created with the same name.
  This can be enabled using the `iceberg.unique-table-location` configuration
  property. ({issue}`6063`)
* Return proper query results for queries accessing multiple snapshots of single Iceberg table. ({issue}`8868`)

## MemSQL connector

* Allow limiting the size of the metadata cache via the `metadata.cache-maximum-size` configuration property. ({issue}`8652`)

## MongoDB connector

* Add {func}`timestamp_objectid` function. ({issue}`8824`)
* Enable `mongodb.socket-keep-alive` config property by default. ({issue}`8832`)

## MySQL connector

* Allow limiting the size of the metadata cache via the `metadata.cache-maximum-size` configuration property. ({issue}`8652`)
* Fix incorrect results for aggregation functions applied to columns of type `varchar` and `char`. ({issue}`7320`)

## Oracle connector

* Allow limiting the size of the metadata cache via the `metadata.cache-maximum-size` configuration property. ({issue}`8652`)

## Phoenix connector

* Allow limiting the size of the metadata cache via the `metadata.cache-maximum-size` configuration property. ({issue}`8652`)

## Pinot connector

* Implement aggregation pushdown for `count`, `avg`, `min`, `max`, `sum`, `count(DISTINCT)` and `approx_distinct`.
  It is enabled by default and can be disabled using the configuration property `pinot.aggregation-pushdown.enabled`
  or the catalog session property `aggregation_pushdown_enabled`. ({issue}`4140`)
* Allow `https` URLs in `pinot.controller-urls`. ({issue}`8617`)
* Fix failures when querying `information_schema.columns` with a filter on the table name. ({issue}`8307`)

## PostgreSQL connector

* Allow limiting the size of the metadata cache via the `metadata.cache-maximum-size` configuration property. ({issue}`8652`)
* Fix incorrect results for aggregation functions applied to columns of type `varchar` and `char`. ({issue}`7320`)

## Redshift connector

* Allow limiting the size of the metadata cache via the `metadata.cache-maximum-size` configuration property. ({issue}`8652`)

## SQL Server connector

* Allow limiting the size of the metadata cache via the `metadata.cache-maximum-size` configuration property. ({issue}`8652`)
* Fix incorrect results for aggregation functions applied to columns of type `varchar` and `char`. ({issue}`7320`)
