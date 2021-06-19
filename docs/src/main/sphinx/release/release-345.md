# Release 345 (23 Oct 2020)

## General

* Add {func}`concat_ws` function. ({issue}`4680`)
* Add support for {func}`extract` for `time with time zone` values with precision other than 3. ({issue}`5539`)
* Add dynamic filtering support for right joins. ({issue}`5555`)
* Add support for catalog session properties to the file and database backed session property managers. ({issue}`5330`)
* Improve query scalability by increasing the default broadcast join buffer size to 200MB.
  Broadcast join buffer size can be set using the `sink.max-broadcast-buffer-size` configuration property. ({issue}`5551`)
* Improve query performance by allowing larger dynamic filters. ({issue}`5529`)
* Improve performance for join queries where join keys are not of the same type. ({issue}`5461`)
* Improve performance of encrypted spilling. ({issue}`5557`)
* Improve performance of queries that use the `decimal` type. ({issue}`5181`)

## Security

* Add support for JSON Web Key (JWK) to the existing JSON Web Token (JWT) authenticator.  This is enabled by
  setting the `jwt.key-file` configuration property to a `http` or `https` url. ({issue}`5419`)
* Add column security, column mask and row filter to file-based access controls. ({issue}`5460`)
* Enforce access control for column references in `USING` clause. ({issue}`5620`)

## JDBC driver

* Add `source` parameter for directly setting the source name for a query. ({issue}`4739`)

## Hive connector

* Add support for `INSERT` and `DELETE` for ACID tables. ({issue}`5402`)
* Apply `hive.domain-compaction-threshold` to dynamic filters. ({issue}`5365`)
* Add support for reading Parquet timestamps encoded as microseconds. ({issue}`5483`)
* Improve translation of Hive views. ({issue}`4661`)
* Improve storage caching by better distributing files across workers. ({issue}`5621`)
* Fix disk space accounting for storage caching. ({issue}`5621`)
* Fix failure when reading Parquet `timestamp` columns encoded as `int64`. ({issue}`5443`)

## MongoDB connector

* Add support for adding columns. ({issue}`5512`)
* Fix incorrect result for `IS NULL` predicates on fields that do not exist in the document. ({issue}`5615`)

## MemSQL connector

* Fix representation for many MemSQL types. ({issue}`5495`)
* Prevent a query failure when table column name contains a semicolon by explicitly forbidding such names. ({issue}`5495`)
* Add support for case-insensitive table name matching. ({issue}`5495`)

## MySQL connector

* Improve performance of queries with aggregations and `LIMIT` clause (but without `ORDER BY`). ({issue}`5261`)

## PostgreSQL connector

* Improve performance of queries with aggregations and `LIMIT` clause (but without `ORDER BY`). ({issue}`5261`)

## Redshift connector

* Add support for setting column comments. ({issue}`5397`)

## SQL Server connector

* Improve performance of queries with aggregations and `LIMIT` clause (but without `ORDER BY`). ({issue}`5261`)

## Thrift connector

* Fix handling of timestamp values. ({issue}`5596`)
