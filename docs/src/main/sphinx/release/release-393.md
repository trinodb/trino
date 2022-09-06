# Release 393 (17 Aug 2022)

## General

* Add support for {doc}`/sql/merge`. ({issue}`7933`)
* Add query state and error code to the timeline log message. ({issue}`13698`)
* Improve performance of highly selective `LIMIT` queries by making them finish
  as soon as the required number of rows is produced. ({issue}`13616`)
* Disallow inserting `NULL` into non-nullable columns.. ({issue}`13462`)
* Fix queries over bucketed tables never completing. ({issue}`13655`)
* Fix potential query failure for `GROUP BY` queries involving aggregations with
  `ORDER BY` and `FILTER` clauses. ({issue}`13627`)

## Security

* Fix potential incorrect rejection of OAuth 2.0 refresh tokens. ({issue}`13168`)
* Fix incorrectly showing outdated OAuth 2.0 refresh tokens in the web UI. ({issue}`13168`)

## Docker image

* Add experimental image for `ppc64le`. ({issue}`13522`)

## ClickHouse connector

* Add support for the `unsupported-type-handling` catalog configuration
  property. ({issue}`13542`)
* Improve performance for queries with selective joins. ({issue}`13334`)

## Delta Lake connector

* Add support for {doc}`/sql/merge`. ({issue}`7933`)
* Add support for the `NOT NULL` column constraint. ({issue}`13436`)
* Fix writing incorrect results when the order of partition columns is different
  from the order in the table definition. ({issue}`13505`)
* Fix failure when reading a table which has partition columns renamed by 
  another engine. ({issue}`13521`)

## Druid connector

* Improve performance for queries with selective joins. ({issue}`13334`)

## Hive connector

* Add support for {doc}`/sql/merge`. ({issue}`7933`)
* Add support for bucket filtering on bucketed columns of `float`, `double`,
  `date`, `list`, `map` and `bounded varchar` data types. ({issue}`13553`)
* Add `exchange.azure.max-error-retries` configuration property for the number
  of retries performed when accessing Azure blob storage. ({issue}`13663`)
* Improve performance of queries with S3 Select pushdown by not utilizing
  pushdown when it is unnecessary. ({issue}`13477`)
* Reduce Thrift metastore communication overhead when impersonation is enabled. ({issue}`13606`)
* Improve performance when retrieving table statistics from the metastore. ({issue}`13488`)
* Fix error when writing to a table with only `date` columns while using the
  Hive metastore. ({issue}`13502`)
* Fix error when reading a Hive view which has a column names with a reserved
  keyword. ({issue}`13450`)

## Iceberg connector

* Add support for {doc}`/sql/merge`. ({issue}`7933`)
* Improve performance when filtering on `$file_modified_time` column. ({issue}`13504`)
* Improve performance of read queries on Iceberg v2 tables with
  deletion-tracking files. ({issue}`13395`)
* Allow partitioning over columns which use whitespace in their names. ({issue}`12226`)
* Disallow specifying a `NOT NULL` constraint when adding a new column.
  Previously, the option was ignored. ({issue}`13673`)
* Fix error when querying tables which are empty and contain no table history. ({issue}`13576`)
* Prevent truncation of the table history in the `$snapshots` system table by
  certain `DELETE` queries. ({issue}`12843`)
* Prevent errors when optimizing an Iceberg table which is empty and contains
  no table history. ({issue}`13582`)
* Fix incorrect query results when reading from a materialized view that was
  created on a table which was empty and contained no history. ({issue}`13574`)

## Kafka connector

* Fix query failure when applying a negative timestamp predicate on the
  `_timestamp` column. ({issue}`13167`)

## Kudu connector

* Add support for {doc}`/sql/merge`. ({issue}`7933`)

## MariaDB connector

* Improve performance for queries with selective joins. ({issue}`13334`)

## MongoDB connector

* Prevent renaming a table with a name longer than the max length supported by
  MongoDB. Previously, the name was truncated to the max length. ({issue}`13073`)

## MySQL connector

* Improve performance for queries with selective joins. ({issue}`13334`)

## Oracle connector

* Improve performance for queries with selective joins. ({issue}`13334`)

## Phoenix connector

* Improve performance for queries with selective joins. ({issue}`13334`)

## Pinot connector

* Add support for the Pinot `bytes` type. ({issue}`13427`)
* Add support for the `json` type. ({issue}`13428`)

## PostgreSQL connector

* Improve performance for queries with selective joins. ({issue}`13334`)
* Prevent using schema names or renaming a table with a name which is longer
  than the max length supported by PostgreSQL. Previously, long names were
  truncated to the max length. ({issue}`13307`, {issue}`13073`)

## Raptor connector

* Add support for {doc}`/sql/merge`. ({issue}`7933`)

## Redshift connector

* Improve performance for queries with selective joins. ({issue}`13334`)

## SingleStore (MemSQL) connector

* Improve performance for queries with selective joins. ({issue}`13334`)

## SQL Server connector

* Improve performance for queries with selective joins. ({issue}`13334`)
* Prevent renaming a table with a name longer than the max length supported by
  SQL Server. Previously, the name was truncated to the max length. ({issue}`13073`)

## SPI

* Add `@Experimental` annotation to designate SPIs that are still under active
  development. ({issue}`13302`)
* Deprecate `io.trino.spi.block.MethodHandleUtil`. ({issue}`13245`)
