# Release 401 (26 Oct 2022)

## General

* Add support for using path-style access for all requests to S3 when using
  fault-tolerant execution with exchange spooling. This can be enabled with the
  `exchange.s3.path-style-access` configuration property. ({issue}`14655`)
* Add support for table functions in file-based access control. ({issue}`13713`)
* Add output buffer utilization distribution to `EXPLAIN ANALYZE VERBOSE`. ({issue}`14596`)
* Add operator blocked time distribution to `EXPLAIN ANALYZE VERBOSE`. ({issue}`14640`)
* Improve performance and reliability of `INSERT` and `MERGE`. ({issue}`14553`)
* Fix query failure caused by a
  `com.google.common.base.VerifyException: cannot unset noMoreSplits` error. ({issue}`14668`)
* Fix underestimation of CPU usage and scheduled time statistics for joins in
  `EXPLAIN ANALYZE`. ({issue}`14572`)

## Cassandra connector

* Upgrade minimum required Cassandra version to 3.0. ({issue}`14562`)

## Delta Lake connector

* Add support for writing to tables with [Delta Lake writer protocol version 4](https://docs.delta.io/latest/versioning.html#features-by-protocol-version).
  This does not yet include support for [change data feeds](https://docs.delta.io/2.0.0/delta-change-data-feed.html)
  or generated columns. ({issue}`14573 `)
* Add support for writes on Google Cloud Storage. ({issue}`12264`)
* Avoid overwriting the reader and writer versions when executing a `COMMENT` or
  `ALTER TABLE ... ADD COLUMN` statement. ({issue}`14611`)
* Fix failure when listing tables from the Glue metastore and one of the tables
  has no properties. ({issue}`14577`)

## Hive connector

* Add support for [IBM Cloud Object Storage](/connector/hive-cos). ({issue}`14625`)
* Allow creating tables with an Avro schema literal using the new table property
  `avro_schema_literal`. ({issue}`14426`)
* Fix potential query failure or incorrect results when reading from a table
  with the `avro.schema.literal` Hive table property set. ({issue}`14426`)
* Fix failure when listing tables from the Glue metastore and one of the tables
  has no properties. ({issue}`14577`)

## Iceberg connector

* Improve performance of the `remove_orphan_files` table procedure. ({issue}`13691`)
* Fix query failure when analyzing a table that contains a column with a
  non-lowercase name. ({issue}`14583`)
* Fix failure when listing tables from the Glue metastore and one of the tables
  has no properties. ({issue}`14577`)

## Kafka connector

* Add support for configuring the prefix for internal column names with the
  `kafka.internal-column-prefix` catalog configuration property. The default
  value is `_` to maintain current behavior. ({issue}`14224`)

## MongoDB connector

* Add `query` table function for query pass-through to the connector. ({issue}`14535`)

## MySQL connector

* Add support for writes when [fault-tolerant
  execution](/admin/fault-tolerant-execution) is enabled. ({issue}`14445`)

## Pinot connector

* Fix failure when executing `SHOW CREATE TABLE`. ({issue}`14071`)

## PostgreSQL connector

* Add support for writes when [fault-tolerant
  execution](/admin/fault-tolerant-execution) is enabled. ({issue}`14445`)

## SQL Server connector

* Add support for writes when [fault-tolerant
  execution](/admin/fault-tolerant-execution) is enabled. ({issue}`14730`)

## SPI

* Add stage output buffer distribution to `EventListener`. ({issue}`14638`)
