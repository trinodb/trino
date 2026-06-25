# Release 481 (11 May 2026)

## General

* Add support for casting `boolean` to `number`. ({issue}`28879`)
* Add support for the `number` type in Python UDFs. ({issue}`28921`)
* Add support for casting between `number` and `json`. ({issue}`28394`)
* Improve performance of {ref}`json_value <json-value>` and {ref}`json_table <json-table>` by evaluating `ON EMPTY` and `ON ERROR` clauses lazily. ({issue}`28969`)
* Add support for `DESCRIBE OUTPUT` with inline queries, allowing direct description
  of query results without requiring a `PREPARE` statement. For example,
  `DESCRIBE OUTPUT (SELECT * FROM nation)`. ({issue}`28002`)
* Add support for the `NEAREST` clause for approximate matches in joins. ({issue}`21759`)
* Add support for binding parameters in `WITH SESSION`, `SET SESSION`, and `CALL` statements. ({issue}`29053`)
* Add support for persisting external authentication tokens to disk (in `~/.trino/`), allowing reuse across separate JDBC client processes. Set `externalAuthenticationTokenCache=SYSTEM` to enable. ({issue}`28783`)
* Include connector split source metrics in 
  `io.trino.spi.eventlistener.QueryInputMetadata#connectorMetrics`. ({issue}`28870`)
* Replace the Esri geometry library with JTS to improve interoperability with the broader spatial ecosystem. ({issue}`27881`)
* {{breaking}} Reject WKT input that does not conform to OGC standards. Some inputs that were silently accepted before will now fail. ({issue}`27881`)
* {{breaking}} Change {func}`ST_Union` to return an empty geometry collection instead of `NULL` for empty inputs. ({issue}`27881`)
* {{breaking}} Stop inserting vertices at intersection points for point-on-line unions in {func}`ST_Union`. ({issue}`27881`)
* Improve compatibility of file system exchange with Azure containers that have hierarchical namespaces enabled. ({issue}`29042`)
* Improve performance of queries with simple `AND` and `OR` predicates that have highly selective terms. ({issue}`24336`)
* Reduce excessive memory usage caused by suboptimal join ordering for queries on columns with unknown statistics. ({issue}`29157`)
* Fix query failures and transaction errors caused by race conditions when dynamic catalogs are dropped concurrently with `system.jdbc` or `system.metadata` queries. ({issue}`28017`)
* Fix incorrect results when using {func}`json_parse` or `JSON` type literals on documents containing numbers with more than 16 significant digits in the decimal portion. ({issue}`28867`)
* Fix failure when executing table procedures on tables with uppercase column names. ({issue}`28970`)
* Fix failure when executing `ALTER TABLE EXECUTE OPTIMIZE` with `OR` predicates on partitioned `timestamp with time zone` columns. ({issue}`27136`)
* Fix incorrect freshness check for materialized views whose definition contains non-deterministic functions. ({issue}`28682`)
* Fix failure when executing `DESCRIBE OUTPUT` with `[VERSION | TIMESTAMP] AS OF` clauses. ({issue}`29077`)

## JDBC driver

* Add support for the `variant` type. ({issue}`29046`)
* Add support for transparent OAuth2 token refresh when using the JDBC `accessToken`
  connection property against a server configured with `http-server.authentication.oauth2.refresh-tokens=true`.
  Previously, such connections failed with `401 Unauthorized` once the embedded
  access token expired. ({issue}`29264`)
* Fix failure when calling `setBigDecimal()` with a `BigDecimal` value whose
  `toString()` representation uses scientific notation, such as `0E-10`. ({issue}`23523`)

## CLI

* Add support for the `variant` type. ({issue}`29046`)

## ClickHouse connector

* Add support for reading all ClickHouse `DECIMAL` columns. ({issue}`28873`)

## Delta Lake connector

* {{breaking}} Remove legacy object storage support for Azure Storage,
  Google Cloud Storage, IBM Cloud Object Storage, S3, and S3-compatible
  systems. Use native file system support for object storage.
  `fs.hadoop.enabled` applies only to HDFS. See
  [legacy file system support](file-system-legacy) for migration details. ({issue}`24878`)
* Add support for configuring the Azure connection-pool idle time and HTTP request timeout via the `azure.connection-pool-max-idle-time` and `azure.http-request-timeout` configuration properties. ({issue}`29284`)
* Reduce memory usage when writing files to S3. ({issue}`28488`)
* Reduce query planning time by reading metadata and protocol information from Delta checksum files.
  This optimization can be disabled by setting the `delta.load-metadata-from-checksum-file` configuration property or
  `load_metadata_from_checksum_file` session property to `false`. ({issue}`28381`)
* Fix `DELETE` deleting incorrect rows on Delta Lake tables backed by Parquet files with column indexes, particularly when the table was written by Apache Spark. ({issue}`28885`)

## Hive connector

* {{breaking}} Remove legacy object storage support for Azure Storage,
  Google Cloud Storage, IBM Cloud Object Storage, S3, and S3-compatible
  systems. Use native file system support for object storage.
  `fs.hadoop.enabled` applies only to HDFS. See
  [legacy file system support](file-system-legacy) for migration details. ({issue}`24878`)
* Add support for [Esri GeoJSON](https://doc.arcgis.com/en/arcgis-online/reference/geojson.htm). ({issue}`28859`)
* Add support for configuring the Azure connection-pool idle time and HTTP request timeout via the `azure.connection-pool-max-idle-time` and `azure.http-request-timeout` configuration properties. ({issue}`29284`)
* Improve performance of queries with date partition predicates on Hive tables in the Glue catalog. ({issue}`28817`)
* Reduce memory usage when writing files to S3. ({issue}`28488`)
* Fix failure when creating tables in the Hive metastore version 3.1. ({issue}`28798`)
* Fix failure when parsing bucket numbers on tables backed by files without a bucket name. ({issue}`28632`)

## Hudi connector

* Add support for configuring the Azure connection-pool idle time and HTTP request timeout via the `azure.connection-pool-max-idle-time` and `azure.http-request-timeout` configuration properties. ({issue}`29284`)

## Iceberg connector

* {{breaking}} Remove legacy object storage support for Azure Storage,
  Google Cloud Storage, IBM Cloud Object Storage, S3, and S3-compatible
  systems. Use native file system support for object storage.
  `fs.hadoop.enabled` applies only to HDFS. See
  [legacy file system support](file-system-legacy) for migration details. ({issue}`24878`)
* Add experimental support for the `variant` type for Iceberg v3 tables.
  Older CLI versions will render the value as JSON unless updated. ({issue}`24538`)
* Add support for reading and writing `timestamp(9)` and
  `timestamp(9) with time zone` in Iceberg v3 tables. ({issue}`27835`)
* Add support for Azure vended credentials in Iceberg REST catalog. ({issue}`23238`)
* Add support for specifying HTTP headers in REST catalog. ({issue}`24236`)
* Add support for execution metrics while running the `add_files` and `add_files_from_table`
  procedures. ({issue}`28996`)
* Add support for execution metrics while running the `optimize` procedure. ({issue}`28992`)
* Add more columns to `$files` system table. ({issue}`29044`)
* Add `added_snapshot_id` column to `$files` system table. ({issue}`28911`)
* Add support for configuring the Azure connection-pool idle time and HTTP request timeout via the `azure.connection-pool-max-idle-time` and `azure.http-request-timeout` configuration properties. ({issue}`29284`)
* Add support for refreshable vended credentials for the Iceberg REST catalog (S3, GCS, Azure). ({issue}`28998`)
* Reduce memory usage when writing files to S3. ({issue}`28488`)
* Fix failure when reading the `$entries` or `$all_entries` metadata tables. ({issue}`29211`)

## Lakehouse connector

* {{breaking}} Remove legacy object storage support for Azure Storage,
  Google Cloud Storage, IBM Cloud Object Storage, S3, and S3-compatible
  systems. Use native file system support for object storage.
  `fs.hadoop.enabled` applies only to HDFS. See
  [legacy file system support](file-system-legacy) for migration details. ({issue}`24878`)
* Add support for configuring the Azure connection-pool idle time and HTTP request timeout via the `azure.connection-pool-max-idle-time` and `azure.http-request-timeout` configuration properties. ({issue}`29284`)
* Reduce memory usage when writing files to S3. ({issue}`28488`)

## MongoDB connector

* Fix incorrect results when reading JSON columns containing numbers with more than 16 significant digits in the decimal portion. ({issue}`28867`)

## MySQL connector

* Fix incorrect results when reading JSON columns containing numbers with more than 16 significant digits in the decimal portion. ({issue}`28867`)

## Pinot connector

* Fix incorrect results when reading JSON columns containing numbers with more than 16 significant digits in the decimal portion. ({issue}`28867`)

## PostgreSQL connector

* Improve performance of queries involving `COALESCE` by pushing expression
  computation to the underlying database. ({issue}`11535`)
* Fix incorrect results when reading JSON columns containing numbers with more than 16 significant digits in the decimal portion. ({issue}`28867`)

## SingleStore connector

* Fix incorrect results when reading JSON columns containing numbers with more than 16 significant digits in the decimal portion. ({issue}`28867`)

## SQL Server connector

* Add support for the `json` type. ({issue}`28082`)
* Fix permission denied failure when reading tables. ({issue}`29244`)

## SPI

* Add `variant` type defined by the Iceberg specification. ({issue}`24538`)
* Add a `tableBranch` parameter to the `ConnectorAccessControl.checkCanXxx` table-level methods. ({issue}`29179`)
* Add support for pushing down `COALESCE` expressions in connectors. ({issue}`28984`)
* {{breaking}} Remove the deprecated `getObject` and `appendTo` methods from the `Type` interface. ({issue}`29003`)
