# Release 477 (24 Sep 2025)

## General

* Add {doc}`/connector/lakehouse`. ({issue}`25347`)
* Add support for [`ALTER MATERIALIZED VIEW ... SET AUTHORIZATION`](/sql/alter-materialized-view). ({issue}`25910`)
* Add support for default column values when creating tables or adding new
  columns. ({issue}`25679`)
* Add support for [`ALTER VIEW ... REFRESH`](/sql/alter-view). ({issue}`25906`)
* Add support for managing and querying table branches. ({issue}`25751`, {issue}`26300`, {issue}`26136`)
* Add the {func}`cosine_distance` function for sparse vectors. ({issue}`24027`)
* {{breaking}} Improve precision and scale inference for arithmetic operations with
  decimal values. The previous behavior can be restored by setting the
  `deprecated.legacy-arithmetic-decimal-operators` config property to `true`. ({issue}`26422`)
* {{breaking}} Remove the HTTP server event listener plugin from the server binary distribution
   and the Docker container. ({issue}`25967`)
* {{breaking}} Enforce requirement for catalogs to be deployed in all nodes. ({issue}`26063`)
* Add `query.max-write-physical-size` configuration property and
  `query_max_write_physical_size` session property to allow configuring limits
  on the amount of data written by a query. ({issue}`25955`)
* Add `system.metadata.tables_authorization`,
  `system.metadata.schemas_authorization`,
  `system.metadata.functions_authorization` tables that expose the information
  about the authorization for given entities. ({issue}`25907`)
* Add physical data scan tracking to resource groups. ({issue}`25003`)
* Add `internal_network_input_bytes` column to `system.runtime.tasks` table. ({issue}`26524`)
* Add support for `Geometry` type in {func}`to_geojson_geometry`. ({issue}`26451`)
* Remove `raw_input_bytes` and `raw_input_rows` columns from `system.runtime.tasks` table. ({issue}`26524`)
* Do not include catalogs that failed to load in `system.metadata.catalogs`. ({issue}`26493`)
* Simplify node discovery configuration for Kubernetes-like environments that 
  provide DNS names for all workers when the `discovery.type` config property is set 
  to `dns`. ({issue}`26119`)
* Improve memory usage for certain queries involving {func}`row_number`, {func}`rank`,
  {func}`dense_rank`, and `ORDER BY ... LIMIT`. ({issue}`25946`)
* Improve memory usage for queries involving `GROUP BY`. ({issue}`25879`)
* Reduce memory required for queries containing aggregations with a `DISTINCT`
  or `ORDER BY` clause. ({issue}`26276`)
* Improve performance of simple queries in clusters with small number of 
  nodes. ({issue}`26525`)
* Improve cluster stability when querying slow data sources and queries terminate early 
  or are cancelled. ({issue}`26602`)
* Improve join and aggregation reliability when spilling. ({issue}`25892`, {issue}`25976`)
* Ensure spill files are cleaned up for queries involving `GROUP BY`. ({issue}`26141`)
* Reduce out-of-memory errors for queries involving joins. ({issue}`26142`)
* Fix incorrect results for queries involving `GROUP BY` when spilling is enabled. ({issue}`25892`)
* Fix failure when aggregation exists in other expressions in `GROUP BY AUTO`. ({issue}`25987`)
* Fix incorrect results for queries involving joins using {func}`ST_Contains`, {func}`ST_Intersects`, 
  and {func}`ST_Distance` functions. ({issue}`26021`)
* Fix out-of-memory failures when the client spooling protocol is enabled. ({issue}`25999`)
* Fix worker crashes due to JVM out-of-memory errors when running `GROUP BY` queries
  with aggregations containing an `ORDER BY` clause. ({issue}`26276`)
* Fix incorrectly ignored grant when access is granted through groups via
  `SET SESSION AUTHORIZATION`. ({issue}`26344`)
* Fix incorrect results for `geometry_to_bing_tiles`, where the tiles wouldn't cover the full 
  geometry area. ({issue}`26459`)
* Fix over-reporting the amount of memory used when aggregating over `ROW` 
  values when nested inside of an `ARRAY` type ({issue}`26405`)
* Improve accounting of physical input metrics in output of `EXPLAIN ANALYZE`. ({issue}`26637`)

## Web UI

* Add query details page to the [](/admin/preview-web-interface). ({issue}`25554`)
* Add query JSON page to the [](/admin/preview-web-interface). ({issue}`26319`)
* Add query live plan flow page to the [](/admin/preview-web-interface). ({issue}`26392`)
* Add query references page to the [](/admin/preview-web-interface). ({issue}`26327`)
* Add query stages view to the [](/admin/preview-web-interface). ({issue}`26440`)
* Add query live plan flow page to the [](/admin/preview-web-interface). ({issue}`26610`)
* Enhance UI responsiveness for Trino clusters without external network
  access. ({issue}`26031`)

## CLI

* Add support for keyboard navigation with {kbd}`Alt+↑` or {kbd}`Alt+↓` in query
  history. ({issue}`26138`)

## Delta Lake connector

* Add support for using GCS without credentials. ({issue}`25810`)
* Rename `s3.socket-read-timeout` config property to `s3.socket-timeout`. ({issue}`26263`)
* Add metrics for data read from filesystem cache in 
  `EXPLAIN ANALYZE VERBOSE` output. ({issue}`26342`)
* Improve resource utilization when using Alluxio. ({issue}`26121`)
* Improve resource utilization by releasing native filesystem resources as soon as possible. ({issue}`26085`)
* Improve throughput for write-heavy queries on Azure when the `azure.multipart-write-enabled`
  config option is set to `true`. ({issue}`26225`)
* Reduce query failures due to S3 throttling. ({issue}`26407`)
* Avoid worker crashes due to out-of-memory errors when decoding unusually
  large Parquet footers. ({issue}`25973`)
* Fix incorrect results when reading from Parquet files produced by old versions
  of PyArrow. ({issue}`26058`)
* Fix writing malformed checkpoint files when [deletion vector](https://docs.delta.io/latest/delta-deletion-vectors.html) 
  is enabled. ({issue}`26145`)
* Fix failure when reading tables that contain null values in `variant` columns. ({issue}`26016`, {issue}`26184`)
* Fix incorrect results when reading `decimal` numbers from Parquet files and the declared precision 
  differs from the precision described in the Parquet metadata. ({issue}`26203`)
* Fix incorrect results when a table uses [deletion vector](https://docs.delta.io/latest/delta-deletion-vectors.html) 
  and its partition path contains special characters. ({issue}`26299`)

## Exasol connector

* Add support for Exasol `hashtype` type. ({issue}`26512`)
* Improve performance for queries involving `LIMIT`. ({issue}`26592`)

## Hive connector

* Add support for using GCS without credentials. ({issue}`25810`)
* Add support for reading tables using the [Esri JSON](https://doc.arcgis.com/en/velocity/ingest/esrijson.htm)
  format. ({issue}`25241`)
* Add support for `extended_boolean_literal` in text-file formats. ({issue}`21156`)
* Add metrics for data read from filesystem cache in `EXPLAIN ANALYZE VERBOSE` output. ({issue}`26342`)
* Add support for Twitter Elephantbird protobuf deserialization. ({issue}`26305`)
* Rename `s3.socket-read-timeout` config property to `s3.socket-timeout`. ({issue}`26263`)
* Improve throughput for write-heavy queries on Azure when the `azure.multipart-write-enabled`
  config option is set to `true`. ({issue}`26225`)
* Reduce query failures due to S3 throttling. ({issue}`26407`)
* Avoid worker crashes due to out-of-memory errors when decoding unusually
  large Parquet footers. ({issue}`25973`)
* Improve resource utilization when using Alluxio. ({issue}`26121`)
* Fix incorrect results when reading from Parquet files produced by old versions
  of PyArrow. ({issue}`26058`)
* Fix reading `partition_projection_format` column property for date partition
  projection. ({issue}`25642`)
* Fix incorrect results when reading `decimal` numbers from Parquet files and the declared precision
  differs from the precision described in the Parquet metadata. ({issue}`26203`)
* Fix physical input read time metric for tables containing text files. ({issue}`26612`)
* Add support for reading Hive OpenCSV tables with quoting and escaping disabled. ({issue}`26619`)

## HTTP Event Listener

* Add support for configuring the HTTP method to use via the `http-event-listener.connect-http-method` 
  config property. ({issue}`26181`)

## Hudi connector

* Add support for configuring batch size for reads on Parquet files using the
  `parquet.max-read-block-row-count` configuration property or the
    `parquet_max_read_block_row_count` session property. ({issue}`25981`)
* Add support for using GCS without credentials. ({issue}`25810`)
* Rename `s3.socket-read-timeout` config property to `s3.socket-timeout`. ({issue}`26263`)
* Improve resource utilization when using Alluxio. ({issue}`26121`)
* Improve throughput for write-heavy queries on Azure when the `azure.multipart-write-enabled`
  config option is set to `true`. ({issue}`26225`)
* Reduce query failures due to S3 throttling. ({issue}`26407`)
* Avoid worker crashes due to out-of-memory errors when decoding unusually
  large Parquet footers. ({issue}`25973`)
* Fix incorrect results when reading from Parquet files produced by old versions
  of PyArrow. ({issue}`26058`)
* Fix incorrect results when reading `decimal` numbers from Parquet files and the declared precision
  differs from the precision described in the Parquet metadata. ({issue}`26203`)

## Iceberg connector

* Add support for `SIGV4` as an independent authentication scheme. It can be 
  enabled by setting the `iceberg.rest-catalog.security` config property to `SIGV4`. 
  The `iceberg.rest-catalog.sigv4-enabled` config property is no longer 
  supported. ({issue}`26218`)
* Add support for using GCS without credentials. ({issue}`25810`)
* Allow configuring the compression codec to use for reading a table via the `compression_codec` table 
  property. The `compression_codec` session is no longer supported. ({issue}`25755`)
* Add metrics for data read from filesystem cache in `EXPLAIN ANALYZE VERBOSE` 
  output. ({issue}`26342`)
* Rename `s3.socket-read-timeout` config property to `s3.socket-timeout`. ({issue}`26263`)
* Improve performance of `expire_snapshots` procedure. ({issue}`26230`)
* Improve performance of `remove_orphan_files` procedure. ({issue}`26326`, {issue}`26438`)
* Improve performance of queries on `$files` metadata table. ({issue}`25677`)
* Improve performance of writes to Iceberg tables when task retries are 
  enabled. ({issue}`26620`)
* Reduce memory usage of `remove_orphan_files` procedure. ({issue}`25847`)
* Improve throughput for write-heavy queries on Azure when the `azure.multipart-write-enabled`
  config option is set to `true`. ({issue}`26225`)
* Reduce query failures due to S3 throttling. ({issue}`26407`, {issue}`26432`)
* Avoid worker crashes due to out-of-memory errors when decoding unusually
  large Parquet footers. ({issue}`25973`)
* Improve resource utilization when using Alluxio. ({issue}`26121`)
* Reduce amount of metadata generated in writes to Iceberg tables. ({issue}`15439`)
* Fix performance regression and potential query failures for `REFRESH MATERIALIZED VIEW`. ({issue}`26051`)
* Fix incorrect results when reading from Parquet files produced by old versions
  of PyArrow. ({issue}`26058`)
* Fix failure for `optimize_manifests` procedure when top-level partition columns contain null values. ({issue}`26185`)
* Fix incorrect results when reading `decimal` numbers from Parquet files and the declared precision
  differs from the precision described in the Parquet metadata. ({issue}`26203`)
* Fix coordinator out-of-memory failures when running `OPTIMIZE_MANIFESTS` on partitioned 
  tables. ({issue}`26323`)

## Kafka Event Listener

* Add support for configuring the max request size with the
  `kafka-event-listener.max-request-size` config property. ({issue}`26129`)
* Add support for configuring the batch size with the
  `kafka-event-listener.batch-size` config property. ({issue}`26129`)

## Memory connector

* Add support for default column values. ({issue}`25679`)
* Add support for `ALTER VIEW ... REFRESH`. ({issue}`25906`)

## MongoDB connector

* Fix failure when reading array type with different element types. ({issue}`26585`)

## MySQL Event Listener

* Ignore startup failure if `mysql-event-listener.terminate-on-initialization-failure` 
  is disabled. ({issue}`26252`)

## OpenLineage Event Listener

* Add user identifying fields to the OpenLineage `trino_query_context` facet. ({issue}`26074`)
* Add `query_id` field to `trino_metadata` facet. ({issue}`26074`)
* Allow customizing the job facet name with the `openlineage-event-listener.job.name-format` 
  config property. ({issue}`25535`)

## PostgreSQL connector

* Add support for `geometry` types when `PostGIS` is installed in schemas other 
  than `public`. ({issue}`25972`)

## SPI

* Remove `ConnectorSession` from `Type.getObjectValue`. ({issue}`25945`)
* Remove unused `NodeManager.getEnvironment` method. ({issue}`26096`)
* Remove `@Experimental` annotation. ({issue}`26200`)
* Remove deprecated `ConnectorPageSource.getNextPage` method. ({issue}`26222`)
* Remove support for `EventListener#splitCompleted`. ({issue}`26436`)
  and `ConnectorMetadata.refreshMaterializedView`. ({issue}`26455`)
* Remove unused `CatalogHandle` class. ({issue}`26520`)
* Change the signature of `ConnectorMetadata.beginRefreshMaterializedView` and
  `ConnectorMetadata.finishRefreshMaterializedView`. Table handles for other
  catalogs are no longer passed to these methods. ({issue}`26454`)
* Deprecate `NodeManager.getCurrentNode` in favor of `ConnectorContext.getCurrentNode`. ({issue}`26096`)
* Deprecate `ConnectorMetadata.delegateMaterializedViewRefreshToConnector`. ({issue}`26455`)
* Remove `totalBytes` and `totalRows` from `io.trino.spi.eventlistener.QueryStatistics`. ({issue}`26524`)
