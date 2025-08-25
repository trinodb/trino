# Release 477 (dd MMM 2025)

## General

* Improve reliability of spilling for aggregation by retaining revocable memory, preventing out‑of‑memory errors. ({issue}`25892`)
* Fix correctness issues when unspilling hash aggregation by using correct hash values, avoiding aggregation miscalculations. ({issue}`25892`)
* Reduce memory consumed by grouped Top N rank and row number operations ({issue}`25946`)
* Remove the HTTP server event listener plugin from the tar.gz archive and the Docker container. ({issue}`25967`)
* Fix failure when aggregation exists in other expressions in `GROUP BY AUTO`. ({issue}`25987`)
* Improve spilling reliability for join queries. ({issue}`25976`)
* Add support for `ALTER MATERIALIZED VIEW ... SET AUTHORIZATION`. ({issue}`25910`)
* Fix incorrect results with spatial joins. ({issue}`26021`)
* Reduce memory usage of aggregations by incrementally releasing memory as output rows are produced ({issue}`25879`) 
* Add the `coordinatorId` to the `/v1/info` endpoint. ({issue}`23910`)
* Use UUIDv7 for `runId` in OpenLineage event listener. ({issue}`25534`)
* Partition large pages to avoid OOM while serializing data in the spooling protocol. ({issue}`25999`)
* All catalogs are now required to be deployed to all nodes. ({issue}`26063`)
* Add Lakehouse connector. ({issue}`25347`)
* Add user identifying fields to OpenLineage `trino_query_context` facet in OpenLineage event listener. ({issue}`26074`)
* Add `query_id` field to `trino_metadata` facet in OpenLineage event listener. ({issue}`26074`)
* Add `query.max-write-physical-size` configuration property and `query_max_write_physical_size` session property to allow configuring limits on the amount of data written by a query. ({issue}`25955`)
* Add `system.metadata.tables_authorization`, `system.metadata.schemas_authorization`, `system.metadata.functions_authorization` tables that expose the information about the authorization for given entities. ({issue}`25907`)
* Add Announce node inventory to replace Airlift Discovery. ({issue}`26119`)
* Add DNS node inventory for use in K8s like environments that provide a DNS name for all workers. ({issue}`26119`)
* Add `discovery.type` to set node inventory system. The value can be `announce`, `dns` or `airlift-discovery`. ({issue}`26119`)
* Add support for creating, dropping, fast-forwarding and listing branches. ({issue}`25751`)
* Fix spill files leaking in aggregation queries. ({issue}`26141`)
* Reduce number of worker OOMs when running join queries. ({issue}`26142`)
* Add support for default column values. ({issue}`25679`)
* Add support for configuring max request size with the `kafka-event-listener.max-request-size` config property in Kafka Event Listener. ({issue}`26129`)
* Add support for configuring batch size with the `kafka-event-listener.batch-size` config property in Kafka Event Listener. ({issue}`26129`)
* Add support for branching with `INSERT`, `DELETE`, `UPDATE`, and `MERGE` statements. ({issue}`26136`)
* Add `debug_adaptive_planner` session property which allows gathering extra diagnostics information regarding adaptive planner operation. ({issue}`26274`)
* Reduce memory required for distinct and ordered grouped aggregations. ({issue}`26276`)
* Fix memory tracking for ordered grouped aggregations. ({issue}`26276`)
* Add support for view refresh operation. ({issue}`25906`)
* Add support for creating a branch from specified branches. ({issue}`26300`)
* Add support for configuring the HTTP method with `http-event-listener.connect-http-method` config property. ({issue}`26181`)
* Add physical data scan tracking to resource groups. ({issue}`25003`)
* Fix access control check when access is granted through groups in `SET SESSION AUTHORIZATION`. ({issue}`26344`)

## Security

## Web UI

* Improve UI rendering when Trino cluster is air-gapped ({issue}`26031`)
* Add query details page to the Preview Web UI. ({issue}`25554`)

## JDBC driver

* Send detailed client information in the source ({issue}`25889`)

## Docker image

## CLI

* Send detailed client information in the source. ({issue}`25889`)
* Allow configuring `--max-buffered-rows` and `--max-queued-rows`. ({issue}`26015`)
* Use actual stage id instead incremental number in progress output. ({issue}`26139`)
* Use {kbd}`Alt+↑` or {kbd}`Alt+↓` to move through the history. ({issue}`26138`)

## BigQuery connector

## Blackhole connector

## Cassandra connector

## ClickHouse connector

## Delta Lake connector

* Prevent workers from going into full GC or crashing when decoding unusually large parquet footers. ({issue}`25973`)
* Add support for using GCS without credentials. ({issue}`25810`)
* Fix failure when reading tables with `null` on `variant` type. ({issue}`26016`)
* Fix incorrect results when reading from parquet files produced by old versions of pyarrow. ({issue}`26058`)
* Release native filesystem resources/prevent leaks. ({issue}`26085`)
* Add ability to detect resource leakage in the runtime. ({issue}`26087`)
* Fix delta lake connector not closing resource streams properly. ({issue}`26092`)
* Prevent creating multiple Alluxio client which can cause excessive resource usage. ({issue}`26121`)
* Fix writing malformed checkpoint files when deletion vector is enabled. ({issue}`26145`)
* Fix failure when reading `null` values on `json` type columns. ({issue}`26184`)
* Fix skipping of row groups when the trino type is different from logical types in case of parquet files. ({issue}`26203`)
* Add `azure.multipart-write-enabled` that enables multipart uploads for large files. ({issue}`26225`)
* Rename `s3.socket-read-timeout` config property to `s3.socket-timeout`. ({issue}`26263`)
* Add support for `GRANT`, `DENY`, `REVOKE` with branches. ({issue}`25152`)
* Fix incorrect results when the table uses deletion vectors and special characters are used in the partition path. ({issue}`26299`)
* Reduce query failures from S3 throttling. ({issue}`26407`)

## Druid connector

## DuckDB connector

## Elasticsearch connector

## Exasol connector

## Faker connector

## Google Sheets connector

## Hive connector

* Prevent workers from going into full GC or crashing when decoding unusually large parquet footers. ({issue}`25973`)
* Add support for using GCS without credentials. ({issue}`25810`)
* Fix incorrect results when reading from parquet files produced by old versions of pyarrow. ({issue}`26058`)
* Add support for reading tables using the ESRI JSON format ({issue}`25241`)
* Add ability to detect resource leakage in the runtime. ({issue}`26087`)
* Prevent creating multiple Alluxio client which can cause excessive resource usage. ({issue}`26121`)
* Fix reading specifying format for date partition projection. ({issue}`25642`)
* Fix skipping of row groups when the trino type is different from logical types in case of parquet files. ({issue}`26203`)
* Add `azure.multipart-write-enabled` that enables multipart uploads for large files. ({issue}`26225`)
* Rename `s3.socket-read-timeout` config property to `s3.socket-timeout`. ({issue}`26263`)
* Reduce query failures from S3 throttling. ({issue}`26407`)
* Add support for `extended_boolean_literal` in text-file formats. ({issue}`21156`)

## Hudi connector

* Prevent workers from going into full GC or crashing when decoding unusually large parquet footers. ({issue}`25973`)
* Add support for `parquet_max_read_block_row_count` session property. ({issue}`25981`)
* Add support for using GCS without credentials. ({issue}`25810`)
* Fix incorrect results when reading from parquet files produced by old versions of pyarrow. ({issue}`26058`)
* Add ability to detect resource leakage in the runtime. ({issue}`26087`)
* Prevent creating multiple Alluxio client which can cause excessive resource usage. ({issue}`26121`)
* Fix skipping of row groups when the trino type is different from logical types in case of parquet files. ({issue}`26203`)
* Add `azure.multipart-write-enabled` that enables multipart uploads for large files. ({issue}`26225`)
* Rename `s3.socket-read-timeout` config property to `s3.socket-timeout`. ({issue}`26263`)
* Reduce query failures from S3 throttling. ({issue}`26407`)

## Iceberg connector

* Prevent workers from going into full GC or crashing when decoding unusually large parquet footers. ({issue}`25973`)
* Add support for using GCS without credentials. ({issue}`25810`)
* Fix latency regression and potential query failures for `REFRESH MATERIALIZED VIEW` command from 475 release. ({issue}`26051`)
* Fix incorrect results when reading from parquet files produced by old versions of pyarrow. ({issue}`26058`)
* Add ability to detect resource leakage in the runtime. ({issue}`26087`)
* Prevent creating multiple Alluxio client which can cause excessive resource usage. ({issue}`26121`)
* Remove `iceberg.rest-catalog.sigv4-enabled` config property and add `SIGV4` to `iceberg.rest-catalog.security`. ({issue}`26218`)
* Fix failure when executing `optimize_manifests` procedure on tables having `NULL` on the top level partition. ({issue}`26185`)
* Fix skipping of row groups when the trino type is different from logical types in case of parquet files. ({issue}`26203`)
* Add `azure.multipart-write-enabled` that enables multipart uploads for large files. ({issue}`26225`)
* Rename `s3.socket-read-timeout` config property to `s3.socket-timeout`. ({issue}`26263`)
* Improve performance of expire_snapshots procedure. ({issue}`26230`)
* Reduce memory usage of remove_orphan_files procedure. ({issue}`25847`)
* Improve performance of remove_orphan_files procedure. ({issue}`26326`)
* Improve performance of queries on `$files` metadata table. ({issue}`25677`)
* Add `compression_codec` table property and remove `compression_codec` session property. ({issue}`25755`)
* Reduce query failures from S3 throttling. ({issue}`26407`)

## Ignite connector

## JMX connector

## Kafka connector

## Loki connector

## MariaDB connector

## Memory connector

* Add support for default column values. ({issue}`25679`)
* Add support for view refresh operation. ({issue}`25906`)

## MongoDB connector

## MySQL connector

## OpenSearch connector

## Oracle connector

## Pinot connector

## PostgreSQL connector

* Add support for `geometry` types installed in schemas other than `public`. ({issue}`25972`)

## Prometheus connector

## Redis connector

## Redshift connector

## SingleStore connector

## Snowflake connector

## SQL Server connector

## TPC-H connector

## TPC-DS connector

## Vertica connector

## SPI

* Remove `ConnectorSession` from `Type.getObjectValue`. ({issue}`25945`)
* Remove unused `NodeManager` `getEnvironment` method. ({issue}`26096`)
* Deprecate `NodeManager` `getCurrentNode` in favor of `ConnectorContext` `getCurrentNode`. ({issue}`26096`)
* Remove `@Experimental` annotation. ({issue}`26200`)
* Remove deprecated `ConnectorPageSource.getNextPage` method. ({issue}`26222`)
