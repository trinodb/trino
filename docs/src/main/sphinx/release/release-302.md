# Release 302 (6 Feb 2019)

## General

- Fix cluster starvation when wait for minimum number of workers is enabled. ({issue}`155`)
- Fix backup of queries blocked waiting for minimum number of workers. ({issue}`155`)
- Fix failure when preparing statements that contain a quoted reserved word as a table name. ({issue}`80`)
- Fix query failure when spilling is triggered during certain phases of query execution. ({issue}`164`)
- Fix `SHOW CREATE VIEW` output to preserve table name quoting. ({issue}`80`)
- Add {doc}`/connector/elasticsearch`. ({issue}`118`)
- Add support for `boolean` type to {func}`approx_distinct`. ({issue}`82`)
- Add support for boolean columns to `EXPLAIN` with type `IO`. ({issue}`157`)
- Add `SphericalGeography` type and related {doc}`geospatial functions </functions/geospatial>`. ({issue}`166`)
- Remove deprecated system memory pool. ({issue}`168`)
- Improve query performance for certain queries involving `ROLLUP`. ({issue}`105`)

## CLI

- Add `--trace-token` option to set the trace token. ({issue}`117`)
- Display spilled data size as part of debug information. ({issue}`161`)

## Web UI

- Add spilled data size to query details page. ({issue}`161`)

## Security

- Add `http.server.authentication.krb5.principal-hostname` configuration option to set the hostname
  for the Kerberos service principal. ({issue}`146`, {issue}`153`)
- Add support for client-provided extra credentials that can be utilized by connectors. ({issue}`124`)

## Hive connector

- Fix Parquet predicate pushdown for `smallint`, `tinyint` types. ({issue}`131`)
- Add support for Google Cloud Storage (GCS). Credentials can be provided globally using the
  `hive.gcs.json-key-file-path` configuration property, or as a client-provided extra credential
  named `hive.gcs.oauth` if the `hive.gcs.use-access-token` configuration property is enabled. ({issue}`124`)
- Allow creating tables with the `external_location` property pointing to an empty S3 directory. ({issue}`75`)
- Reduce GC pressure from Parquet reader by constraining the maximum column read size. ({issue}`58`)
- Reduce network utilization and latency for S3 when reading ORC or Parquet. ({issue}`142`)

## Kafka connector

- Fix query failure when reading `information_schema.columns` without an equality condition on `table_name`. ({issue}`120`)

## Redis connector

- Fix query failure when reading `information_schema.columns` without an equality condition on `table_name`. ({issue}`120`)

## SPI

- Include query peak task user memory in `QueryCreatedEvent` and `QueryCompletedEvent`. ({issue}`163`)
- Include plan node cost and statistics estimates in `QueryCompletedEvent`. ({issue}`134`)
- Include physical and internal network input data size in `QueryCompletedEvent`. ({issue}`133`)
