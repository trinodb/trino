# Release 467 (6 Dec 2024)

## General

* Add support for the `DISTINCT` clause in windowed aggregate functions. ({issue}`24352`)
* Allow using `LISTAGG` as a windowed aggregate function. ({issue}`24366`)
* Change default protocol for internal communication to HTTP/1.1 to address
  issues with HTTP/2. ({issue}`24299`)
* Return compressed results to clients by default when using the spooling
  protocol. ({issue}`24332`)
* Add application identifier `azure.application-id`, `gcs.application-id`, or
  `s3.application-id` to the storage when using the spooling protocol. ({issue}`24361`)
* Add support for OpenTelemetry tracing to the HTTP, Kafka, and MySQL event
  listener. ({issue}`24389`)
* Fix incorrect handling of SIGTERM signal, which prevented the server from
  shutting down. ({issue}`24380`)
* Fix query failures or missing statistics in `SHOW STATS` when a connector
  returns `NaN` values for table statistics. ({issue}`24315`)

## Docker image

* Remove the `microdnf` package manager.  ({issue}`24281`)

## Iceberg connector

* Add the `$all_manifests` metadata tables. ({issue}`24330`)
* {{breaking}} Remove the deprecated `schema` and `table` arguments from the
  `table_changes` table function. Use `schema_name` and `table_name` instead. ({issue}`24324`)
* {{breaking}} Use the `iceberg.rest-catalog.warehouse` configuration property
  instead of `iceberg.rest-catalog.parent-namespace` with Unity catalogs. ({issue}`24269`)
* Fix failure when writing concurrently with [transformed
  partition](https://iceberg.apache.org/spec/#partition-transforms) columns.
  ({issue}`24160`)
* Clean up table transaction files when `CREATE TABLE` fails. ({issue}`24279`)

## Delta Lake

* Add the `$transactions` metadata table. ({issue}`24330`)
* Add the `operation_metrics` column to the `$history` metadata table. ({issue}`24379`)

## SPI

* {{breaking}} Remove the deprecated `SystemAccessControlFactory#create` method. ({issue}`24382`)
