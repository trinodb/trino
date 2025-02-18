# Release 470 (5 Feb 2025)

## General

* Add [](/connector/duckdb). ({issue}`18031`)
* Add [](/connector/loki). ({issue}`23053`)
* Add support for the [](select-with-session) to set per-query session
  properties with `SELECT` queries. ({issue}`24889`)
* Improve compatibility of fault-tolerant exchange storage with S3-compliant
  object stores. ({issue}`24822`)
* Allow skipping directory schema validation to improve compatibility of
  fault-tolerant exchange storage with HDFS-like file systems. This can be
  configured with the `exchange.hdfs.skip-directory-scheme-validation` property. ({issue}`24627`)
* Export JMX metric for `blockedQueries`. ({issue}`24907`)
* {{breaking}} Remove support for the `optimize_hash_generation` session
  property and the `optimizer.optimize-hash-generation` configuration option.
  ({issue}`24792`)
* Fix failure when using upper-case variable names in SQL user-defined
  functions. ({issue}`24460`)
* Prevent failures of the {func}`array_histogram` function when the input
  contains null values. ({issue}`24765`)

## JDBC driver

* {{breaking}} Raise minimum runtime requirement to Java 11. ({issue}`23639`)

## CLI

* {{breaking}} Raise minimum runtime requirement to Java 11. ({issue}`23639`)

## Delta Lake connector

* Prevent connection leakage when using the Azure Storage file system. ({issue}`24116`)
* Deprecate use of the legacy file system support for Azure Storage, Google
  Cloud Storage, IBM Cloud Object Storage, S3 and S3-compatible object storage
  systems. Use the migration guides for [Azure
  Storage](fs-legacy-azure-migration), [Google Cloud
  Storage](fs-legacy-gcs-migration), and [S3](fs-legacy-s3-migration) to assist
  if you have not switched from legacy support. ({issue}`24878`)
* Fix potential table corruption when using the `vacuum` procedure. ({issue}`24872`)

## Faker connector

* [Derive constraints](faker-statistics) from source data when using `CREATE TABLE ... AS SELECT`. ({issue}`24585`)

## Hive connector

* Deprecate use of the legacy file system support for Azure Storage, Google
  Cloud Storage, IBM Cloud Object Storage, S3 and S3-compatible object storage
  systems. Use the migration guides for [Azure
  Storage](fs-legacy-azure-migration), [Google Cloud
  Storage](fs-legacy-gcs-migration), and [S3](fs-legacy-s3-migration) to assist
  if you have not switched from legacy support. ({issue}`24878`)
* Prevent connection leakage when using the Azure Storage file system. ({issue}`24116`)
* Fix NullPointerException when listing tables on Glue. ({issue}`24834`)

## Hudi connector

* Deprecate use of the legacy file system support for Azure Storage, Google
  Cloud Storage, IBM Cloud Object Storage, S3 and S3-compatible object storage
  systems. Use the migration guides for [Azure
  Storage](fs-legacy-azure-migration), [Google Cloud
  Storage](fs-legacy-gcs-migration), and [S3](fs-legacy-s3-migration) to assist
  if you have not switched from legacy support. ({issue}`24878`)
* Prevent connection leakage when using the Azure Storage file system. ({issue}`24116`)

## Iceberg connector

* Add the [optimize_manifests](iceberg-optimize-manifests) table procedure. ({issue}`14821`)
* Allow configuration of the number of commit retries with the
  `max_commit_retry` table property. ({issue}`22672`)
* Allow caching of table metadata when using the Hive metastore. ({issue}`13115`)
* Deprecate use of the legacy file system support for Azure Storage, Google
  Cloud Storage, IBM Cloud Object Storage, S3 and S3-compatible object storage
  systems. Use the migration guides for [Azure
  Storage](fs-legacy-azure-migration), [Google Cloud
  Storage](fs-legacy-gcs-migration), and [S3](fs-legacy-s3-migration) to assist
  if you have not switched from legacy support. ({issue}`24878`)
* Prevent connection leakage when using the Azure Storage file system. ({issue}`24116`)
* Fix failure when adding a new column with a name containing a dot. ({issue}`24813`)
* Fix failure when reading from tables with [equality
  deletes](https://iceberg.apache.org/spec/#equality-delete-files) with nested
  fields. ({issue}`18625`)
* Fix failure when reading `$entries` and `$all_entries` tables using [equality
  deletes](https://iceberg.apache.org/spec/#equality-delete-files). ({issue}`24775`)

## JMX connector

* Prevent missing metrics values when MBeans in coordinator and workers do not
  match. ({issue}`24908`)

## Kinesis connector

* {{breaking}} Remove the Kinesis connector. ({issue}`23923`) 

## MySQL connector

* Add support for `MERGE` statement. ({issue}`24428`)
* Prevent writing of invalid, negative date values. ({issue}`24809`)

## PostgreSQL connector

* Raise minimum required version to PostgreSQL 12. ({issue}`24836`)
