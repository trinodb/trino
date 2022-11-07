# Release 371 (16 Feb 2022)

## General

* Add support for using secrets in database backed resource group manager
  configuration. ({issue}`10996`)
* Add support for the user group selector rule in database backed resource group
  manager. ({issue}`10914`)
* Remove `deprecated.disable-set-properties-security-check-for-create-ddl`
  configuration toggle. ({issue}`10923`)
* Prevent infinite planning loop by removing redundant predicates above table
  scan. ({issue}`10532`)
* Prevent time outs for planning of certain queries. ({issue}`10985`)
* Fix performance regression in internal communication authentication
  processing. ({issue}`10954`)
* Fix failure when casting values to `decimal(38, 38)`. ({issue}`10946`)
* Enforce timeout for idle transactions. ({issue}`10923`)
* Ensure removal of all catalog session properties when using session property
  defaults with transactions. ({issue}`10923`)

## Security

* Invoke correct authorization check when table is created via `CREATE TABLE
  AS`. ({issue}`10939`)

## ClickHouse connector

* Remove support for ClickHouse connector in Altinity distribution 20.3.
  ({issue}`10975`)
* Add missing output of table properties for `SHOW CREATE TABLE` statements.
  ({issue}`11027`)

## Hive connector

* Allow specifying AWS role session name via S3 security mapping config.
  ({issue}`10714`)
* Disallow writes to bucketed tables recognized as created by Spark to prevent
  data corruption. Spark uses a custom bucketing hash function that is not
  compatible with Hive and Trino. ({issue}`10815`)
* Fix failure when reading Hive tables that contain symlinks that are text
  files. ({issue}`10910`)
* Fix metastore impersonation for Avro tables. ({issue}`11035`)

## Iceberg connector

* Allow running queries performing DML on Iceberg tables with fault-tolerant
  execution. ({issue}`10622`)
* Create files of up to approximately 1GB of size when writing. This can be
  configured using `hive.target-max-file-size` catalog property or
  `target_max_file_size` session property. ({issue}`10957`)

## Kudu connector

* Drop support for Kudu versions older than 1.13.0. ({issue}`10940`)

## SQL Server connector

* Fix incorrect results when negative dates are specified in predicates.
  ({issue}`10263`)
* Fix incorrect results when writing negative dates. ({issue}`10263`)

## SPI

* Add `ConnectorSession` to the `Connector` `getMetadata` method. The former
  signature is deprecated and should be updated. ({issue}`9482`)
* Remove deprecated `checkCanCreateTable` and `checkCanCreateMaterializedView`
  methods not taking parameters. ({issue}`10939`)
