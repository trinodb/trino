# Release 381 (16 May 2022)

## General

* Add support for fault-tolerant execution with exchange spooling on Azure Blob Storage. ({issue}`12211`)
* Add experimental support for {doc}`/functions/table`. ({issue}`1839`)
* Increase the default number of stages allowed for a query from 100 to 150,
  specified with `query.max-stage-count`. ({issue}`12292`)
* Allow configuring the number of partitions for distributed joins and
  aggregations when task-based fault-tolerant execution is enabled. This can be
  set with the `fault-tolerant-execution-partition-count` configuration property
  or the `fault_tolerant_execution_partition_count` session property. ({issue}`12263`)
* Introduce the `least-waste` low memory task killer policy. This policy avoids
  killing tasks that are already executing for a long time, so the amount of
  wasted work is minimized. It can be enabled with the
  `task.low-memory-killer.policy` configuration property. ({issue}`12393`)
* Fix potential planning failure of queries with multiple subqueries. ({issue}`12199`)

## Security

* Add support for automatic discovery of OpenID Connect metadata with OAuth 2.0
  authentication. ({issue}`9788`)
* Re-introduce `ldap.ssl-trust-certificate` as legacy configuration to avoid
  failures when updating Trino version. ({issue}`12187`)
* Fix potential query failure when a table has multiple column masks defined. ({issue}`12262`)
* Fix incorrect masking of columns when multiple rules in file-based system and
  connector access controls match. ({issue}`12203`)
* Fix authentication failure when using the LDAP password authenticator with
  ActiveDirectory. ({issue}`12321`)

## Web UI

* Ensure consistent sort order in the list of workers. ({issue}`12290`)

## Docker image

* Improve Advanced Encryption Standard (AES) processing performance on ARM64
  processors. This is used for operations such as accessing object storage
  systems via TLS/SSL. ({issue}`12251`)

## CLI

* Add automatic suggestions from command history. This can be disabled with the
  `--disable-auto-suggestion` option. ({issue}`11671`)

## BigQuery connector

* Support reading materialized views. ({issue}`12352`)
* Allow skipping view materialization via `bigquery.skip-view-materialization`
  configuration property. ({issue}`12210`)
* Support reading snapshot tables. ({issue}`12380`)

## ClickHouse connector

* Add support for [`COMMENT ON TABLE`](/sql/comment). ({issue}`11216`)
* Prevent data loss when non-transactional insert fails. ({issue}`12229`)

## Druid connector

* Prevent data loss when non-transactional insert fails. ({issue}`12229`)

## Elasticsearch connector

* Improve query performance by simplifying filters sent to Elasticsearch. ({issue}`10717`)
* Fix failure when reading nested timestamp values that are not ISO 8601 formatted. ({issue}`12250`)

## Hive connector

* Fix query failure when the table and partition bucket counts do not match. ({issue}`11885`)

## Iceberg connector

* Add support for {doc}`/sql/update`. ({issue}`12026`)
* Fix potential query failure or incorrect results when reading data from an
  Iceberg table that contains
  [equality delete files](https://iceberg.apache.org/spec/#equality-delete-files). ({issue}`12026`)

## MariaDB connector

* Prevent data loss when non-transactional insert fails. ({issue}`12229`)

## MySQL connector

* Prevent data loss when non-transactional insert fails. ({issue}`12229`)

## Oracle connector

* Prevent data loss when non-transactional insert fails. ({issue}`12229`)

## PostgreSQL connector

* Prevent data loss when non-transactional insert fails. ({issue}`12225`)

## Redis connector

* Allow specifying the refresh interval for fetching the table description with
  the `redis.table-description-cache-ttl` configuration property. ({issue}`12240`)
* Support setting username for the connection with the `redis.user`
  configuration property. ({issue}`12279`)

## Redshift connector

* Prevent data loss when non-transactional insert fails. ({issue}`12229`)

## SingleStore (MemSQL) connector

* Prevent data loss when non-transactional insert fails. ({issue}`12229`)

## SQL Server connector

* Prevent data loss when non-transactional insert fails. ({issue}`12229`)

## SPI

* Remove deprecated `ConnectorMetadata` methods without the retry mode parameter. ({issue}`12342`)
