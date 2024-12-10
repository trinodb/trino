# Release 468 (18 Dec 2024)

* Add support for Python user-defined functions. NEED TO WRITE DOCS AND LINK ({issue}`24378`)

## General

## Security

## Web UI

## JDBC driver

## Server RPM

## Docker image

## CLI

## BigQuery connector

## Blackhole connector

## Cassandra connector

## ClickHouse connector

## Delta Lake connector

## Druid connector

## Elasticsearch connector

## Exasol connector

## Faker connector

## Google Sheets connector

## Hive connector

* Deactivate bucket execution when not useful in query processing. ({issue}`23432`)
* Enable mismatched bucket execution optimization by default. This can be
  disabled with `hive.optimize-mismatched-bucket-count` configuration property,
  and the `optimize_mismatched_bucket_count` session property. ({issue}`23432`)

## Hudi connector

## Iceberg connector

* Add bucketed execution which can improve performance when running a join or
  aggregation on a bucketed table. This can be deactivated with
  `iceberg.bucket-execution` configuration property, and the
  `bucket_execution_enabled` session property. ({issue}`23432`)


## Ignite connector

## JMX connector

## Kafka connector

## Kinesis connector

## Kudu connector

## MariaDB connector

## Memory connector

## MongoDB connector

## MySQL connector

## OpenSearch connector

## Oracle connector

## Phoenix connector

## Pinot connector

## PostgreSQL connector

* Adding non-transactional support for [MERGE statements](/sql/merge). ({issue}`23034`)

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

* Add partitioning push down, which a connector can use to activate optional
  partitioning, or choose between multiple partitioning strategies. ({issue}`23432`)
