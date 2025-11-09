# Release 479 (TBD 2025)

## General

* Support automatic TLS certificate generation in the `ANNOUNCE` node discovery mode. ({issue}`27030`)
* Add support for specifying the location of the Exchange Manager configuration file. ({issue}`26611`)
* Publish query finishing time in event listener. ({issue}`27202`)
* Enhance after `RemoveTrivialFilters` call `RemoveEmptyUnionBranches` to prune empty union branches. ({issue}`21506`)
* Fix planning failure of certain queries where part of the plan is optimized to empty values. ({issue}`21506`)

## Security

## Web UI

## JDBC driver

## Docker image

* Update JDK to 25.0.1 ({issue}`27117`)

## CLI

* Add a `--extra-header` argument to the trino-cli to support sending arbitrary HTTP headers to Trino({issue}`15826`)

## BigQuery connector

## Blackhole connector

## Cassandra connector

## ClickHouse connector

## Delta Lake connector

## Druid connector

## DuckDB connector

## Elasticsearch connector

## Exasol connector

## Faker connector

## Google Sheets connector

## Hive connector

## Hudi connector

## Iceberg connector

* Add support for disabling token exchange via the `iceberg.rest-catalog.oauth2.token-exchange-enabled` config property. ({issue}`27174`)
* Fix failures when querying `$files` table after changes to table partitioning. ({issue}`26746`)

## Ignite connector

## JMX connector

## Kafka connector

## Loki connector

* Fix failure when initializing the connector. ({issue}`27180`)

## MariaDB connector

## Memory connector

* Fix concurrent modification exception in `RENAME SCHEMA` of the `memory` connector. ({issue}`27205`)

## MongoDB connector

## MySQL connector

## OpenSearch connector

## Oracle connector

## Pinot connector

## PostgreSQL connector

## Prometheus connector

## Redis connector

## Redshift connector

* Fix failure when reading Redshift `character varying` type. ({issue}`27224`)

## SingleStore connector

## Snowflake connector

## SQL Server connector

## TPC-H connector

## TPC-DS connector

## Vertica connector

## SPI

* Add non-callback based entry builder to RowBlockBuilder. ({issue}`27198`)
* Add non-callback based entry builder to ArrayBlockBuilder. ({issue}`27198`)
