# Release 316 (8 Jul 2019)

## General

- Fix `date_format` function failure when format string contains non-ASCII
  characters. ({issue}`1056`)
- Improve performance of queries using `UNNEST`.  ({issue}`901`)
- Improve error message when statement parsing fails. ({issue}`1042`)

## CLI

- Fix refresh of completion cache when catalog or schema is changed. ({issue}`1016`)
- Allow reading password from console when stdout is a pipe. ({issue}`982`)

## Hive connector

- Acquire S3 credentials from the default AWS locations if not configured explicitly. ({issue}`741`)
- Only allow using roles and grants with SQL standard based authorization. ({issue}`972`)
- Add support for `CSV` file format. ({issue}`920`)
- Support reading from and writing to Hadoop encryption zones (Hadoop KMS). ({issue}`997`)
- Collect column statistics on write by default. This can be disabled using the
  `hive.collect-column-statistics-on-write` configuration property or the
  `collect_column_statistics_on_write` session property. ({issue}`981`)
- Eliminate unused idle threads when using the metastore cache. ({issue}`1061`)

## PostgreSQL connector

- Add support for columns of type `UUID`. ({issue}`1011`)
- Export JMX statistics for various JDBC and connector operations. ({issue}`906`).

## MySQL connector

- Export JMX statistics for various JDBC and connector operations. ({issue}`906`).

## Redshift connector

- Export JMX statistics for various JDBC and connector operations. ({issue}`906`).

## SQL Server connector

- Export JMX statistics for various JDBC and connector operations. ({issue}`906`).

## TPC-H connector

- Fix `SHOW TABLES` failure when used with a hidden schema. ({issue}`1005`)

## TPC-DS connector

- Fix `SHOW TABLES` failure when used with a hidden schema. ({issue}`1005`)

## SPI

- Add support for pushing simple column and row field reference expressions into
  connectors via the `ConnectorMetadata.applyProjection()` method. ({issue}`676`)
