# Release 314 (7 Jun 2019)

## General

- Fix incorrect results for `BETWEEN` involving `NULL` values. ({issue}`877`)
- Fix query history leak in coordinator. ({issue}`939`, {issue}`944`)
- Fix idle client timeout handling. ({issue}`947`)
- Improve performance of {func}`json_parse` function. ({issue}`904`)
- Visualize plan structure in `EXPLAIN` output. ({issue}`888`)
- Add support for positional access to `ROW` fields via the subscript
  operator. ({issue}`860`)

## CLI

- Add JSON output format. ({issue}`878`)

## Web UI

- Fix queued queries counter in UI. ({issue}`894`)

## Server RPM

- Change default location of the `http-request.log` to `/var/log/presto`. Previously,
  the log would be located in `/var/lib/presto/data/var/log` by default. ({issue}`919`)

## Hive connector

- Fix listing tables and views from Hive 2.3+ Metastore on certain databases,
  including Derby and Oracle. This fixes `SHOW TABLES`, `SHOW VIEWS` and
  reading from `information_schema.tables` table. ({issue}`833`)
- Fix handling of Avro tables with `avro.schema.url` defined in Hive
  `SERDEPROPERTIES`. ({issue}`898`)
- Fix regression that caused ORC bloom filters to be ignored. ({issue}`921`)
- Add support for reading LZ4 and ZSTD compressed Parquet data. ({issue}`910`)
- Add support for writing ZSTD compressed ORC data. ({issue}`910`)
- Add support for configuring ZSTD and LZ4 as default compression methods via the
  `hive.compression-codec` configuration option. ({issue}`910`)
- Do not allow inserting into text format tables that have a header or footer. ({issue}`891`)
- Add `textfile_skip_header_line_count` and `textfile_skip_footer_line_count` table properties
  for text format tables that specify the number of header and footer lines. ({issue}`845`)
- Add `hive.max-splits-per-second` configuration property to allow throttling
  the split discovery rate, which can reduce load on the file system. ({issue}`534`)
- Support overwriting unpartitioned tables for insert queries. ({issue}`924`)

## PostgreSQL connector

- Support PostgreSQL arrays declared using internal type
  name, for example `_int4` (rather than `int[]`). ({issue}`659`)

## Elasticsearch connector

- Add support for mixed-case field names. ({issue}`887`)

## Base-JDBC connector library

- Allow connectors to customize how they store `NULL` values. ({issue}`918`)

## SPI

- Expose the SQL text of the executed prepared statement to `EventListener`. ({issue}`908`)
- Deprecate table layouts for `ConnectorMetadata.makeCompatiblePartitioning()`. ({issue}`689`)
- Add support for delete pushdown into connectors via the `ConnectorMetadata.applyDelete()`
  and `ConnectorMetadata.executeDelete()` methods. ({issue}`689`)
- Allow connectors without distributed tables. ({issue}`893`)
