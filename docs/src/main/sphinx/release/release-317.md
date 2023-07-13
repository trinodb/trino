# Release 317 (1 Aug 2019)

## General

- Fix {func}`url_extract_parameter` when the query string contains an encoded `&` or `=` character.
- Export MBeans from the `db` resource group configuration manager. ({issue}`1151`)
- Add {func}`all_match`, {func}`any_match`, and {func}`none_match` functions. ({issue}`1045`)
- Add support for fractional weights in {func}`approx_percentile`. ({issue}`1168`)
- Add support for node dynamic filtering for semi-joins and filters when the experimental
  WorkProcessor pipelines feature is enabled. ({issue}`1075`, {issue}`1155`, {issue}`1119`)
- Allow overriding session time zone for clients via the
  `sql.forced-session-time-zone` configuration property. ({issue}`1164`)

## Web UI

- Fix tooltip visibility on stage performance details page. ({issue}`1113`)
- Add planning time to query details page. ({issue}`1115`)

## Security

- Allow schema owner to create, drop, and rename schema when using file-based
  connector access control. ({issue}`1139`)
- Allow respecting the `X-Forwarded-For` header when retrieving the IP address
  of the client submitting the query. This information is available in the
  `remoteClientAddress` field of the `QueryContext` class for query events.
  The behavior can be controlled via the `dispatcher.forwarded-header`
  configuration property, as the header should only be used when the Presto
  coordinator is behind a proxy. ({issue}`1033`)

## JDBC driver

- Fix `DatabaseMetaData.getURL()` to include the `jdbc:` prefix. ({issue}`1211`)

## Elasticsearch connector

- Add support for nested fields. ({issue}`1001`)

## Hive connector

- Fix bucketing version safety check to correctly disallow writes
  to tables that use an unsupported bucketing version. ({issue}`1199`)
- Fix metastore error handling when metastore debug logging is enabled. ({issue}`1152`)
- Improve performance of file listings in `system.sync_partition_metadata` procedure,
  especially for S3. ({issue}`1093`)

## Kudu connector

- Update Kudu client library version to `1.10.0`. ({issue}`1086`)

## MongoDB connector

- Allow passwords to contain the `:` or `@` characters. ({issue}`1094`)

## PostgreSQL connector

- Add support for reading `hstore` data type. ({issue}`1101`)

## SPI

- Allow delete to be implemented for non-legacy connectors. ({issue}`1015`)
- Remove deprecated method from `ConnectorPageSourceProvider`. ({issue}`1095`)
