# Release 331 (16 Mar 2020)

## General

- Prevent query failures when worker is shut down gracefully. ({issue}`2648`)
- Fix join failures for queries involving `OR` predicate with non-comparable functions. ({issue}`2861`)
- Ensure query completed event is fired when there is an error during analysis or planning. ({issue}`2842`)
- Fix memory accounting for `ORDER BY` queries. ({issue}`2612`)
- Fix {func}`last_day_of_month` for `timestamp with time zone` values. ({issue}`2851`)
- Fix excessive runtime when parsing deeply nested expressions with unmatched parenthesis. ({issue}`2968`)
- Correctly reject `date` literals that cannot be represented in Presto. ({issue}`2888`)
- Improve query performance by removing redundant data reshuffling. ({issue}`2853`)
- Improve performance of inequality joins involving `BETWEEN`. ({issue}`2859`)
- Improve join performance for dictionary encoded data. ({issue}`2862`)
- Enable dynamic filtering by default. ({issue}`2793`)
- Show reorder join cost in `EXPLAIN ANALYZE VERBOSE` ({issue}`2725`)
- Allow configuring resource groups selection based on user's groups. ({issue}`3023`)
- Add `SET AUTHORIZATION` action to {doc}`/sql/alter-schema`. ({issue}`2673`)
- Add {doc}`/connector/bigquery`. ({issue}`2532`)
- Add support for large prepared statements. ({issue}`2719`)

## Security

- Remove unused `internal-communication.jwt.enabled` configuration property. ({issue}`2709`)
- Rename JWT configuration properties from `http.authentication.jwt.*` to `http-server.authentication.jwt.*`. ({issue}`2712`)
- Add access control checks for query execution, view query, and kill query. This can be
  configured using {ref}`query-rules` in {doc}`/security/file-system-access-control`. ({issue}`2213`)
- Hide columns of tables for which the user has no privileges in {doc}`/security/file-system-access-control`. ({issue}`2925`)

## JDBC driver

- Implement `PreparedStatement.getMetaData()`. ({issue}`2770`)

## Web UI

- Fix copying worker address to clipboard. ({issue}`2865`)
- Fix copying query ID to clipboard. ({issue}`2872`)
- Fix display of data size values. ({issue}`2810`)
- Fix redirect from `/` to `/ui/` when Presto is behind a proxy. ({issue}`2908`)
- Fix display of prepared queries. ({issue}`2784`)
- Display physical input read rate. ({issue}`2873`)
- Add simple form based authentication that utilizes the configured password authenticator. ({issue}`2755`)
- Allow disabling the UI via the `web-ui.enabled` configuration property. ({issue}`2755`)

## CLI

- Fix formatting of `varbinary` in nested data types. ({issue}`2858`)
- Add `--timezone` parameter. ({issue}`2961`)

## Hive connector

- Fix incorrect results for reads from `information_schema` tables and
  metadata queries when using a Hive 3.x metastore. ({issue}`3008`)
- Fix query failure when using Glue metastore and the table storage descriptor has no properties. ({issue}`2905`)
- Fix deadlock when Hive caching is enabled and has a refresh interval configured. ({issue}`2984`)
- Respect `bucketing_version` table property when using Glue metastore. ({issue}`2905`)
- Improve performance of partition fetching from Glue. ({issue}`3024`)
- Add support for bucket sort order in Glue when creating or updating a table or partition. ({issue}`1870`)
- Add support for Hive full ACID tables. ({issue}`2068`, {issue}`1591`, {issue}`2790`)
- Allow data conversion when reading decimal data from Parquet files and precision or scale in the file schema
  is different from the precision or scale in partition schema. ({issue}`2823`)
- Add option to enforce that a filter on a partition key be present in the query. This can be enabled by setting the
  `hive.query-partition-filter-required` configuration property or the `query_partition_filter_required` session property
  to `true`. ({issue}`2334`)
- Allow selecting the `Intelligent-Tiering` S3 storage class when writing data to S3. This can be enabled by
  setting the `hive.s3.storage-class` configuration property to `INTELLIGENT_TIERING`. ({issue}`3032`)
- Hide the Hive system schema `sys` for security reasons. ({issue}`3008`)
- Add support for changing the owner of a schema. ({issue}`2673`)

## MongoDB connector

- Fix incorrect results when queries contain filters on certain data types, such
  as `real` or `decimal`. ({issue}`1781`)

## Other connectors

These changes apply to the MemSQL, MySQL, PostgreSQL, Redshift, Phoenix, and SQL Server connectors.

- Add support for dropping schemas. ({issue}`2956`)

## SPI

- Remove deprecated `Identity` constructors. ({issue}`2877`)
- Introduce a builder for `ConnectorIdentity` and deprecate its public constructors. ({issue}`2877`)
- Add support for row filtering and column masking via the `getRowFilter()` and `getColumnMask()` APIs in
  `SystemAccessControl` and `ConnectorAccessControl`. ({issue}`1480`)
- Add access control check for executing procedures. ({issue}`2924`)
