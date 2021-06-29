# Release 357 (21 May 2021)

## General

* Add support for subquery expressions that return multiple columns.
  Example: `SELECT x = (VALUES (1, 'a'))` ({issue}`7773`, {issue}`7863`)
* Allow aggregation pushdown when `COUNT(1)` is used with `GROUP BY`. ({issue}`7251`)
* Add support for `CURRENT_CATALOG` and `CURRENT_SCHEMA`. ({issue}`7824`)
* Add {func}`format_number` function. ({issue}`1878`)
* Change `row` to `json` cast to produce JSON objects instead of JSON arrays. This behavior can be restored 
  with the `deprecated.legacy-row-to-json-cast` configuration option. ({issue}`3536`)
* Print dynamic filters summary in `EXPLAIN ANALYZE`. ({issue}`7874`)
* Improve performance for queries using `IN` predicate with a short list of constants. ({issue}`7840`)
* Release memory immediately when queries involving window functions fail. ({issue}`7947`)
* Fix incorrect handling of row expressions for `IN` predicates, quantified comparisons and scalar subqueries. Previously,
  the queries would succeed where they should have failed with a type mismatch error. ({issue}`7797`)
* Fix failure when using `PREPARE` with a `GRANT` statement that contains quoted SQL keywords. ({issue}`7941`)
* Fix cluster instability after executing certain large `EXPLAIN` queries. ({issue}`8017`)

## Security

* Enforce materialized view creator security policies when view is fresh. ({issue}`7618`)
* Use system truststore for OAuth2 and JWK for JWT authentication. Previously, the truststore 
  configured for internal communication was used. This means that globally trusted certificates 
  will work by default. ({issue}`7936`)
* Fix handling of SNI for multiple TLS certificates. ({issue}`8007`)

## Web UI

* Make the UI aware of principal-field (configured with `http-server.authentication.oauth2.principal-field`) when 
  `web-ui.authentication.type` is set to `oauth2`. ({issue}`7526`)

## JDBC driver

* Cancel Trino query execution when JDBC statement is closed. ({issue}` 7819`) 
* Close statement when connection is closed. ({issue}` 7819`)
             
## CLI

* Add `clear` command to clear the screen. ({issue}`7632`)

## BigQuery connector

* Fix failures for queries accessing `information_schema.columns` when `case-insensitive-name-matching` is disabled. ({issue}`7830`)
* Fix query failure when a predicate on a BigQuery `string` column contains a value with a single quote (`'`). ({issue}`7784`)

## ClickHouse connector

* Improve performance of aggregation queries by computing aggregations within ClickHouse. Currently, the following aggregate functions 
  are eligible for pushdown: `count`,  `min`, `max`, `sum` and `avg`. ({issue}`7434`)
* Map ClickHouse `UUID` columns as `UUID` type in Trino instead of `VARCHAR`. ({issue}`7097`)

## Elasticsearch connector

* Support decoding `timestamp` columns encoded as strings containing milliseconds since epoch values. ({issue}`7838`)
* Retry requests with backoff when Elasticsearch is overloaded. ({issue}`7423`)

## Kinesis connector

* Add `kinesis.table-description-refresh-interval` configuration property to set the
  refresh interval for fetching table descriptions from S3. ({issue}`1609`)

## Kudu connector

* Fix query failures for grouped execution on range partitioned tables. ({issue}`7738`)

## MongoDB connector

* Redact the value of `mongodb.credentials` in the server log. ({issue}`7862`)
* Add support for dropping columns. ({issue}`7853`)

## Pinot connector
                                
* Add support for complex filter expressions in passthrough queries. ({issue}`7161`)

## Other connectors

This change applies to the Druid, MemSQL, MySQL, Oracle, Phoenix, PosgreSQL, Redshift, and SQL Server connectors.
* Add rule support for identifier mapping. The rules can be configured via the
`case-insensitive-name-matching.config-file` configuration property. ({issue}`7841`)

## SPI

* Make `ConnectorMaterializedViewDefinition` non-serializable. It is the responsibility of the connector to serialize 
  and store the materialized view definitions in an appropriate format. ({issue}`7762`)
* Deprecate `TupleDomain.transform`. ({issue}`7980`)
