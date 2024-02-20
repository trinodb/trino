# Release 308 (11 Apr 2019)

## General

- Fix a regression that prevented the server from starting on Java 9+. ({issue}`610`)
- Fix correctness issue for queries involving `FULL OUTER JOIN` and `coalesce`. ({issue}`622`)

## Security

- Add authorization for listing table columns. ({issue}`507`)

## CLI

- Add option for specifying Kerberos service principal pattern. ({issue}`597`)

## JDBC driver

- Correctly report precision and column display size in `ResultSetMetaData`
  for `char` and `varchar` columns. ({issue}`615`)
- Add option for specifying Kerberos service principal pattern. ({issue}`597`)

## Hive connector

- Fix regression that could cause queries to fail with `Query can potentially
  read more than X partitions` error. ({issue}`619`)
- Improve ORC read performance significantly. For TPC-DS, this saves about 9.5% of
  total CPU when running over gzip-compressed data. ({issue}`555`)
- Require access to a table (any privilege) in order to list the columns. ({issue}`507`)
- Add directory listing cache for specific tables. The list of tables is specified
  using the  `hive.file-status-cache-tables` configuration property. ({issue}`343`)

## MySQL connector

- Fix `ALTER TABLE ... RENAME TO ...` statement. ({issue}`586`)
- Push simple `LIMIT` queries into the external database. ({issue}`589`)

## PostgreSQL connector

- Push simple `LIMIT` queries into the external database. ({issue}`589`)

## Redshift connector

- Push simple `LIMIT` queries into the external database. ({issue}`589`)

## SQL Server connector

- Fix writing `varchar` values with non-Latin characters in `CREATE TABLE AS`. ({issue}`573`)
- Support writing `varchar` and `char` values with length longer than 4000
  characters in `CREATE TABLE AS`. ({issue}`573`)
- Support writing `boolean` values in `CREATE TABLE AS`. ({issue}`573`)
- Push simple `LIMIT` queries into the external database. ({issue}`589`)

## Elasticsearch connector

- Add support for Search Guard in Elasticsearch connector. Please refer to {doc}`/connector/elasticsearch`
  for the relevant configuration properties. ({issue}`438`)
