# Release 320 (10 Oct 2019)

## General

- Fix incorrect parameter binding order for prepared statement execution when
  parameters appear inside a `WITH` clause. ({issue}`1191`)
- Fix planning failure for certain queries involving a mix of outer and
  cross joins. ({issue}`1589`)
- Improve performance of queries containing complex predicates. ({issue}`1515`)
- Avoid unnecessary evaluation of redundant filters. ({issue}`1516`)
- Improve performance of certain window functions when using bounded window
  frames (e.g., `ROWS BETWEEN ... PRECEDING AND ... FOLLOWING`). ({issue}`464`)
- Add {doc}`/connector/kinesis`. ({issue}`476`)
- Add {func}`geometry_from_hadoop_shape`. ({issue}`1593`)
- Add {func}`at_timezone`. ({issue}`1612`)
- Add {func}`with_timezone`. ({issue}`1612`)

## JDBC driver

- Only report warnings on `Statement`, not `ResultSet`, as warnings
  are not associated with reads of the `ResultSet`. ({issue}`1640`)

## CLI

- Add multi-line editing and syntax highlighting. ({issue}`1380`)

## Hive connector

- Add impersonation support for calls to the Hive metastore. This can be enabled using the
  `hive.metastore.thrift.impersonation.enabled` configuration property. ({issue}`43`)
- Add caching support for Glue metastore. ({issue}`1625`)
- Add separate configuration property `hive.hdfs.socks-proxy` for accessing HDFS via a
  SOCKS proxy. Previously, it was controlled with the `hive.metastore.thrift.client.socks-proxy`
  configuration property. ({issue}`1469`)

## MySQL connector

- Add `mysql.jdbc.use-information-schema` configuration property to control whether
  the MySQL JDBC driver should use the MySQL `information_schema` to answer metadata
  queries. This may be helpful when diagnosing problems. ({issue}`1598`)

## PostgreSQL connector

- Add support for reading PostgreSQL system tables, e.g., `pg_catalog` relations.
  The functionality is disabled by default and can be enabled using the
  `postgresql.include-system-tables` configuration property. ({issue}`1527`)

## Elasticsearch connector

- Add support for `VARBINARY`, `TIMESTAMP`, `TINYINT`, `SMALLINT`,
  and `REAL` data types. ({issue}`1639`)
- Discover available tables and their schema dynamically. ({issue}`1639`)
- Add support for special `_id`, `_score` and `_source` columns. ({issue}`1639`)
- Add support for {ref}`full text queries <elasticsearch-full-text-queries>`. ({issue}`1662`)

## SPI

- Introduce a builder for `Identity` and deprecate its public constructors. ({issue}`1624`)
