# Release 347 (25 Nov 2020)

## General

* Add `ALTER VIEW ... SET AUTHORIZATION` syntax for changing owner of the view. ({issue}`5789`)
* Add support for `INTERSECT ALL` and `EXCEPT ALL`. ({issue}`2152`)
* Add {func}`contains_sequence` function. ({issue}`5593`)
* Support defining cluster topology (used for query scheduling) using network subnets. ({issue}`4862`)
* Improve query performance by reducing worker to worker communication overhead. ({issue}`5905`, {issue}`5949`)
* Allow disabling client HTTP response compression, which can improve throughput over fast network links.
  Compression can be disabled globally via the `query-results.compression-enabled` config property, for CLI via
  the `--disable-compression` flag, and for the JDBC driver via the `disableCompression` driver property. ({issue}`5818`)
* Rename ``rewrite-filtering-semi-join-to-inner-join`` session property to ``rewrite_filtering_semi_join_to_inner_join``. ({issue}`5954`)
* Throw a user error when session property value cannot be decoded. ({issue}`5731`)
* Fix query failure when expressions that produce values of type `row` are used in a `VALUES` clause. ({issue}`3398`)

## Server

* A minimum Java version of 11.0.7 is now required for Presto to start. This is to mitigate JDK-8206955. ({issue}`5957`)

## Security

* Add support for multiple LDAP bind patterns. ({issue}`5874`)
* Include groups for view owner when checking permissions for views. ({issue}`5945`)

## JDBC driver

* Implement `addBatch()`, `clearBatch()` and `executeBatch()` methods in `PreparedStatement`. ({issue}`5507`)

## CLI

* Add support for providing queries to presto-cli via shell redirection. ({issue}`5881`)

## Docker image

* Update Presto docker image to use CentOS 8 as the base image. ({issue}`5920`)

## Hive connector

* Add support for `ALTER VIEW  ... SET AUTHORIZATION` SQL syntax to change the view owner. This supports Presto and Hive views. ({issue}`5789`)
* Allow configuring HDFS replication factor via the `hive.dfs.replication` config property. ({issue}`1829`)
* Add access checks for tables in Hive Procedures. ({issue}`1489`)
* Decrease latency of `INSERT` and `CREATE TABLE AS ...` queries by updating table and column statistics in parallel. ({issue}`3638`)
* Fix leaking S3 connections when querying Avro tables. ({issue}`5562`)

## Kudu connector

* Add dynamic filtering support. It can be enabled by setting a non-zero duration value for ``kudu.dynamic-filtering.wait-timeout`` config property
  or ``dynamic_filtering_wait_timeout`` session property. ({issue}`5594`)

## MongoDB connector

* Improve performance of queries containing a `LIMIT` clause. ({issue}`5870`)

## Other connectors

* Improve query performance by compacting large pushed down predicates for the PostgreSQL, MySQL, Oracle,
  Redshift and SQL Server connectors. Compaction threshold can be changed using the ``domain-compaction-threshold``
  config property or ``domain_compaction_threshold`` session property. ({issue}`6057`)
* Improve performance for the PostgreSQL, MySQL, SQL Server connectors for certain complex queries involving
  aggregation and predicates by pushing the aggregation and predicates computation into the remote database. ({issue}`4112`)

## SPI

* Add support for connectors to redirect table scan operations to another connector. ({issue}`5792`)
* Add physical input bytes and rows for table scan operation to query completion event. ({issue}`5872`)
