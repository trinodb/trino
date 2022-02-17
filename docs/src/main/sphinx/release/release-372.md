# Release 372 (2 Mar 2022)

## General

* Add {func}`trim_array` function. ({issue}`11238`)
* Improve handling of prepared statements with long query text by compressing
  them within HTTP headers. This can be configured or disabled using the
  ``protocol.v1.prepared-statement-compression.length-threshold`` configuration
  property. ({issue}`11098`)
* Improve performance of specific queries which compare table columns of type
  `timestamp` with `date` literals. ({issue}`11170`)
* Add redirection awareness for `ADD COLUMN`, `DROP TABLE`, `COMMENT` tasks. ({issue} `11072`)
* Remove support for reserved memory pool. Configuration property
  `experimental.reserved-pool-disabled` can no longer be used. ({issue}`6677`)
* Ensure memory is released completely after query completion. ({issue}`11030`)
* Fix certain queries failing due to dictionary compacting error. ({issue}`11080`)
* Fix `SET SESSION` and `RESET SESSION` not working for catalogs which include
  special characters in their name. ({issue}`11171`)
* Fix bug where queries were not transitioned to `RUNNING` state when task-level
  retries were enabled. ({issue}`11198`)

## Security

* Allow configuration of connect and read timeouts for LDAP authentication. ({issue}`10925`)

## Docker image

* Add a health check to the Docker container image. ({issue}`10413`)

## JDBC driver

* Fix `DatabaseMetaData#getTables` and `DatabaseMetaData#getColumns` to include
  views for Iceberg, Raptor, Accumulo and Blackhole connectors. ({issue}`11063`, {issue}`11060`)

## Base-JDBC connector library

* Fix spurious query failures when metadata cache is not enabled and data
  access depends on the session state. ({issue}`11068`)

## Accumulo connector

* Fix incorrect results when querying `date` type columns. ({issue}`11055`)

## Cassandra connector

* Fix incorrect results when filtering partition keys without projections.
  ({issue}`11001`)

## ClickHouse connector

* Fix spurious query failures when metadata cache is not enabled and extra
  credentials with `user-credential-name` or `password-credential-name` are used
  to access data. ({issue}`11068`)

## Druid connector

* Fix spurious query failures when metadata cache is not enabled and extra
  credentials with `user-credential-name` or `password-credential-name` are used
  to access data. ({issue}`11068`)

## Hive connector

* Allow redirects of Iceberg or Delta tables which have no ``StorageDescriptor``
  in the Glue metastore. ({issue}`11092`)
* Stop logging spurious failures when [storage caching](/connector/hive-caching)
  is used. ({issue}`11101`)
* Allow reading Zstandard compressed Avro files. ({issue}`11090`)
* Fix incorrect query results after writes to a table when directory caching is
  enabled enabled with the `hive.file-status-cache-tables` configuration
  property. ({issue}`10621`)
* Fix potential query failures for queries writing data to tables backed by S3.
  ({issue}`11089`)

## Iceberg connector

* Add support for ``COMMENT ON COLUMN`` statement. ({issue}`11143`)
* Improve query performance after table schema evolved, by fixing the connector
  to support table stats in such case. ({issue}`11091`)
* Fix potential query failures for queries writing data to tables backed by S3. ({issue}`11089`)
* Prevent query failure from dereference pushdown when a column has a comment. ({issue}`11104`)

## Kudu connector

* Add support for Kerberos authentication. ({issue}`10953`)

## MongoDB connector

* Map MongoDB `bindata` type to Trino `varbinary` type if explicit schema does
  not exist. ({issue}`11122`)

## MySQL connector

* Fix spurious query failures when metadata cache is not enabled and extra
  credentials with `user-credential-name` or `password-credential-name` are used
  to access data. ({issue}`11068`)

## Oracle connector

* Fix spurious query failures when metadata cache is not enabled and extra
  credentials with `user-credential-name` or `password-credential-name` are used
  to access data. ({issue}`11068`)

## Phoenix connector

* Fix query failures when applying predicates on `array(char)` type columns. ({issue}`10451`)
* Fix metadata listing failure in case of concurrent table deletion. ({issue}`10904`)

## PostgreSQL connector

* Add support for pushing down joins on character string type columns. ({issue}`10059`)
* Fix spurious query failures when metadata cache is not enabled and extra
  credentials with `user-credential-name` or `password-credential-name` are used
  to access data. ({issue}`11068`)

## Redshift connector

* Fix spurious query failures when metadata cache is not enabled and extra
  credentials with `user-credential-name` or `password-credential-name` are used
  to access data. ({issue}`11068`)

## SingleStore (MemSQL) connector

* Fix spurious query failures when metadata cache is not enabled, and extra
  credentials with `user-credential-name` or `password-credential-name` are used
  to access data. ({issue}`11068`)

## SQL Server connector

* Update JDBC driver to 10.2.0. The new version automatically enables TLS and
  certificate verification. Update the [TLS configuration](sqlserver-tls) to
  keep the old behavior. ({issue}`10898`)
* Fix spurious query failures when metadata cache is not enabled and extra
  credentials with `user-credential-name` or `password-credential-name` are used
  to access data. ({issue}`11068`)

## SPI

* Pass more information about predicates in `ConnectorMetadata#applyFilter`
  invocation. The predicates that cannot be represented with a `TupleDomain` are
  available via `Constraint.getExpression()`. ({issue}`7994`)
