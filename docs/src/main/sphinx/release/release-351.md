# Release 351 (3 Jan 2021)

## General

* Rename client protocol headers to start with `X-Trino-`.
  Legacy clients can be supported by setting the configuration property
  `protocol.v1.alternate-header-name` to `Presto`. This configuration
  property is deprecated and will be removed in a future release.

## JMX MBean naming

* Rename base domain name for server MBeans to `trino`. The name can
  be changed using the configuration property `jmx.base-name`.
* Rename base domain name for the Elasticsearch, Hive, Iceberg, Raptor,
  and Thrift connectors to `trino.plugin`. The name can be changed
  using the catalog configuration property `jmx.base-name`.

## Server RPM

* Rename installation directories from `presto` to `trino`.

## Docker image

* Publish image as [`trinodb/trino`](https://hub.docker.com/r/trinodb/trino).
* Change base image to `azul/zulu-openjdk-centos`.
* Change configuration directory to `/etc/trino`.
* Rename CLI in image to `trino`.

## CLI

* Use new client protocol header names. The CLI is not compatible with older servers.

## JDBC driver

* Use new client protocol header names. The driver is not compatible with older servers.
* Change driver URL prefix to `jdbc:trino:`.
  The old prefix is deprecated and will be removed in a future release.
* Change driver class to `io.trino.jdbc.TrinoDriver`.
  The old class name is deprecated and will be removed in a future release.
* Rename Java package for all driver classes to `io.trino.jdbc` and rename
  various driver classes such as `TrinoConnection` to start with `Trino`.

## Hive connector

* Rename JMX name for `PrestoS3FileSystem` to `TrinoS3FileSystem`.
* Change configuration properties
  `hive.hdfs.presto.principal` to `hive.hdfs.trino.principal` and
  `hive.hdfs.presto.keytab` to `hive.hdfs.trino.keytab`.
  The old names are deprecated and will be removed in a future release.

## Local file connector

* Change configuration properties
  `presto-logs.http-request-log.location` to `trino-logs.http-request-log.location` and
  `presto-logs.http-request-log.pattern` to `trino-logs.http-request-log.pattern`.
  The old names are deprecated and will be removed in a future release.

## Thrift connector

* Rename Thrift service method names starting with `presto` to `trino`.
* Rename all classes in the Thrift IDL starting with `Presto` to `Trino`.
* Rename configuration properties starting with `presto` to `trino`.

## SPI

* Rename Java package to `io.trino.spi`.
* Rename `PrestoException` to `TrinoException`.
* Rename `PrestoPrincipal` to `TrinoPrincipal`.
* Rename `PrestoWarning` to `TrinoWarning`.
