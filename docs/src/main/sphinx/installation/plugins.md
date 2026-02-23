# Plugins 

Trino uses a plugin architecture to extend its capabilities and integrate with
various data sources and other systems. Trino includes many plugins as part of
the binary packages - specifically the [tarball](glosstarball) and the [Docker
image](glosscontainer).

Plugins implement some of the following capabilities:

* [Connectors](/connector)
* [Authentication types](security-authentication)
* [Access control systems](security-access-control)
* [Event listeners](admin-event-listeners)
* Additional types and global functions
* Block encodings
* Resource group configuration managers
* Session property configuration managers
* Exchange managers
* Spooling managers

All plugins are optional for your use of Trino because they support specific
functionality that is potentially not needed for your use case. Plugins are
located in the `plugin` folder of your Trino installation and are loaded
automatically during Trino startup.

(plugins-download)=
## Download

Typically, downloading a plugin is not necessary because Trino binaries include
many plugins as part of the binary package.  Individual plugin bundles are
published as a ZIP archive to [github](https://github.com/trinodb/trino/releases)
as part of each release.

For example, the PostgreSQL connector plugin can be downloaded at
```
https://github.com/trinodb/trino/releases/download/479/trino-postgresql-479.zip
```

Availability of plugins from other projects and organizations varies widely, and
may require building a plugin from source.

When downloading a plugin you must ensure to download a version of the plugin
that is compatible with your Trino installation. Full compatibility is only
guaranteed when using the same Trino version used for the plugin build and the
deployment, and therefore using the same version is recommended. Use the
documentation or the source code of the specific plugin to confirm and refer to
the [SPI compatibility notes](spi-compatibility) for further technical details.

(plugins-installation)=
## Installation

To install a plugin, extract the ZIP archive into a directory in the `plugin`
directory of your Trino installation on all nodes of the cluster. The directory
contains all necessary resources. 

For example, for a plugin called `example-plugin` with a version of `1.0`,
extract the `example-plugin-1.0.zip` archive. Rename the resulting directory
`example-plugin-1.0` to `example-plugin` and copy it into the `plugin` directory
of your Trino installation on all workers and the coordinator of the cluster.

:::{note} 
Every Trino plugin must be in a separate directory underneath the `plugin`
directory. Do not put JAR files directly into the `plugin` directory. Each
plugin directory should only contain JAR files. Any subdirectories and other
files are ignored.
:::

By default, the plugin directory is the `plugin` directory relative to the
directory in which Trino is installed, but it is configurable using the
configuration variable `plugin.dir` with the launcher. The [Docker
image](/installation/containers) uses the path `/usr/lib/trino/plugin`.

Restart Trino to use the plugin. 

The [trino-packages project](https://github.com/trinodb/trino-packages) contains
example projects to create a tarball and Docker image with a selection of
plugins by installing only the desired plugins.

(plugins-removal)=
## Removal

Plugins can be safely removed if the functionality is not needed or desired on
your Trino cluster. Use the following steps for a safe removal across the
cluster:

* Shut down Trino on all nodes.
* Delete the directory in the `plugin` folder of the Trino installation on all
  nodes. 
* Start Trino on all nodes.

Refer to the [](plugins-list) for relevant directory names.

For repeated deployments, you can remove the plugin from the binary package for
your installation by creating a custom tarball or a custom Docker image.

(plugins-development)=
## Development

You can develop plugins in your own fork of the Trino codebase or a separate
project. Refer to the [](/develop) for further details.

(plugins-list)=
## List of plugins

The following list of plugins is available from the Trino project. They are
included in the build and release process and the resulting the binary packages.

:::{list-table} List of plugins 
:widths: 30, 30, 40 
:header-rows: 1

* - Plugin directory
  - Description
  - Download
* - ai-functions
  - [](/functions/ai)
  - {download_gh}`ai-functions`  
* - bigquery
  - [](/connector/bigquery)
  - {download_gh}`bigquery`
* - blackhole
  - [](/connector/blackhole)
  - {download_gh}`blackhole`
* - cassandra
  - [](/connector/cassandra)
  - {download_gh}`cassandra`
* - clickhouse
  - [](/connector/clickhouse)
  - {download_gh}`clickhouse`
* - delta-lake
  - [](/connector/delta-lake)
  - {download_gh}`delta-lake`
* - druid
  - [](/connector/druid)
  - {download_gh}`druid`
* - duckdb
  - [](/connector/duckdb)
  - {download_gh}`duckdb`
* - elasticsearch
  - [](/connector/elasticsearch)
  - {download_gh}`elasticsearch`
* - example-http
  - [](/develop/example-http)
  - {download_gh}`example-http`
* - exasol
  - [](/connector/exasol)
  - {download_gh}`exasol`
* - exchange-filesystem
  - [](/admin/fault-tolerant-execution) exchange file system
  - {download_gh}`exchange-filesystem`
* - exchange-hdfs
  - [](/admin/fault-tolerant-execution) exchange file system for HDFS
  - {download_gh}`exchange-hdfs`
* - faker
  - [](/connector/faker)
  - {download_gh}`faker`
* - functions-python
  - [](/udf/python)
  - {download_gh}`functions-python`
* - geospatial
  - [](/functions/geospatial)
  - {download_gh}`geospatial`
* - google-sheets
  - [](/connector/googlesheets)
  - {download_gh}`google-sheets`
* - hive
  - [](/connector/hive)
  - {download_gh}`hive`
* - http-event-listener
  - [](/admin/event-listeners-http)
  - {download_gh}`http-event-listener`
* - http-server-event-listener
  - HTTP server event listener
  - {download_gh}`http-server-event-listener`
* - hudi
  - [](/connector/hudi)
  - {download_gh}`hudi`
* - iceberg
  - [](/connector/iceberg)
  - {download_gh}`iceberg`
* - ignite
  - [](/connector/ignite)
  - {download_gh}`ignite`
* - jmx
  - [](/connector/jmx)
  - {download_gh}`jmx`
* - kafka
  - [](/connector/kafka)
  - {download_gh}`kafka`
* - kafka-event-listener
  - [](/admin/event-listeners-kafka)
  - {download_gh}`kafka-event-listener`
* - lakehouse
  - [](/connector/lakehouse)
  - {download_gh}`lakehouse`
* - loki
  - [](/connector/loki)
  - {download_gh}`loki`
* - mariadb
  - [](/connector/mariadb)
  - {download_gh}`mariadb`
* - memory
  - [](/connector/memory)
  - {download_gh}`memory`
* - ml
  - [](/functions/ml)
  - {download_gh}`ml`
* - mongodb
  - [](/connector/mongodb)
  - {download_gh}`mongodb`
* - mysql
  - [](/connector/mysql)
  - {download_gh}`mysql`
* - mysql-event-listener
  - [](/admin/event-listeners-mysql)
  - {download_gh}`mysql-event-listener`
* - opa
  - [](/security/opa-access-control)
  - {download_gh}`opa`
* - openlineage
  - [](/admin/event-listeners-openlineage)
  - {download_gh}`openlineage`
* - opensearch
  - [](/connector/opensearch)
  - {download_gh}`opensearch`
* - oracle
  - [](/connector/oracle)
  - {download_gh}`oracle`
* - password-authenticators
  - Password authentication
  - {download_gh}`password-authenticators`
* - pinot
  - [](/connector/pinot)
  - {download_gh}`pinot`
* - postgresql
  - [](/connector/postgresql)
  - {download_gh}`postgresql`
* - prometheus
  - [](/connector/prometheus)
  - {download_gh}`prometheus`
* - ranger
  - [](/security/ranger-access-control)
  - {download_gh}`ranger`
* - redis
  - [](/connector/redis)
  - {download_gh}`redis`
* - redshift
  - [](/connector/redshift)
  - {download_gh}`redshift`
* - resource-group-managers
  - [](/admin/resource-groups)
  - {download_gh}`resource-group-managers`
* - session-property-managers
  - [](/admin/session-property-managers)
  - {download_gh}`session-property-managers`
* - singlestore
  - [](/connector/singlestore)
  - {download_gh}`singlestore`
* - snowflake
  - [](/connector/snowflake)
  - {download_gh}`snowflake`
* - spooling-filesystem
  - Server side support for [](protocol-spooling)
  - {download_gh}`spooling-filesystem`
* - sqlserver
  - [](/connector/sqlserver)
  - {download_gh}`sqlserver`
* - teradata-functions
  - [](/functions/teradata)
  - {download_gh}`teradata-functions`
* - thrift
  - [](/connector/thrift)
  - {download_gh}`thrift`
* - tpcds
  - [](/connector/tpcds)
  - {download_gh}`tpcds`
* - tpch
  - [](/connector/tpch)
  - {download_gh}`tpch`
* - vertica
  - [](/connector/vertica)
  - {download_gh}`vertica`
:::
