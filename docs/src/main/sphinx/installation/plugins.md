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
many plugins as part of the binary package.

Every Trino release publishes each plugin as a ZIP archive to the [Maven Central
Repository](https://central.sonatype.com/). Refer to [](plugins-list) for details.
The specific location is derived from the Maven coordinates of each plugin as
defined in the `pom.xml` of the source code for the plugin.

For example, the PostgreSQL connector plugin can be found in the
`plugin/trino-postgresql` directory, and the `pom.xml` file contains the
following identifier section:

```xml
<parent>
    <groupId>io.trino</groupId>
    <artifactId>trino-root</artifactId>
    <version>470</version>
    <relativePath>../../pom.xml</relativePath>
</parent>

<artifactId>trino-postgresql</artifactId>
<packaging>trino-plugin</packaging>
```

The Maven coordinates are therefore `io.trino:trino-postgresql:470` with version
or `io.trino:trino-postgresql` without version. Use this term for a [search to
locate the
artifact](https://central.sonatype.com/search?q=io.trino%3Atrino-postgresql).

After searching, click **View all** next to **Latest version**, then click
**Browse** to find the ZIP file for the desired version.

The coordinates translate into a path to the ZIP archive on the Maven Central
Repository. Use this URL to download the plugin.

```
https://repo1.maven.org/maven2/io/trino/trino-postgresql/470/trino-postgresql-470.zip
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
You can also [download](plugins-download) them from the Maven Central Repository
with the listed coordinates.

:::{list-table} List of plugins 
:widths: 25, 25, 25, 25 
:header-rows: 1

* - Plugin directory
  - Description
  - Maven coordinates
  - Download
* - ai-functions
  - [](/functions/ai)
  - [io.trino:trino-ai-functions](https://central.sonatype.com/search?q=io.trino%3Atrino-ai-functions)
  - {maven_download}`ai-functions`  
* - bigquery
  - [](/connector/bigquery)
  - [io.trino:trino-bigquery](https://central.sonatype.com/search?q=io.trino%3Atrino-bigquery)
  - {maven_download}`bigquery`
* - blackhole
  - [](/connector/blackhole)
  - [io.trino:trino-blackhole](https://central.sonatype.com/search?q=io.trino%3Atrino-blackhole)
  - {maven_download}`blackhole`
* - cassandra
  - [](/connector/cassandra)
  - [io.trino:trino-cassandra](https://central.sonatype.com/search?q=io.trino%3Atrino-cassandra)
  - {maven_download}`cassandra`
* - clickhouse
  - [](/connector/clickhouse)
  - [io.trino:trino-clickhouse](https://central.sonatype.com/search?q=io.trino%3Atrino-clickhouse)
  - {maven_download}`clickhouse`
* - delta-lake
  - [](/connector/delta-lake)
  - [io.trino:trino-delta-lake](https://central.sonatype.com/search?q=io.trino%3Atrino-delta-lake)
  - {maven_download}`delta-lake`
* - druid
  - [](/connector/druid)
  - [io.trino:trino-druid](https://central.sonatype.com/search?q=io.trino%3Atrino-druid)
  - {maven_download}`druid`
* - duckdb
  - [](/connector/duckdb)
  - [io.trino:trino-duckdb](https://central.sonatype.com/search?q=io.trino%3Atrino-duckdb)
  - {maven_download}`duckdb`
* - elasticsearch
  - [](/connector/elasticsearch)
  - [io.trino:trino-elasticsearch](https://central.sonatype.com/search?q=io.trino%3Atrino-elasticsearch)
  - {maven_download}`elasticsearch`
* - example-http
  - [](/develop/example-http)
  - [io.trino:trino-example-http](https://central.sonatype.com/search?q=io.trino%3Atrino-example-http)
  - {maven_download}`example-http`
* - exasol
  - [](/connector/exasol)
  - [io.trino:trino-exasol](https://central.sonatype.com/search?q=io.trino%3Atrino-exasol)
  - {maven_download}`exasol`
* - exchange-filesystem
  - [](/admin/fault-tolerant-execution) exchange file system
  - [io.trino:trino-exchange-filesystem](https://central.sonatype.com/search?q=io.trino%3Atrino-exchange-filesystem)
  - {maven_download}`exchange-filesystem`
* - exchange-hdfs
  - [](/admin/fault-tolerant-execution) exchange file system for HDFS
  - [io.trino:trino-exchange-hdfs](https://central.sonatype.com/search?q=io.trino%3Atrino-exchange-hdfs)
  - {maven_download}`exchange-hdfs`
* - faker
  - [](/connector/faker)
  - [io.trino:trino-faker](https://central.sonatype.com/search?q=io.trino%3Atrino-faker)
  - {maven_download}`faker`
* - functions-python
  - [](/udf/python)
  - [io.trino:trino-functions-python](https://central.sonatype.com/search?q=io.trino%3Atrino-functions-python)
  - {maven_download}`functions-python`
* - geospatial
  - [](/functions/geospatial)
  - [io.trino:trino-geospatial](https://central.sonatype.com/search?q=io.trino%3Atrino-geospatial)
  - {maven_download}`geospatial`
* - google-sheets
  - [](/connector/googlesheets)
  - [io.trino:trino-google-sheets](https://central.sonatype.com/search?q=io.trino%3Atrino-google-sheets)
  - {maven_download}`google-sheets`
* - hive
  - [](/connector/hive)
  - [io.trino:trino-hive](https://central.sonatype.com/search?q=io.trino%3Atrino-hive)
  - {maven_download}`hive`
* - http-event-listener
  - [](/admin/event-listeners-http)
  - [io.trino:trino-http-event-listener](https://central.sonatype.com/search?q=io.trino%3Atrino-http-event-listener)
  - {maven_download}`http-event-listener`
* - http-server-event-listener
  - HTTP server event listener
  - [io.trino:trino-http-server-event-listener](https://central.sonatype.com/search?q=io.trino%3Atrino-http-server-event-listener)
  - {maven_download}`http-server-event-listener`
* - hudi
  - [](/connector/hudi)
  - [io.trino:trino-hudi](https://central.sonatype.com/search?q=io.trino%3Atrino-hudi)
  - {maven_download}`hudi`
* - iceberg
  - [](/connector/iceberg)
  - [io.trino:trino-iceberg](https://central.sonatype.com/search?q=io.trino%3Atrino-iceberg)
  - {maven_download}`iceberg`
* - ignite
  - [](/connector/ignite)
  - [io.trino:trino-ignite](https://central.sonatype.com/search?q=io.trino%3Atrino-ignite)
  - {maven_download}`ignite`
* - jmx
  - [](/connector/jmx)
  - [io.trino:trino-jmx](https://central.sonatype.com/search?q=io.trino%3Atrino-jmx)
  - {maven_download}`jmx`
* - kafka
  - [](/connector/kafka)
  - [io.trino:trino-kafka](https://central.sonatype.com/search?q=io.trino%3Atrino-kafka)
  - {maven_download}`kafka`
* - kafka-event-listener
  - [](/admin/event-listeners-kafka)
  - [io.trino:trino-kafka-event-listener](https://central.sonatype.com/search?q=io.trino%3Atrino-kafka-event-listener)
  - {maven_download}`kafka-event-listener`
* - loki
  - [](/connector/loki)
  - [io.trino:trino-loki](https://central.sonatype.com/search?q=io.trino%3Atrino-loki)
  - {maven_download}`loki`
* - mariadb
  - [](/connector/mariadb)
  - [io.trino:trino-mariadb](https://central.sonatype.com/search?q=io.trino%3Atrino-mariadb)
  - {maven_download}`mariadb`
* - memory
  - [](/connector/memory)
  - [io.trino:trino-memory](https://central.sonatype.com/search?q=io.trino%3Atrino-memory)
  - {maven_download}`memory`
* - ml
  - [](/functions/ml)
  - [io.trino:trino-ml](https://central.sonatype.com/search?q=io.trino%3Atrino-ml)
  - {maven_download}`ml`
* - mongodb
  - [](/connector/mongodb)
  - [io.trino:trino-mongodb](https://central.sonatype.com/search?q=io.trino%3Atrino-mongodb)
  - {maven_download}`mongodb`
* - mysql
  - [](/connector/mysql)
  - [io.trino:trino-mysql](https://central.sonatype.com/search?q=io.trino%3Atrino-mysql)
  - {maven_download}`mysql`
* - mysql-event-listener
  - [](/admin/event-listeners-mysql)
  - [io.trino:trino-mysql-event-listener](https://central.sonatype.com/search?q=io.trino%3Atrino-mysql-event-listener)
  - {maven_download}`mysql-event-listener`
* - opa
  - [](/security/opa-access-control)
  - [io.trino:trino-opa](https://central.sonatype.com/search?q=io.trino%3Atrino-opa)
  - {maven_download}`opa`
* - openlineage
  - [](/admin/event-listeners-openlineage)
  - [io.trino:trino-openlineage](https://central.sonatype.com/search?q=io.trino%3Atrino-openlineage)
  - {maven_download}`openlineage`
* - opensearch
  - [](/connector/opensearch)
  - [io.trino:trino-opensearch](https://central.sonatype.com/search?q=io.trino%3Atrino-opensearch)
  - {maven_download}`opensearch`
* - oracle
  - [](/connector/oracle)
  - [io.trino:trino-oracle](https://central.sonatype.com/search?q=io.trino%3Atrino-oracle)
  - {maven_download}`oracle`
* - password-authenticators
  - Password authentication
  - [io.trino:trino-password-authenticators](https://central.sonatype.com/search?q=io.trino%3Atrino-password-authenticators)
  - {maven_download}`password-authenticators`
* - pinot
  - [](/connector/pinot)
  - [io.trino:trino-pinot](https://central.sonatype.com/search?q=io.trino%3Atrino-pinot)
  - {maven_download}`pinot`
* - postgresql
  - [](/connector/postgresql)
  - [io.trino:trino-postgresql](https://central.sonatype.com/search?q=io.trino%3Atrino-postgresql)
  - {maven_download}`postgresql`
* - prometheus
  - [](/connector/prometheus)
  - [io.trino:trino-prometheus](https://central.sonatype.com/search?q=io.trino%3Atrino-prometheus)
  - {maven_download}`prometheus`
* - ranger
  - [](/security/ranger-access-control)
  - [io.trino:trino-ranger](https://central.sonatype.com/search?q=io.trino%3Atrino-ranger)
  - {maven_download}`ranger`
* - redis
  - [](/connector/redis)
  - [io.trino:trino-redis](https://central.sonatype.com/search?q=io.trino%3Atrino-redis)
  - {maven_download}`redis`
* - redshift
  - [](/connector/redshift)
  - [io.trino:trino-redshift](https://central.sonatype.com/search?q=io.trino%3Atrino-redshift)
  - {maven_download}`redshift`
* - resource-group-managers
  - [](/admin/resource-groups)
  - [io.trino:trino-resource-group-managers](https://central.sonatype.com/search?q=io.trino%3Atrino-resource-group-managers)
  - {maven_download}`resource-group-managers`
* - session-property-managers
  - [](/admin/session-property-managers)
  - [io.trino:trino-session-property-managers](https://central.sonatype.com/search?q=io.trino%3Atrino-session-property-managers)
  - {maven_download}`session-property-managers`
* - singlestore
  - [](/connector/singlestore)
  - [io.trino:trino-singlestore](https://central.sonatype.com/search?q=io.trino%3Atrino-singlestore)
  - {maven_download}`singlestore`
* - snowflake
  - [](/connector/snowflake)
  - [io.trino:trino-snowflake](https://central.sonatype.com/search?q=io.trino%3Atrino-snowflake)
  - {maven_download}`snowflake`
* - spooling-filesystem
  - Server side support for [](protocol-spooling)
  - [io.trino:trino-spooling-filesystem](https://central.sonatype.com/search?q=io.trino%3Atrino-spooling-filesystem)
  - {maven_download}`spooling-filesystem`
* - sqlserver
  - [](/connector/sqlserver)
  - [io.trino:trino-sqlserver](https://central.sonatype.com/search?q=io.trino%3Atrino-sqlserver)
  - {maven_download}`sqlserver`
* - teradata-functions
  - [](/functions/teradata)
  - [io.trino:trino-teradata-functions](https://central.sonatype.com/search?q=io.trino%3Atrino-teradata-functions)
  - {maven_download}`teradata-functions`
* - thrift
  - [](/connector/thrift)
  - [io.trino:trino-thrift](https://central.sonatype.com/search?q=io.trino%3Atrino-thrift)
  - {maven_download}`thrift`
* - tpcds
  - [](/connector/tpcds)
  - [io.trino:trino-tpcds](https://central.sonatype.com/search?q=io.trino%3Atrino-tpcds)
  - {maven_download}`tpcds`
* - tpch
  - [](/connector/tpch)
  - [io.trino:trino-tpch](https://central.sonatype.com/search?q=io.trino%3Atrino-tpch)
  - {maven_download}`tpch`
* - vertica
  - [](/connector/vertica)
  - [io.trino:trino-vertica](https://central.sonatype.com/search?q=io.trino%3Atrino-vertica)
  - {maven_download}`vertica`
:::