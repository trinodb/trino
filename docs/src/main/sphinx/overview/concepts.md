# Trino concepts

## Overview

To understand Trino, you must first understand the terms and concepts
used throughout the Trino documentation.

While it is easy to understand statements and queries, as an end-user
you should have familiarity with concepts such as stages and splits to
take full advantage of Trino to execute efficient queries. As a
Trino administrator or a Trino contributor you should understand how
Trino's concepts of stages map to tasks and how tasks contain a set
of drivers which process data.

This section provides a solid definition for the core concepts
referenced throughout Trino, and these sections are sorted from most
general to most specific.

:::{note}
The book [Trino: The Definitive Guide](https://trino.io/trino-the-definitive-guide.html) and the research
paper [Presto: SQL on Everything](https://trino.io/paper.html) can
provide further information about Trino and the concepts in use.
:::

(trino-concept-architecture)=
## Architecture

Trino is a distributed query engine that processes data in parallel across
multiple servers. There are two types of Trino servers,
{ref}`coordinators <trino-concept-coordinator>` and
{ref}`workers <trino-concept-worker>`. The following sections describe these
servers and other components of Trino's architecture.

(trino-concept-cluster)=
### Cluster

A Trino cluster consists of several Trino [nodes](trino-concept-node) - one
[coordinator](trino-concept-coordinator) and zero or more
[workers](trino-concept-worker). Users connect to the coordinator with their
[SQL](glossSQL) query tool. The coordinator collaborates with the workers. The
coordinator and the workers access the connected [data
sources](trino-concept-data-source). This access is configured in
[catalogs](trino-concept-catalog).

Processing each query is a stateful operation. The workload is orchestrated by
the coordinator and spread parallel across all workers in the cluster. Each node
runs Trino in one JVM instance, and processing is parallelized further using
threads.

(trino-concept-node)=
### Node

Any Trino server in a specific Trino cluster is considered a **node** of the
[cluster](trino-concept-cluster). Technically this refers to the Java process
running the Trino program, but node is often used to refer to the computer
running the process due to the recommendation to run only one Trino process per
computer.

(trino-concept-coordinator)=
### Coordinator

The Trino coordinator is the server that is responsible for parsing
statements, planning queries, and managing Trino worker nodes.  It is
the "brain" of a Trino installation and is also the node to which a
client connects to submit statements for execution. Every Trino
installation must have a Trino coordinator alongside one or more
Trino workers. For development or testing purposes, a single
instance of Trino can be configured to perform both roles.

The coordinator keeps track of the activity on each worker and
coordinates the execution of a query. The coordinator creates
a logical model of a query involving a series of stages, which is then
translated into a series of connected tasks running on a cluster of
Trino workers.

Coordinators communicate with workers and clients using a REST API.

(trino-concept-worker)=
### Worker

A Trino worker is a server in a Trino installation, which is responsible
for executing tasks and processing data. Worker nodes fetch data from
connectors and exchange intermediate data with each other. The coordinator
is responsible for fetching results from the workers and returning the
final results to the client.

When a Trino worker process starts up, it advertises itself to the discovery
server in the coordinator, which makes it available to the Trino coordinator
for task execution.

Workers communicate with other workers and Trino coordinators
using a REST API.

(trino-concept-client)=
## Client

Clients allow you to connect to Trino, submit SQL queries, and receive the
results. Clients can access all configured data sources using
[catalogs](trino-concept-catalog). Clients are full-featured client applications
or client drivers and libraries that allow you to connect with any application
supporting that driver, or even your own custom application or script.

Clients applications include command line tools, desktop applications, web-based
applications, and software-as-a-service solutions with features such as
interactive SQL query authoring with editors, or rich user interfaces for
graphical query creation, query running and result rendering, visualizations
with charts and graphs, reporting, and dashboard creation.

Client application that support other query languages or user interface
components to build a query, must translate each request to [SQL as supported by
Trino](/language).

More details are available in the [Trino client documentation](/client).

(trino-concept-plugin)=
## Plugin

Trino uses a plugin architecture to extend its capabilities and integrate with
various data sources and other systems. Details about different types of
plugins, installation, removal, and other aspects are available in the [Plugin
documentation](/installation/plugins).

(trino-concept-data-source)=
## Data source

Trino is a query engine that you can use to query many different data sources.
They include data lakes and lakehouses, numerous relational database management
systems, key-value stores, and many other data stores.

[A comprehensive list with more details for each data source is available on the
Trino website](https://trino.io/ecosystem/data-source).

Data sources provide the data for Trino to query. Configure a
[catalog](trino-concept-catalog) with the required Trino
[connector](trino-concept-connector) for the specific data source to access the
data. With Trino you are ready to use any supported
[client](trino-concept-client) to query the data sources using SQL and the
features of your client.

Throughout this documentation, you'll read terms such as connector,
catalog, schema, and table. These fundamental concepts cover Trino's
model of a particular data source and are described in the following
section.

(trino-concept-connector)=
### Connector

A connector adapts Trino to a data source such as a data lake using Hadoop/Hive
or Apache Iceberg, or a relational database such as PostgreSQL. You can think of
a connector the same way you think of a driver for a database. It is an
implementation of Trino's [service provider interface
(SPI)](/develop/spi-overview), which allows Trino to interact with a resource
using a standard API.

Trino contains [many built-in connectors](/connector):

* Connectors for data lakes and lakehouses including the [Delta
  Lake](/connector/delta-lake), [Hive](/connector/hive),
  [Hudi](/connector/hudi), and [Iceberg](/connector/iceberg) connectors.
* Connectors for relational database management systems, including the
  [MySQL](/connector/mysql), [PostgreSQL](/connector/postgresql),
  [Oracle](/connector/oracle), and [SQL Server](/connector/sqlserver)
  connectors.
* Connectors for a variety of other systems, including the
  [Cassandra](/connector/cassandra), [ClickHouse](/connector/clickhouse),
  [OpenSearch](/connector/opensearch), [Pinot](/connector/pinot),
  [Prometheus](/connector/prometheus), [SingleStore](/connector/singlestore),
  and [Snowflake](/connector/snowflake) connectors.
* A number of other utility connectors such as the [JMX](/connector/jmx),
  [System](/connector/system), and [TPC-H](/connector/tpch) connectors.

Every catalog uses a specific connector. If you examine a catalog configuration
file, you see that each contains a mandatory property `connector.name` with the
value identifying the connector.

(trino-concept-catalog)=
### Catalog

A Trino catalog is a collection of configuration properties used to access a
specific data source, including the required connector and any other details
such as credentials and URL. Catalogs are defined in properties files stored in
the Trino configuration directory. The name of the properties file determines
the name of the catalog. For example, the properties file
`etc/example.properties` results in a catalog name `example`.

You can configure and use many catalogs, with different or identical connectors,
to access different data sources. For example, if you have two data lakes, you
can configure two catalogs in a single Trino cluster that both use the Hive
connector, allowing you to query data from both clusters, even within the same
SQL query. You can also use a Hive connector for one catalog to access a data
lake, and use the Iceberg connector for another catalog to access the data
lakehouse. Or, you can configure different catalogs to access different
PostgreSQL database. The combination of different catalogs is determined by your
needs to access different data sources only.

A catalog contains one or more schemas, which in turn contain objects such as
tables, views, or materialized views. When addressing an objects such as tables
in Trino, the fully-qualified name is always rooted in a catalog. For example, a
fully-qualified table name of `example.test_data.test` refers to the `test`
table in the `test_data` schema in the `example` catalog.

### Schema

Schemas are a way to organize tables. Together, a catalog and schema define a
set of tables and other objects that can be queried. When accessing Hive or a
relational database such as MySQL with Trino, a schema translates to the same
concept in the target database. Other types of connectors may organize tables
into schemas in a way that makes sense for the underlying data source.

### Table

A table is a set of unordered rows, which are organized into named columns with
[types](/language/types). This is the same as in any relational database. Type
mapping from source data to Trino is defined by the connector, varies across
connectors, and is documented in the specific connector documentation, for
example the [type mapping in the PostgreSQL connector](postgresql-type-mapping).

## Query execution model

Trino executes SQL statements and turns these statements into queries,
that are executed across a distributed cluster of coordinator and workers.

### Statement

Trino executes ANSI-compatible SQL statements.  When the Trino
documentation refers to a statement, it is referring to statements as
defined in the ANSI SQL standard, which consists of clauses,
expressions, and predicates.

Some readers might be curious why this section lists separate concepts
for statements and queries. This is necessary because, in Trino,
statements simply refer to the textual representation of a statement written
in SQL. When a statement is executed, Trino creates a query along
with a query plan that is then distributed across a series of Trino
workers.

### Query

When Trino parses a statement, it converts it into a query and creates
a distributed query plan, which is then realized as a series of
interconnected stages running on Trino workers. When you retrieve
information about a query in Trino, you receive a snapshot of every
component that is involved in producing a result set in response to a
statement.

The difference between a statement and a query is simple. A statement
can be thought of as the SQL text that is passed to Trino, while a query
refers to the configuration and components instantiated to execute
that statement. A query encompasses stages, tasks, splits, connectors,
and other components and data sources working in concert to produce a
result.

(trino-concept-stage)=
### Stage

When Trino executes a query, it does so by breaking up the execution
into a hierarchy of stages. For example, if Trino needs to aggregate
data from one billion rows stored in Hive, it does so by creating a
root stage to aggregate the output of several other stages, all of
which are designed to implement different sections of a distributed
query plan.

The hierarchy of stages that comprises a query resembles a tree.
Every query has a root stage, which is responsible for aggregating
the output from other stages. Stages are what the coordinator uses to
model a distributed query plan, but stages themselves don't run on
Trino workers.

(trino-concept-task)=
### Task

As mentioned in the previous section, stages model a particular
section of a distributed query plan, but stages themselves don't
execute on Trino workers. To understand how a stage is executed,
you need to understand that a stage is implemented as a series of
tasks distributed over a network of Trino workers.

Tasks are the "work horse" in the Trino architecture as a distributed
query plan is deconstructed into a series of stages, which are then
translated to tasks, which then act upon or process splits. A Trino
task has inputs and outputs, and just as a stage can be executed in
parallel by a series of tasks, a task is executing in parallel with a
series of drivers.

(trino-concept-splits)=
### Split

Tasks operate on splits, which are sections of a larger data
set. Stages at the lowest level of a distributed query plan retrieve
data via splits from connectors, and intermediate stages at a higher
level of a distributed query plan retrieve data from other stages.

When Trino is scheduling a query, the coordinator queries a
connector for a list of all splits that are available for a table.
The coordinator keeps track of which machines are running which tasks,
and what splits are being processed by which tasks.

### Driver

Tasks contain one or more parallel drivers. Drivers act upon data and
combine operators to produce output that is then aggregated by a task
and then delivered to another task in another stage. A driver is a
sequence of operator instances, or you can think of a driver as a
physical set of operators in memory. It is the lowest level of
parallelism in the Trino architecture. A driver has one input and
one output.

### Operator

An operator consumes, transforms and produces data. For example, a table
scan fetches data from a connector and produces data that can be consumed
by other operators, and a filter operator consumes data and produces a
subset by applying a predicate over the input data.

### Exchange

Exchanges transfer data between Trino nodes for different stages of
a query. Tasks produce data into an output buffer and consume data
from other tasks using an exchange client.
