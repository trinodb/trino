---
myst:
  substitutions:
    default_domain_compaction_threshold: '`256`'
---

# Redshift connector

```{raw} html
<img src="../_static/img/redshift.png" class="connector-logo">
```

The Redshift connector allows querying and creating tables in an
external [Amazon Redshift](https://aws.amazon.com/redshift/) cluster. This can be used to join data between
different systems like Redshift and Hive, or between two different
Redshift clusters.

## Requirements

To connect to Redshift, you need:

- Network access from the Trino coordinator and workers to Redshift.
  Port 5439 is the default port.

## Configuration

To configure the Redshift connector, create a catalog properties file in
`etc/catalog` named, for example, `example.properties`, to mount the
Redshift connector as the `example` catalog. Create the file with the
following contents, replacing the connection properties as appropriate for your
setup:

```text
connector.name=redshift
connection-url=jdbc:redshift://example.net:5439/database
connection-user=root
connection-password=secret
```

The `connection-user` and `connection-password` are typically required and
determine the user credentials for the connection, often a service user. You can
use {doc}`secrets </security/secrets>` to avoid actual values in the catalog
properties files.

(redshift-tls)=
### Connection security

If you have TLS configured with a globally-trusted certificate installed on your
data source, you can enable TLS between your cluster and the data
source by appending a parameter to the JDBC connection string set in the
`connection-url` catalog configuration property.

For example, on version 2.1 of the Redshift JDBC driver, TLS/SSL is enabled by
default with the `SSL` parameter. You can disable or further configure TLS
by appending parameters to the `connection-url` configuration property:

```properties
connection-url=jdbc:redshift://example.net:5439/database;SSL=TRUE;
```

For more information on TLS configuration options, see the [Redshift JDBC driver
documentation](https://docs.aws.amazon.com/redshift/latest/mgmt/jdbc20-configuration-options.html#jdbc20-ssl-option).

```{include} jdbc-authentication.fragment
```

### UNLOAD configuration

The connector brings parallelism by converting Trino query to corresponding 
Redshift `UNLOAD` command to unload query results on to the S3 bucket in the 
form of Parquet files and reading these Parquet files from S3 bucket. Parquet 
files will be removed as Trino query finishes. Additionally, it's preferable to 
define a custom life cycle policy on S3 bucket.
This feature intends to bring query results faster compared to the default JDBC 
approach when fetching large query result set from Redshift.

The following table describes configuration properties for using 
`UNLOAD` command in Redshift connector:

:::{list-table}
:widths: 40, 60
:header-rows: 1

* - Property name
  - Description
* - ``redshift.unload-location``
  - A writeable location in Amazon S3, to be used for temporarily unloading Redshift query results.
* - ``redshift.unload-iam-role``
  - Fully specified ARN of the IAM Role attached to the Redshift cluster. Provided role will be used
    in `UNLOAD` command.
:::

### Multiple Redshift databases or clusters

The Redshift connector can only access a single database within
a Redshift cluster. Thus, if you have multiple Redshift databases,
or want to connect to multiple Redshift clusters, you must configure
multiple instances of the Redshift connector.

To add another catalog, simply add another properties file to `etc/catalog`
with a different name, making sure it ends in `.properties`. For example,
if you name the property file `sales.properties`, Trino creates a
catalog named `sales` using the configured connector.

```{include} jdbc-common-configurations.fragment
```

```{include} query-comment-format.fragment
```

```{include} jdbc-domain-compaction-threshold.fragment
```

```{include} jdbc-case-insensitive-matching.fragment
```

```{include} non-transactional-insert.fragment
```

(redshift-fte-support)=
## Fault-tolerant execution support

The connector supports {doc}`/admin/fault-tolerant-execution` of query
processing. Read and write operations are both supported with any retry policy.

## Querying Redshift

The Redshift connector provides a schema for every Redshift schema.
You can see the available Redshift schemas by running `SHOW SCHEMAS`:

```
SHOW SCHEMAS FROM example;
```

If you have a Redshift schema named `web`, you can view the tables
in this schema by running `SHOW TABLES`:

```
SHOW TABLES FROM example.web;
```

You can see a list of the columns in the `clicks` table in the `web` database
using either of the following:

```
DESCRIBE example.web.clicks;
SHOW COLUMNS FROM example.web.clicks;
```

Finally, you can access the `clicks` table in the `web` schema:

```
SELECT * FROM example.web.clicks;
```

If you used a different name for your catalog properties file, use that catalog
name instead of `example` in the above examples.

(redshift-type-mapping)=
## Type mapping

```{include} jdbc-type-mapping.fragment
```

(redshift-sql-support)=
## SQL support

The connector provides read access and write access to data and metadata in
Redshift. In addition to the {ref}`globally available
<sql-globally-available>` and {ref}`read operation <sql-read-operations>`
statements, the connector supports the following features:

- {doc}`/sql/insert`
- {doc}`/sql/update`
- {doc}`/sql/delete`
- {doc}`/sql/truncate`
- {ref}`sql-schema-table-management`

```{include} sql-update-limitation.fragment
```

```{include} sql-delete-limitation.fragment
```

```{include} alter-table-limitation.fragment
```

```{include} alter-schema-limitation.fragment
```

### Procedures

```{include} jdbc-procedures-flush.fragment
```
```{include} procedures-execute.fragment
```

### Table functions

The connector provides specific {doc}`table functions </functions/table>` to
access Redshift.

(redshift-query-function)=
#### `query(varchar) -> table`

The `query` function allows you to query the underlying database directly. It
requires syntax native to Redshift, because the full query is pushed down and
processed in Redshift. This can be useful for accessing native features which
are not implemented in Trino or for improving query performance in situations
where running a query natively may be faster.

```{include} query-passthrough-warning.fragment
```

For example, query the `example` catalog and select the top 10 nations by
population:

```
SELECT
  *
FROM
  TABLE(
    example.system.query(
      query => 'SELECT
        TOP 10 *
      FROM
        tpch.nation
      ORDER BY
        population DESC'
    )
  );
```

```{include} query-table-function-ordering.fragment
```
