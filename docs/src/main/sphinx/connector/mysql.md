---
myst:
  substitutions:
    default_domain_compaction_threshold: '`32`'
---

# MySQL connector

```{raw} html
<img src="../_static/img/mysql.png" class="connector-logo">
```

The MySQL connector allows querying and creating tables in an external
[MySQL](https://www.mysql.com/) instance. This can be used to join data between different
systems like MySQL and Hive, or between two different MySQL instances.

## Requirements

To connect to MySQL, you need:

- MySQL 5.7, 8.0 or higher.
- Network access from the Trino coordinator and workers to MySQL.
  Port 3306 is the default port.

## Configuration

To configure the MySQL connector, create a catalog properties file in
`etc/catalog` named, for example, `example.properties`, to mount the MySQL
connector as the `mysql` catalog. Create the file with the following contents,
replacing the connection properties as appropriate for your setup:

```text
connector.name=mysql
connection-url=jdbc:mysql://example.net:3306
connection-user=root
connection-password=secret
```

The `connection-url` defines the connection information and parameters to pass
to the MySQL JDBC driver. The supported parameters for the URL are
available in the [MySQL Developer Guide](https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-configuration-properties.html).

For example, the following `connection-url` allows you to require encrypted
connections to the MySQL server:

```text
connection-url=jdbc:mysql://example.net:3306?sslMode=REQUIRED
```

The `connection-user` and `connection-password` are typically required and
determine the user credentials for the connection, often a service user. You can
use {doc}`secrets </security/secrets>` to avoid actual values in the catalog
properties files.

(mysql-tls)=

### Connection security

If you have TLS configured with a globally-trusted certificate installed on your
data source, you can enable TLS between your cluster and the data
source by appending a parameter to the JDBC connection string set in the
`connection-url` catalog configuration property.

For example, with version 8.0 of MySQL Connector/J, use the `sslMode`
parameter to secure the connection with TLS. By default the parameter is set to
`PREFERRED` which secures the connection if enabled by the server. You can
also set this parameter to `REQUIRED` which causes the connection to fail if
TLS is not established.

You can set the `sslMode` parameter in the catalog configuration file by
appending it to the `connection-url` configuration property:

```properties
connection-url=jdbc:mysql://example.net:3306/?sslMode=REQUIRED
```

For more information on TLS configuration options, see the [MySQL JDBC security
documentation](https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-security.html#cj-conn-prop_sslMode).

```{include} jdbc-authentication.fragment
```

### Multiple MySQL servers

You can have as many catalogs as you need, so if you have additional
MySQL servers, simply add another properties file to `etc/catalog`
with a different name, making sure it ends in `.properties`. For
example, if you name the property file `sales.properties`, Trino
creates a catalog named `sales` using the configured connector.

```{include} jdbc-common-configurations.fragment
```

```{include} query-comment-format.fragment
```

```{include} jdbc-domain-compaction-threshold.fragment
```

```{include} jdbc-procedures.fragment
```

```{include} jdbc-case-insensitive-matching.fragment
```

```{include} non-transactional-insert.fragment
```

(mysql-type-mapping)=

## Type mapping

Because Trino and MySQL each support types that the other does not, this
connector {ref}`modifies some types <type-mapping-overview>` when reading or
writing data. Data types may not map the same way in both directions between
Trino and the data source. Refer to the following sections for type mapping in
each direction.

### MySQL to Trino type mapping

The connector maps MySQL types to the corresponding Trino types following
this table:

```{eval-rst}
.. list-table:: MySQL to Trino type mapping
  :widths: 30, 20, 50
  :header-rows: 1

  * - MySQL database type
    - Trino type
    - Notes
  * - ``BIT``
    - ``BOOLEAN``
    -
  * - ``BOOLEAN``
    - ``TINYINT``
    -
  * - ``TINYINT``
    - ``TINYINT``
    -
  * - ``TINYINT UNSIGNED``
    - ``SMALLINT``
    -
  * - ``SMALLINT``
    - ``SMALLINT``
    -
  * - ``SMALLINT UNSIGNED``
    - ``INTEGER``
    -
  * - ``INTEGER``
    - ``INTEGER``
    -
  * - ``INTEGER UNSIGNED``
    - ``BIGINT``
    -
  * - ``BIGINT``
    - ``BIGINT``
    -
  * - ``BIGINT UNSIGNED``
    - ``DECIMAL(20, 0)``
    -
  * - ``DOUBLE PRECISION``
    - ``DOUBLE``
    -
  * - ``FLOAT``
    - ``REAL``
    -
  * - ``REAL``
    - ``REAL``
    -
  * - ``DECIMAL(p, s)``
    - ``DECIMAL(p, s)``
    - See :ref:`MySQL DECIMAL type handling <mysql-decimal-handling>`
  * - ``CHAR(n)``
    - ``CHAR(n)``
    -
  * - ``VARCHAR(n)``
    - ``VARCHAR(n)``
    -
  * - ``TINYTEXT``
    - ``VARCHAR(255)``
    -
  * - ``TEXT``
    - ``VARCHAR(65535)``
    -
  * - ``MEDIUMTEXT``
    - ``VARCHAR(16777215)``
    -
  * - ``LONGTEXT``
    - ``VARCHAR``
    -
  * - ``ENUM(n)``
    - ``VARCHAR(n)``
    -
  * - ``BINARY``, ``VARBINARY``, ``TINYBLOB``, ``BLOB``, ``MEDIUMBLOB``, ``LONGBLOB``
    - ``VARBINARY``
    -
  * - ``JSON``
    - ``JSON``
    -
  * - ``DATE``
    - ``DATE``
    -
  * - ``TIME(n)``
    - ``TIME(n)``
    -
  * - ``DATETIME(n)``
    - ``DATETIME(n)``
    -
  * - ``TIMESTAMP(n)``
    - ``TIMESTAMP(n)``
    -
```

No other types are supported.

### Trino to MySQL type mapping

The connector maps Trino types to the corresponding MySQL types following
this table:

```{eval-rst}
.. list-table:: Trino to MySQL type mapping
  :widths: 30, 20, 50
  :header-rows: 1

  * - Trino type
    - MySQL type
    - Notes
  * - ``BOOLEAN``
    - ``TINYINT``
    -
  * - ``TINYINT``
    - ``TINYINT``
    -
  * - ``SMALLINT``
    - ``SMALLINT``
    -
  * - ``INTEGER``
    - ``INTEGER``
    -
  * - ``BIGINT``
    - ``BIGINT``
    -
  * - ``REAL``
    - ``REAL``
    -
  * - ``DOUBLE``
    - ``DOUBLE PRECISION``
    -
  * - ``DECIMAL(p, s)``
    - ``DECIMAL(p, s)``
    - :ref:`MySQL DECIMAL type handling <mysql-decimal-handling>`
  * - ``CHAR(n)``
    - ``CHAR(n)``
    -
  * - ``VARCHAR(n)``
    - ``VARCHAR(n)``
    -
  * - ``JSON``
    - ``JSON``
    -
  * - ``DATE``
    - ``DATE``
    -
  * - ``TIME(n)``
    - ``TIME(n)``
    -
  * - ``TIMESTAMP(n)``
    - ``TIMESTAMP(n)``
    -
```

No other types are supported.

(mysql-decimal-handling)=

```{include} decimal-type-handling.fragment
```

```{include} jdbc-type-mapping.fragment
```

## Querying MySQL

The MySQL connector provides a schema for every MySQL *database*.
You can see the available MySQL databases by running `SHOW SCHEMAS`:

```
SHOW SCHEMAS FROM example;
```

If you have a MySQL database named `web`, you can view the tables
in this database by running `SHOW TABLES`:

```
SHOW TABLES FROM example.web;
```

You can see a list of the columns in the `clicks` table in the `web` database
using either of the following:

```
DESCRIBE example.web.clicks;
SHOW COLUMNS FROM example.web.clicks;
```

Finally, you can access the `clicks` table in the `web` database:

```
SELECT * FROM example.web.clicks;
```

If you used a different name for your catalog properties file, use
that catalog name instead of `example` in the above examples.

(mysql-sql-support)=

## SQL support

The connector provides read access and write access to data and metadata in the
MySQL database. In addition to the {ref}`globally available <sql-globally-available>` and
{ref}`read operation <sql-read-operations>` statements, the connector supports
the following statements:

- {doc}`/sql/insert`
- {doc}`/sql/delete`
- {doc}`/sql/truncate`
- {doc}`/sql/create-table`
- {doc}`/sql/create-table-as`
- {doc}`/sql/drop-table`
- {doc}`/sql/create-schema`
- {doc}`/sql/drop-schema`

```{include} sql-delete-limitation.fragment
```

(mysql-fte-support)=

## Fault-tolerant execution support

The connector supports {doc}`/admin/fault-tolerant-execution` of query
processing. Read and write operations are both supported with any retry policy.

## Table functions

The connector provides specific {doc}`table functions </functions/table>` to
access MySQL.

(mysql-query-function)=

### `query(varchar) -> table`

The `query` function allows you to query the underlying database directly. It
requires syntax native to MySQL, because the full query is pushed down and
processed in MySQL. This can be useful for accessing native features which are
not available in Trino or for improving query performance in situations where
running a query natively may be faster.

```{include} query-passthrough-warning.fragment
```

For example, query the `example` catalog and group and concatenate all
employee IDs by manager ID:

```
SELECT
  *
FROM
  TABLE(
    example.system.query(
      query => 'SELECT
        manager_id, GROUP_CONCAT(employee_id)
      FROM
        company.employees
      GROUP BY
        manager_id'
    )
  );
```

```{include} query-table-function-ordering.fragment
```

## Performance

The connector includes a number of performance improvements, detailed in the
following sections.

(mysql-table-statistics)=

### Table statistics

The MySQL connector can use {doc}`table and column statistics
</optimizer/statistics>` for {doc}`cost based optimizations
</optimizer/cost-based-optimizations>`, to improve query processing performance
based on the actual data in the data source.

The statistics are collected by MySQL and retrieved by the connector.

The table-level statistics are based on MySQL's `INFORMATION_SCHEMA.TABLES`
table. The column-level statistics are based on MySQL's index statistics
`INFORMATION_SCHEMA.STATISTICS` table. The connector can return column-level
statistics only when the column is the first column in some index.

MySQL database can automatically update its table and index statistics. In some
cases, you may want to force statistics update, for example after creating new
index, or after changing data in the table. You can do that by executing the
following statement in MySQL Database.

```text
ANALYZE TABLE table_name;
```

:::{note}
MySQL and Trino may use statistics information in different ways. For this
reason, the accuracy of table and column statistics returned by the MySQL
connector might be lower than than that of others connectors.
:::

**Improving statistics accuracy**

You can improve statistics accuracy with histogram statistics (available since
MySQL 8.0). To create histogram statistics execute the following statement in
MySQL Database.

```text
ANALYZE TABLE table_name UPDATE HISTOGRAM ON column_name1, column_name2, ...;
```

Refer to MySQL documentation for information about options, limitations
and additional considerations.

(mysql-pushdown)=

### Pushdown

The connector supports pushdown for a number of operations:

- {ref}`join-pushdown`
- {ref}`limit-pushdown`
- {ref}`topn-pushdown`

{ref}`Aggregate pushdown <aggregation-pushdown>` for the following functions:

- {func}`avg`
- {func}`count`
- {func}`max`
- {func}`min`
- {func}`sum`
- {func}`stddev`
- {func}`stddev_pop`
- {func}`stddev_samp`
- {func}`variance`
- {func}`var_pop`
- {func}`var_samp`

```{include} pushdown-correctness-behavior.fragment
```

```{include} join-pushdown-enabled-true.fragment
```

```{include} no-pushdown-text-type.fragment
```
