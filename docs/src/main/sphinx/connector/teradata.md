# Teradata connector

```{raw} html
<img src="../_static/img/teradata.png" class="connector-logo">
```

The Teradata connector allows querying and creating tables in an external Teradata database.
This can be used to join data between different systems like Teradata and Hive, or between different Teradata instances.

## Requirements

To connect to Teradata, you need:

- Teradata Database
- Network access from the Trino coordinator and workers to Teradata. Port 1025 is the default port

## Configuration

To configure the Teradata connector, create a catalog properties file in `etc/catalog` named, for example, `teradata.properties`, to mount the Teradata connector as the `teradata`
catalog. Create the file with the following contents, replacing the connection properties as appropriate for your setup:

```properties
connector.name=teradata
connection-url=jdbc:teradata://example.teradata.com/CHARSET=UTF8,TMODE=ANSI,LOGMECH=TD2
connection-user=***
connection-password=***
```

The `connection-url` defines the connection information and parameters to pass to the Teradata JDBC driver. The supported parameters for the URL are available in
the [Teradata JDBC documentation](https://teradata-docs.s3.amazonaws.com/doc/connectivity/jdbc/reference/current/jdbcug_chapter_2.html#BABJIHBJ).

For example, the following `connection-url` configures character encoding, transaction mode, and authentication.

```properties
connection-url=jdbc:teradata://example.teradata.com/CHARSET=UTF8,TMODE=ANSI,LOGMECH=TD2
```

The `connection-user` and `connection-password` are typically required and determine the user credentials for the connection, often a service user.

### Connection security

If you have TLS configured with a globally-trusted certificate installed on your data source, you can enable TLS between your cluster and the data source by appending parameters to
the JDBC connection string set in the connection-url catalog configuration property.

For example, to specify SSLMODE:

```properties
connection-url=jdbc:teradata://example.teradata.com/SSLMODE=REQUIRED
```

For more information on TLS configuration options, see the
Teradata [JDBC documentation](https://teradata-docs.s3.amazonaws.com/doc/connectivity/jdbc/reference/current/jdbcug_chapter_2.html#URL_SSLMODE_).

```{include} jdbc-authentication.fragment
```

### Multiple Teradata databases

You can have as many catalogs as you need, so if you have additional Teradata databases, simply add another properties file to etc/catalog with a different name, making sure it
ends in .properties. For example, if you name the property file sales.properties, Trino creates a catalog named sales using the configured connector.

## Type mapping

Because Trino and Teradata each support types that the other does not, this
connector {ref}`modifies some types <type-mapping-overview>` when reading data.
Refer to the following sections for type mapping in when reading data from Teradata
to Trino.

### Teradata type to Trino type mapping

The connector maps Teradata types to the corresponding Trino types following
this table:

:::{list-table} Teradata type to Trino type mapping
:widths: 30, 30, 40
:header-rows: 1

*
    - Teradata type
    - Trino type
    - Notes
*
    - `TINYINT`
    - `TINYINT`
    -

*
    - `SMALLINT`

    - `SMALLINT`
    -

*
    - `INTEGER`

    - `INTEGER`
    -

*
    - `BIGINT`

    - `BIGINT`
    -

*
    - `REAL`

    - `DOUBLE`
    -

*
    - `DOUBLE`

    - `DOUBLE`
    -

*
    - `FLOAT`

    - `DOUBLE`
    -

*
    - `NUMBER(p, s)`

    - `DECIMAL(p, s)`
    - `DECIMAL(p, s)` is an alias of `NUMERIC(p, s)`. See
      [](postgresql-decimal-type-handling) for more information.

*
    - `NUMERIC(p, s)`

    - `DECIMAL(p, s)`
    - `DECIMAL(p, s)` is an alias of `NUMERIC(p, s)`. See
      [](postgresql-decimal-type-handling) for more information.

*
    - `DECIMAL(p, s)`

    - `DECIMAL(p, s)`
    - `DECIMAL(p, s)` is an alias of `NUMERIC(p, s)`. See
      [](postgresql-decimal-type-handling) for more information.

*
    - `CHAR(n)`

    - `CHAR(n)`
    -

*
    - `CHARACTER(n)`

    - `CHAR(n)`
    -

*
    - `VARCHAR(n)`

    - `VARCHAR(n)`
    -

*
    - `BINARY`

    - `VARBINARY`
    -

*
    - `VARBINARY`

    - `VARBINARY`
    -

*
    - `BLOB`

    - `VARBINARY`
    -

*
    - `DATE`

    - `DATE`
    -

*
    - `TIME(n)`

    - `TIME(n)`
    -

*
    - `TIMESTAMP(n)`

    - `TIMESTAMP(n)`
    -

*
    - `TIMESTAMP(n) WITH TIME ZONE`

    - `TIMESTAMP(n) WITH TIME ZONE`
    -

*
    - `TIME(n) WITH TIME ZONE`

    - `TIME(n) WITH TIME ZONE`
    -

*
    - `JSON`

    - `JSON`
    -

:::

No other types are supported.

```{include} jdbc-type-mapping.fragment
```

## Querying Teradata

The Teradata connector provides a schema for every Teradata database. You can see the available Teradata databases by running SHOW SCHEMAS:

```
SHOW SCHEMAS FROM teradata;
```

If you have a Teradata database named sales, you can view the tables in this database by running SHOW TABLES:

```
SHOW TABLES FROM teradata.sales;
```

You can see a list of the columns in the orders table in the sales database using either of the following:

```
DESCRIBE teradata.sales.orders;
SHOW COLUMNS FROM teradata.sales.orders;
```

Finally, you can access the orders table in the sales database:

```
SELECT * FROM teradata.sales.orders;
```

## SQL support

The connector provides read access to data and metadata in the Teradata database. In addition to
the [globally available](https://trino.io/docs/current/language/sql-support.html#globally-available-statements)
and [read operation](https://trino.io/docs/current/language/sql-support.html#read-operations) statements, the connector supports the following features:

## Performance

The connector includes a number of performance improvements, detailed in the following sections.

### Table statistics

The Teradata connector can use [table and column statistics](https://trino.io/docs/current/optimizer/statistics.html)
for [cost based optimizations](https://trino.io/docs/current/optimizer/cost-based-optimizations.html), to improve query processing performance based on the actual data in the data
source.
The statistics are collected by Teradata and retrieved by the connector. The table and column statistics are based on Teradata's Data Dictionary views.

You can update statistics in Teradata by running:

```
COLLECT STATISTICS COLUMN (regionkey), COLUMN (name) ON trino_test_teradatajdbcconnect.nation;
```

Please refer to [Statistics](https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/SQL-Data-Definition-Language-Syntax-and-Examples/Statistics-Statements) for more information
on Table Statistics.

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
- {func}`covar_pop`
- {func}`covar_samp`
- {func}`corr`
- {func}`regr_intercept`
- {func}`regr_slope`

```{include} join-pushdown-enabled-true.fragment
```

### Predicate pushdown support

Predicates are pushed down for most types, including `UUID` and temporal
types, such as `DATE`.

The connector does not support pushdown of range predicates, such as `>`,
`<`, or `BETWEEN`, on columns with {ref}`character string types
<string-data-types>` like `CHAR` or `VARCHAR`. Equality predicates, such as
`IN` or `=`, and inequality predicates, such as `!=` on columns with
textual types are pushed down. This ensures correctness of results since the
remote data source may sort strings differently than Trino.

In the following example, the predicate of the first query is not pushed down
since `name` is a column of type `VARCHAR` and `>` is a range predicate.
The other queries are pushed down.

```sql
-- Not pushed down
SELECT * FROM nation WHERE name > 'CANADA';
-- Pushed down
SELECT * FROM nation WHERE name != 'CANADA';
SELECT * FROM nation WHERE name = 'CANADA';
```

