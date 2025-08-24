---
myst:
  substitutions:
    default_domain_compaction_threshold: "`1000`"
---

# Databend connector

```{raw} html
<img src="../_static/img/databend.png" class="connector-logo">
```

The Databend connector allows querying tables in an external
[Databend](https://docs.databend.com/) server. This can be used to
query data in the databases on that server, or combine it with other data
from different catalogs accessing Databend or any other supported data source.

## Requirements

To connect to a Databend server, you need:

- Databend installed.
- Network access from the Trino coordinator and workers to the Databend
  server. Port 8000 is the default port.

## Configuration

The connector can query a Databend server. Create a catalog properties file
that specifies the Databend connector by setting the `connector.name` to
`databend`.

For example, create the file `etc/catalog/example.properties`. Replace the
connection properties as appropriate for your setup:

```none
connector.name=databend
connection-url=jdbc:databend://host:8000/
connection-user=exampleuser
connection-password=examplepassword
```

The `connection-url` defines the connection information and parameters to pass
to the Databend JDBC driver. The supported parameters for the URL are
available in the [Databend JDBC driver configuration](https://github.com/databendlabs/databend-jdbc/blob/main/docs/Connection.md).

The `connection-user` and `connection-password` are typically required and
determine the user credentials for the connection, often a service user. You can
use {doc}`secrets </security/secrets>` to avoid actual values in the catalog
properties files.

### Connection security

If you have TLS configured with a globally-trusted certificate installed on your
data source, you can enable TLS between your cluster and the data
source by appending a parameter to the JDBC connection string set in the
`connection-url` catalog configuration property.

For example, enable TLS by appending the `ssl=true` parameter to the `connection-url` configuration
property:

```properties
connection-url=jdbc:databend://host:8000/?ssl=true
```

For more information on TLS configuration options, see the [Databend JDBC
driver documentation](https://github.com/databendlabs/databend-jdbc/blob/main/docs)

```{include} jdbc-authentication.fragment
```

## Querying Databend

The Databend connector provides a schema for every Databend *database*.
Run `SHOW SCHEMAS` to see the available Databend databases:

```
SHOW SCHEMAS FROM example;
```

If you have a Databend database named `web`, run `SHOW TABLES` to view the
tables in this database:

```
SHOW TABLES FROM example.web;
```

Run `DESCRIBE` or `SHOW COLUMNS` to list the columns in the `clicks` table
in the `web` databases:

```
DESCRIBE example.web.clicks;
SHOW COLUMNS FROM example.web.clicks;
```

Run `SELECT` to access the `clicks` table in the `web` database:

```
SELECT * FROM example.web.clicks;
```

:::{note}
If you used a different name for your catalog properties file, use
that catalog name instead of `example` in the above examples.
:::


(databend-type-mapping)=
## Type mapping

Because Trino and Databend each support types that the other does not, this
connector {ref}`modifies some types <type-mapping-overview>` when reading or
writing data. Data types may not map the same way in both directions between
Trino and the data source. Refer to the following sections for type mapping in
each direction.

### Databend type to Trino type mapping

The connector maps Databend types to the corresponding Trino types according
to the following table:

:::{list-table} Databend to Trino type mapping
:widths: 30, 30, 40
:header-rows: 1

* - Databend database type
  - Trino type
  - Notes
* - `BOOLEAN`
  - `BOOLEAN`
  -
* - `TINYINT`
  - `TINYINT`
  -
* - `TINYINT UNSIGNED`
  - `SMALLINT`
  -
* - `SMALLINT`
  - `SMALLINT`
  -
* - `SMALLINT UNSIGNED`
  - `INTEGER`
  -
* - `INTEGER`
  - `INTEGER`
  -
* - `INTEGER UNSIGNED`
  - `BIGINT`
  -
* - `BIGINT`
  - `BIGINT`
  -
* - `BIGINT UNSIGNED`
  - `DECIMAL(20, 0)`
  -
* - `DOUBLE PRECISION`
  - `DOUBLE`
  -
* - `FLOAT`
  - `REAL`
  -
* - `DECIMAL(p, s)`
  - `DECIMAL(p, s)`
  - 
* - `VARCHAR(n)`
  - `VARCHAR(n)`
  -
:::

No other types are supported.

### Trino type to Databend type mapping

The connector maps Trino types to the corresponding Databend types according
to the following table:

:::{list-table} Trino type to Databend type mapping
:widths: 30, 25, 50
:header-rows: 1

* - Trino type
  - Databend type
  - Notes
* - `BOOLEAN`
  - `BOOLEAN`
  -
* - `TINYINT`
  - `Int8`
  - `TINYINT`, `BOOL`, `BOOLEAN`, and `INT1` are aliases of `Int8`
* - `SMALLINT`
  - `Int16`
  -  `SMALLINT` and `INT2` are aliases of `Int16`
* - `INTEGER`
  - `Int32`
  - `INT`, `INT4`, and `INTEGER` are aliases of `Int32`
* - `BIGINT`
  - `Int64`
  - `BIGINT` is an alias of `Int64`
* - `REAL`
  - `Float32`
  - `FLOAT` is an alias of `Float32`
* - `DOUBLE`
  - `Float64`
  - `DOUBLE` is an alias of `Float64`
* - `DECIMAL(p,s)`
  - `Decimal(p,s)`
  -
* - `VARCHAR`
  - `String`
  -
* - `CHAR`
  - `String`
  -
* - `VARBINARY`
  - `String`
  - 
* - `DATE`
  - `Date`
  -
* - `TIMESTAMP(0)`
  - `DateTime`
  -
:::

No other types are supported.

```{include} jdbc-type-mapping.fragment
```

(databend-sql-support)=
## SQL support

The connector provides read and write access to data and metadata in
a Databend catalog. In addition to the {ref}`globally available
<sql-globally-available>` and {ref}`read operation <sql-read-operations>`
statements, the connector supports the following features:

- {doc}`/sql/insert`
- {doc}`/sql/truncate`
- {ref}`sql-schema-table-management`

```{include} alter-schema-limitation.fragment
```

### Procedures

```{include} jdbc-procedures-flush.fragment
```
```{include} procedures-execute.fragment
```

### Table functions

The connector provides specific {doc}`table functions </functions/table>` to
access Databend.

(databend-query-function)=
#### `query(varchar) -> table`

The `query` function allows you to query the underlying database directly. It
requires syntax native to Databend, because the full query is pushed down and
processed in Databend. This can be useful for accessing native features which
are not available in Trino or for improving query performance in situations
where running a query natively may be faster.

```{include} query-passthrough-warning.fragment
```

As a simple example, query the `example` catalog and select an entire table:

```
SELECT
  *
FROM
  TABLE(
    example.system.query(
      query => 'SELECT
        *
      FROM
        tpch.nation'
    )
  );
```

```{include} query-table-function-ordering.fragment
```

## Performance

The connector includes a number of performance improvements, detailed in the
following sections.

(databend-pushdown)=
### Pushdown

The connector supports pushdown for a number of operations:

- {ref}`limit-pushdown`

{ref}`Aggregate pushdown <aggregation-pushdown>` for the following functions:

- {func}`avg`
- {func}`count`
- {func}`max`
- {func}`min`
- {func}`sum`


```{include} pushdown-correctness-behavior.fragment
```

```{include} no-inequality-pushdown-text-type.fragment
```
