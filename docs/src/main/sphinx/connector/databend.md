# Databend connector

```{raw} html
<img src="../_static/img/databend.png" class="connector-logo">
```

The Databend connector allows querying and creating tables in an external
[Databend](https://databend.rs/) database. This can be used to join data between
different systems like Databend and Hive, or between two different
Databend instances.

## Requirements

To connect to Databend, you need:

- Databend 1.2.0 or higher
- Network access from the Trino coordinator and workers to Databend. Port 8000 is the default port.

## Configuration

To configure the Databend connector, create a catalog properties file
in `etc/catalog` named, for example, `example.properties`, to
mount the Databend connector as the `databend` catalog.
Create the file with the following contents, replacing the
connection properties as appropriate for your setup:

```none
connector.name=databend
connection-url=jdbc:databend://host:8000/
connection-user=root
connection-password=
```

The `connection-url` defines the connection information and parameters to pass
to the Databend JDBC driver. The supported parameters for the URL are
available in the [Databend JDBC driver documentation](https://github.com/databendlabs/databend-jdbc).

The `connection-user` and `connection-password` are typically required and
determine the user credentials for the connection, often a service user. You can
use {doc}`secrets </security/secrets>` to avoid actual values in the catalog
properties files.

### Multiple Databend databases

The Databend connector can query multiple databases within a Databend instance.
If you have multiple Databend instances, or want to connect to multiple
catalogs in the same instance, configure another instance of the
Databend connector as a separate catalog.

### Connection security

If you have TLS configured with a globally-trusted certificate installed on your
data source, you can enable TLS between your cluster and the data
source by appending the `ssl=true` parameter to the JDBC connection string set in the
`connection-url` catalog configuration property.

For example, with the Databend connector, enable TLS by appending the `ssl=true`
parameter to the `connection-url` configuration property:

```properties
connection-url=jdbc:databend://host:8000/?ssl=true
```

For more information on TLS configuration options, see the
[Databend JDBC driver documentation](https://github.com/databendlabs/databend-jdbc).

```{include} jdbc-authentication.fragment
```

```{include} jdbc-kerberos.fragment
```

(databend-type-mapping)=
## Type mapping

Because Trino and Databend each support types that the other does not, this
connector {ref}`modifies some types <type-mapping-overview>` when reading or
writing data. Data types may not map the same way in both directions between
Trino and the data source. Refer to the following sections for type mapping in
each direction.

### Databend type to Trino type mapping

The connector maps Databend types to the corresponding Trino types following
this table:

:::{list-table} Databend type to Trino type mapping
:widths: 30, 30, 40
:header-rows: 1

* - Databend type
  - Trino type
  - Notes
* - `BOOLEAN`
  - `BOOLEAN`
  -
* - `TINYINT`
  - `TINYINT`
  -
* - `SMALLINT`
  - `SMALLINT`
  -
* - `INTEGER`
  - `INTEGER`
  -
* - `BIGINT`
  - `BIGINT`
  -
* - `REAL`
  - `REAL`
  -
* - `DOUBLE`
  - `DOUBLE`
  -
* - `DECIMAL(p, s)`
  - `DECIMAL(p, s)`
  - See [](decimal-type-handling)
* - `VARCHAR`
  - `VARCHAR`
  -
* - `DATE`
  - `DATE`
  -
* - `TIMESTAMP`
  - `TIMESTAMP(0)`
  - Databend TIMESTAMP is mapped to TIMESTAMP(0) in Trino
:::

No other types are supported.

### Trino type to Databend type mapping

The connector maps Trino types to the corresponding Databend types following
this table:

:::{list-table} Trino type to Databend type mapping
:widths: 30, 30, 40
:header-rows: 1

* - Trino type
  - Databend type
  - Notes
* - `BOOLEAN`
  - `BOOLEAN`
  -
* - `TINYINT`
  - `TINYINT`
  -
* - `SMALLINT`
  - `SMALLINT`
  -
* - `INTEGER`
  - `INTEGER`
  -
* - `BIGINT`
  - `BIGINT`
  -
* - `REAL`
  - `REAL`
  -
* - `DOUBLE`
  - `DOUBLE`
  -
* - `DECIMAL(p, s)`
  - `DECIMAL(p, s)`
  -
* - `CHAR(n)`
  - `VARCHAR`
  -
* - `VARCHAR(n)`
  - `VARCHAR`
  -
* - `VARBINARY`
  - `VARCHAR`
  -
* - `DATE`
  - `DATE`
  -
* - `TIMESTAMP(0)`
  - `TIMESTAMP`
  -
:::

No other types are supported.

```{include} jdbc-type-mapping.fragment
```

(databend-sql-support)=
## SQL support

The connector provides read access and write access to data and metadata in
a Databend database. In addition to the {ref}`globally available
<sql-globally-available>` and {ref}`read operation <sql-read-operations>`
statements, the connector supports the following features:

- {doc}`/sql/insert`
- {doc}`/sql/truncate`
- {doc}`/sql/create-table`
- {doc}`/sql/create-table-as`
- {doc}`/sql/drop-table`
- {doc}`/sql/alter-table`
- {doc}`/sql/create-schema`
- {doc}`/sql/drop-schema`

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

The connector provides specific [table functions](/functions/table) to
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
