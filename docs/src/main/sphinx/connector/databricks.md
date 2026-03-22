---
myst:
  substitutions:
    default_domain_compaction_threshold: "`256`"
---
# Databricks connector

The Databricks connector allows querying and creating tables in an external
[Databricks](https://www.databricks.com/) workspace. This can be used to join
data between different systems like Databricks and Hive, or between two
different Databricks workspaces.

## Configuration

The connector can query a Databricks SQL warehouse. Create a catalog properties
file that specifies the Databricks connector by setting the `connector.name` to
`databricks`.

For example, to access a Databricks workspace as the `example` catalog, create
the file `etc/catalog/example.properties`. Replace the connection properties as
appropriate for your setup:

```none
connector.name=databricks
connection-url=jdbc:databricks://<hostname>:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=<http-path>;
connection-user=token
connection-password=<access-token>
```

The `connection-url` defines the connection information and parameters to pass
to the Databricks JDBC driver. The supported URL format is documented in the
[Databricks JDBC driver documentation](https://docs.databricks.com/en/integrations/jdbc/index.html).

Replace `<hostname>` with your Databricks workspace hostname (e.g.
`dbc-xxxxxxxx-xxxx.cloud.databricks.com`), `<http-path>` with the HTTP path of
your SQL warehouse (e.g. `/sql/1.0/warehouses/xxxxxxxxxxxx`), and
`<access-token>` with your Databricks personal access token.

### Connector properties

The following configuration properties are available:

:::{list-table}
:widths: 40, 40, 20
:header-rows: 1

* - Property name
  - Description
  - Default
* - `databricks.http-path`
  - The HTTP path of the Databricks SQL warehouse.
  - (none)
* - `databricks.catalog`
  - The Databricks Unity Catalog catalog to use.
  - (none)
* - `databricks.schema`
  - The Databricks schema (database) to use.
  - (none)
:::

### Multiple Databricks catalogs

The Databricks connector can access a single Unity Catalog catalog within a
Databricks workspace. If you need to access multiple catalogs, configure
multiple instances of the Databricks connector with different
`databricks.catalog` values.

```{include} jdbc-common-configurations.fragment
```

```{include} query-comment-format.fragment
```

```{include} jdbc-domain-compaction-threshold.fragment
```

(databricks-type-mapping)=
## Type mapping

Because Trino and Databricks each support types that the other does not, this
connector {ref}`modifies some types <type-mapping-overview>` when reading or
writing data. Data types may not map the same way in both directions between
Trino and the data source. Refer to the following sections for type mapping in
each direction.

List of [Databricks SQL data types](https://docs.databricks.com/en/sql/language-manual/sql-ref-datatypes.html).

### Databricks type to Trino type mapping

The connector maps Databricks types to the corresponding Trino types following
this table:

:::{list-table} Databricks type to Trino type mapping
:widths: 30, 30, 40
:header-rows: 1

* - Databricks type
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
* - `INT`
  - `INTEGER`
  -
* - `BIGINT`
  - `BIGINT`
  -
* - `FLOAT`
  - `REAL`
  -
* - `DOUBLE`
  - `DOUBLE`
  -
* - `DECIMAL`
  - `DECIMAL`
  - Default precision and scale are (10,0).
* - `STRING`
  - `VARCHAR`
  -
* - `VARCHAR`
  - `VARCHAR`
  -
* - `DATE`
  - `DATE`
  -
* - `TIMESTAMP`
  - `TIMESTAMP(3)`
  -
* - `TIMESTAMP_NTZ`
  - `TIMESTAMP(3)`
  -
:::

No other types are supported.

### Trino type to Databricks type mapping

The connector maps Trino types to the corresponding Databricks types following
this table:

:::{list-table} Trino type to Databricks type mapping
:widths: 30, 30, 40
:header-rows: 1

* - Trino type
  - Databricks type
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
  - `FLOAT`
  -
* - `DOUBLE`
  - `DOUBLE`
  -
* - `DECIMAL`
  - `DECIMAL`
  -
* - `CHAR`
  - `STRING`
  -
* - `VARCHAR`
  - `STRING`
  -
* - `DATE`
  - `DATE`
  -
* - `TIMESTAMP`
  - `TIMESTAMP`
  -
:::

No other types are supported.

```{include} jdbc-type-mapping.fragment
```

(databricks-sql-support)=
## SQL support

The connector provides read access and write access to data and metadata in a
Databricks SQL warehouse. In addition to the {ref}`globally available
<sql-globally-available>` and {ref}`read operation <sql-read-operations>`
statements, the connector supports the following features:

- {doc}`/sql/insert`
- {doc}`/sql/delete`
- {doc}`/sql/truncate`
- {doc}`/sql/create-table`
- {doc}`/sql/create-table-as`
- {doc}`/sql/drop-table`
- {doc}`/sql/alter-table`
- {doc}`/sql/create-schema`
- {doc}`/sql/drop-schema`

### Procedures

```{include} jdbc-procedures-flush.fragment
```
```{include} procedures-execute.fragment
```

### Table functions

The connector provides specific [table functions](/functions/table) to
access Databricks.

(databricks-query-function)=
#### `query(varchar) -> table`

The `query` function allows you to query the underlying database directly. It
requires syntax native to Databricks SQL, because the full query is pushed down
and processed in Databricks. This can be useful for accessing native features
which are not available in Trino or for improving query performance in
situations where running a query natively may be faster.

Find details about the SQL support of Databricks that you can use in the query
in the [Databricks SQL language
reference](https://docs.databricks.com/en/sql/language-manual/index.html).

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

(databricks-pushdown)=
### Pushdown

The connector supports pushdown for a number of operations:

- [](limit-pushdown)
- [](topn-pushdown)

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
