# DuckDB connector

```{raw} html
<img src="../_static/img/duckdb.png" class="connector-logo">
```

The DuckDB connector allows querying and creating tables in an external
[DuckDB](https://duckdb.org/) instance. This can be used to join data between
different systems like DuckDB and Hive, or between two different
DuckDB instances.

## Configuration

To configure the DuckDB connector, create a catalog properties file
in `etc/catalog` named, for example, `example.properties`, to
mount the DuckDB connector as the `duckdb` catalog.
Create the file with the following contents, replacing the
connection properties as appropriate for your setup:

```none
connector.name=duckdb
connection-url=jdbc:duckdb://<path>
connection-user=root
connection-password=secret
```

### Multiple DuckDB servers

The DuckDB connector can only access a single database within
a DuckDB instance. Thus, if you have multiple DuckDB servers,
or want to connect to multiple DuckDB servers, you must configure
multiple instances of the DuckDB connector.

(duckdb-type-mapping)=
## Type mapping

Because Trino and DuckDB each support types that the other does not, this
connector {ref}`modifies some types <type-mapping-overview>` when reading or
writing data. Data types may not map the same way in both directions between
Trino and the data source. Refer to the following sections for type mapping in
each direction.

List of [DuckDB data types](https://duckdb.org/docs/sql/data_types/overview.html).

### DuckDB type to Trino type mapping

The connector maps DuckDB types to the corresponding Trino types following
this table:

:::{list-table} DuckDB type to Trino type mapping
:widths: 30, 30, 40
:header-rows: 1

* - DuckDB type
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
* - `FLOAT`
  - `REAL`
  - 
* - `DOUBLE`
  - `DOUBLE`
  - 
* - `DECIMAL`
  - `DECIMAL`
  - Default precision and scale are (18,3).
* - `VARCHAR`
  - `VARCHAR`
  -
* - `DATE`
  - `DATE`
  -
:::

No other types are supported.

### Trino type to DuckDB type mapping

The connector maps Trino types to the corresponding DuckDB types following
this table:

:::{list-table} Trino type to DuckDB type mapping
:widths: 30, 30, 40
:header-rows: 1

* - Trino type
  - DuckDB type
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
* - `DECIMAL`
  - `DECIMAL`
  -
* - `CHAR`
  - `VARCHAR`
  -
* - `VARCHAR`
  - `VARCHAR`
  -
* - `DATE`
  - `DATE`
  -
:::

No other types are supported.

```{include} jdbc-type-mapping.fragment
```

(duckdb-sql-support)=
## SQL support

The connector provides read access and write access to data and metadata in
a DuckDB database.  In addition to the {ref}`globally available
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
access DuckDB.

(duckdb-query-function)=
#### `query(varchar) -> table`

The `query` function allows you to query the underlying database directly. It
requires syntax native to DuckDB, because the full query is pushed down and
processed in DuckDB. This can be useful for accessing native features which
are not available in Trino or for improving query performance in situations
where running a query natively may be faster.

Find details about the SQL support of DuckDB that you can use in the query in
the [DuckDB SQL Command
Reference](https://duckdb.org/docs/sql/query_syntax/select) and
other statements and functions.

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
