---
myst:
  substitutions:
    default_domain_compaction_threshold: '`256`'
---

# SQLite connector

```{raw} html
<img src="../_static/img/sqlite.png" class="connector-logo">
```

The SQLite connector allows querying tables in an SQLite database.

## Requirements

To connect to SQLite, you need:

- Local drive access from the Trino workers (ie: connector) to the SQLite database file.

## Configuration

To configure the SQLite connector, create a catalog properties file in
`etc/catalog` named, for example, `example.properties`, to mount the SQLite
connector as the `example` catalog. Create the file with the following
contents, replacing the connection properties as appropriate for your setup:

```text
connector.name=sqlite
connection-url=jdbc:sqlite:file:///tmp/sqlite.db
connection-user=SA
connection-password=
```
The `connection-url` property must point to the SQLite database file via an absolute path, preferably local.

The `connection-user` and `connection-password` are typically required by Trino but not used by SQLite.

```{include} jdbc-common-configurations.fragment
```

```{include} jdbc-domain-compaction-threshold.fragment
```

```{include} jdbc-case-insensitive-matching.fragment
```

## Querying SQLite

The SQLite connector provides a catalog for every SQLite *database* (ie: every catalog properties file).
By default, this catalog has a schema named `main`, which is unique and cannot be modified because
this connector does not support the creation, deletion or renaming of schemas.
In fact, SQLite does not manage schemas but allows a `main` schema to be used as the default schema.

You can see the available SQLite databases by running `SHOW CATALOGS`:

```
SHOW CATALOGS;
```

You can view the tables in the `main` schema by running `SHOW TABLES`:

```
SHOW TABLES FROM example.main;
```

You can see a list of the columns in the `clicks` table in the `main`
schema using either of the following:

```
DESCRIBE example.main.clicks;
SHOW COLUMNS FROM example.main.clicks;
```

Finally, you can access the `clicks` table in the `main` schema of the `example` database:

```
SELECT * FROM example.main.clicks;
```

If you used a different name for your catalog properties file, use
that catalog name instead of `example` in the above examples.

% SQLite-type-mapping:

## Type mapping

Because Trino and SQLite each support types that the other does not, this
connector {ref}`modifies some types <type-mapping-overview>` when reading or
writing data. Data types may not map the same way in both directions between
Trino and the data source. Refer to the following sections for type mapping in
each direction.

SQLite inherently does not impose a SQL type on a column and will use the given data type to
perform its storage. At the storage level, SQLite uses 5 storage classes:
`NULL`, `INTEGER`, `REAL`, `TEXT`, `BLOB`.

But in order to allow the implicit conversions offered by SQL, to each column is assigned
one of the following type affinities: `NUMERIC`, `INTEGER`, `REAL`, `TEXT`, `BLOB`.
These 5 type affinities are converted by the SQLite JDBC driver as Java SQL types:
`NUMERIC`, `INTEGER`, `REAL`, `VARCHAR`, `BLOB`, respectively.

However, SQLite offers the option, when declaring a table, to assign, or not, a named type to each column.
Normally SQLite determines the type affinity based on this named type. But to allow the connector to
recognize more JDBC types, it's the connector that will attempt to determine the SQL type used.
This allows the connector to recognize the following Java SQL types:
`BOOLEAN`, `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT`,
`DOUBLE`, `REAL`, `FLOAT`,
`DECIMAL`, `NUMERIC`,
`CHAR`, `VARCHAR`, `CLOB`,
`BLOB`, `VARBINARY`,
`DATE`.

To offer better compatibility with SQLite, it is possible to declare named type aliases.
By default, the aliases `TEXT` and `INT` will point to the connector SQL types `VARCHAR` and `BIGINT`, respectively.
It is possible to add custom aliases to the connector using the `sqlite.custom-data-types` property entry in the `catalog.properties` file.
If you use this property, you must include the two default aliases if you want them to be retained. Specifically:
`sqlite.custom-data-types=TEXT=VARCHAR,INT=BIGINT,...`

If no named type is used in a column's declaration when creating a table, then the connector will consider
that column to be `BLOB`.

If the named type given to a column does not match any of the SQL types recognized by the connector, nor
the default aliases or aliases defined in the `catalog.properties` file, then there are two possibilities:
- If the `sqlite.use-type-affinity=true` property is set in the `catalog.properties` file, then the connector will use the type affinity determined by SQLite.
- Else, and by default, the connector will not support this column.

For now, since the connector is read-only, only the mapping of SQLite named type to Trino
type needs to be taken into account.

### SQLite named type to Trino type mapping

The connector maps SQLite named types to the corresponding Trino types according
to the following table:

:::{list-table} SQLite named type to Trino type mapping
:widths: 30, 30, 40
:header-rows: 1

* - SQLite named type
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
* - `BIGINT` or `INT`
  - `BIGINT`
  -
* - `REAL` or `FLOAT` or `DOUBLE`
  - `DOUBLE`
  -
* - `DECIMAL(p,s)` or `NUMERIC(p,s)`
  - `DECIMAL(p,s)`
  -
* - `CHAR(n)` or `VARCHAR(n)` or `CLOB(n)` or `TEXT`
  - `VARCHAR(n)`
  -
* - `VARBINARY` or `BLOB(n)`
  - `VARBINARY`
  -
* - `DATE`
  - `DATE`
  -
:::

No other types are supported.

### Trino type mapping to SQLite named type mapping

The connector maps Trino types to the corresponding SQLite named types according
to the following table:

:::{list-table} Trino type mapping to SQLite named type mapping
:widths: 30, 30, 40
:header-rows: 1

* - Trino type
  - SQLite named type
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
* - `DOUBLE`
  - `DOUBLE`
  -
* - `DECIMAL(p,s)`
  - `DECIMAL(p,s)`
  -
* - `CHAR(n)` or `VARCHAR(n)` or `CLOB(n)`
  - `TEXT`
  -
* - `VARBINARY`
  - `CLOB`
  -
* - `DATE`
  - `DATE`
  -
:::

No other types are supported.

Complete list of [SQLite data types affinity](https://www.sqlite.org/datatype3.html#affinity_name_examples).

```{include} jdbc-type-mapping.fragment
```

(SQLite-sql-support)=
## SQL support

The connector provides for now only read access to data and metadata in a
SQLite database. In addition to the [globally
available](sql-globally-available) and [read operation](sql-read-operations)
statements, the connector supports the following features:

- [](/sql/insert)
- [](/sql/update)
- [](/sql/delete)
- [](/sql/truncate)
- [](/sql/create-table)
- [](/sql/create-table-as)
- [](/sql/drop-table)
- [](/sql/alter-table)
- [](/sql/create-schema)
- [](/sql/drop-schema)
- [](SQLite-procedures)
- [](sqlite-table-functions)

(sqlite-procedures)=
### Procedures

```{include} jdbc-procedures-flush.fragment
```
```{include} procedures-execute.fragment
```

(sqlite-table-functions)=
### Table functions

The connector provides specific {doc}`table functions </functions/table>` to
access SQLite.

(sqlite-query-function)=
#### `query(varchar) -> table`

The `query` function allows you to query the underlying database directly. It
requires syntax native to SQLite, because the full query is pushed down and
processed in SQLite. This can be useful for accessing native features which are
not available in Trino or for improving query performance in situations where
running a query natively may be faster.

```{include} query-passthrough-warning.fragment
```

As an example, query the `example` catalog and select the age of employees in `main` schema by
using `TIMEDIFF` and `DATE`:

```
SELECT
  age
FROM
  TABLE(
    example.system.query(
      query => 'SELECT
        TIMEDIFF(
          DATE(),
          date_of_birth
        ) AS age
      FROM
        example.main.employees'
    )
  );
```

```{include} query-table-function-ordering.fragment
```
