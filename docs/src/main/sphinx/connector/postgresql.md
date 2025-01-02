---
myst:
  substitutions:
    default_domain_compaction_threshold: '`256`'
---

# PostgreSQL connector

```{raw} html
<img src="../_static/img/postgresql.png" class="connector-logo">
```

The PostgreSQL connector allows querying and creating tables in an
external [PostgreSQL](https://www.postgresql.org/) database. This can be used to join data between
different systems like PostgreSQL and Hive, or between different
PostgreSQL instances.

## Requirements

To connect to PostgreSQL, you need:

- PostgreSQL 11.x or higher.
- Network access from the Trino coordinator and workers to PostgreSQL.
  Port 5432 is the default port.

## Configuration

The connector can query a database on a PostgreSQL server. Create a catalog
properties file that specifies the PostgreSQL connector by setting the
`connector.name` to `postgresql`.

For example, to access a database as the `example` catalog, create the file
`etc/catalog/example.properties`. Replace the connection properties as
appropriate for your setup:

```text
connector.name=postgresql
connection-url=jdbc:postgresql://example.net:5432/database
connection-user=root
connection-password=secret
```

The `connection-url` defines the connection information and parameters to pass
to the PostgreSQL JDBC driver. The parameters for the URL are available in the
[PostgreSQL JDBC driver documentation](https://jdbc.postgresql.org/documentation/use/#connecting-to-the-database).
Some parameters can have adverse effects on the connector behavior or not work
with the connector.

The `connection-user` and `connection-password` are typically required and
determine the user credentials for the connection, often a service user. You can
use {doc}`secrets </security/secrets>` to avoid actual values in the catalog
properties files.

### Access to system tables

The PostgreSQL connector supports reading [PostgreSQL catalog
tables](https://www.postgresql.org/docs/current/catalogs.html), such as
`pg_namespace`. The functionality is turned off by default, and can be enabled
using the `postgresql.include-system-tables` configuration property.

You can see more details in the `pg_catalog` schema in the `example` catalog,
for example about the `pg_namespace` system table:

```sql
SHOW TABLES FROM example.pg_catalog;
SELECT * FROM example.pg_catalog.pg_namespace;
```

(postgresql-tls)=
### Connection security

If you have TLS configured with a globally-trusted certificate installed on your
data source, you can enable TLS between your cluster and the data
source by appending a parameter to the JDBC connection string set in the
`connection-url` catalog configuration property.

For example, with version 42 of the PostgreSQL JDBC driver, enable TLS by
appending the `ssl=true` parameter to the `connection-url` configuration
property:

```properties
connection-url=jdbc:postgresql://example.net:5432/database?ssl=true
```

For more information on TLS configuration options, see the [PostgreSQL JDBC
driver documentation](https://jdbc.postgresql.org/documentation/use/#connecting-to-the-database).

```{include} jdbc-authentication.fragment
```

### Multiple PostgreSQL databases or servers

The PostgreSQL connector can only access a single database within
a PostgreSQL server. Thus, if you have multiple PostgreSQL databases,
or want to connect to multiple PostgreSQL servers, you must configure
multiple instances of the PostgreSQL connector.

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

(postgresql-fte-support)=
### Fault-tolerant execution support

The connector supports {doc}`/admin/fault-tolerant-execution` of query
processing. Read and write operations are both supported with any retry policy.

(postgresql-type-mapping)=
## Type mapping

Because Trino and PostgreSQL each support types that the other does not, this
connector {ref}`modifies some types <type-mapping-overview>` when reading or
writing data. Data types may not map the same way in both directions between
Trino and the data source. Refer to the following sections for type mapping in
each direction.

### PostgreSQL type to Trino type mapping

The connector maps PostgreSQL types to the corresponding Trino types following
this table:


:::{list-table} PostgreSQL type to Trino type mapping
:widths: 30, 30, 40
:header-rows: 1

* - PostgreSQL type
  - Trino type
  - Notes
* - `BIT`
  - `BOOLEAN`
  -
* - `BOOLEAN`
  - `BOOLEAN`
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
* - `NUMERIC(p, s)`
  - `DECIMAL(p, s)`
  - `DECIMAL(p, s)` is an alias of `NUMERIC(p, s)`. See
    [](postgresql-decimal-type-handling) for more information.
* - `CHAR(n)`
  - `CHAR(n)`
  -
* - `VARCHAR(n)`
  - `VARCHAR(n)`
  -
* - `ENUM`
  - `VARCHAR`
  -
* - `BYTEA`
  - `VARBINARY`
  -
* - `DATE`
  - `DATE`
  -
* - `TIME(n)`
  - `TIME(n)`
  -
* - `TIMESTAMP(n)`
  - `TIMESTAMP(n)`
  -
* - `TIMESTAMPTZ(n)`
  - `TIMESTAMP(n) WITH TIME ZONE`
  -
* - `MONEY`
  - `VARCHAR`
  -
* - `UUID`
  - `UUID`
  -
* - `JSON`
  - `JSON`
  -
* - `JSONB`
  - `JSON`
  -
* - `VECTOR`
  - `ARRAY(REAL)`
  -
* - `HSTORE`
  - `MAP(VARCHAR, VARCHAR)`
  -
* - `ARRAY`
  - Disabled, `ARRAY`, or `JSON`
  - See [](postgresql-array-type-handling) for more information.
* - `GEOMETRY`, `GEOMETRY(GEOMETRY TYPE, SRID)`
  - `GEOMETRY`
  -
:::

No other types are supported.

### Trino type to PostgreSQL type mapping

The connector maps Trino types to the corresponding PostgreSQL types following
this table:

:::{list-table} Trino type to PostgreSQL type mapping
:widths: 30, 30, 40
:header-rows: 1

* - Trino type
  - PostgreSQL type
  - Notes
* - `BOOLEAN`
  - `BOOLEAN`
  -
* - `SMALLINT`
  - `SMALLINT`
  -
* - `TINYINT`
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
* - `DECIMAL(p, s)`
  - `NUMERIC(p, s)`
  - `DECIMAL(p, s)` is an alias of  `NUMERIC(p, s)`. See
    [](postgresql-decimal-type-handling) for more information.
* - `CHAR(n)`
  - `CHAR(n)`
  -
* - `VARCHAR(n)`
  - `VARCHAR(n)`
  -
* - `VARBINARY`
  - `BYTEA`
  -
* - `DATE`
  - `DATE`
  -
* - `TIME(n)`
  - `TIME(n)`
  -
* - `TIMESTAMP(n)`
  - `TIMESTAMP(n)`
  -
* - `TIMESTAMP(n) WITH TIME ZONE`
  - `TIMESTAMPTZ(n)`
  -
* - `UUID`
  - `UUID`
  -
* - `JSON`
  - `JSONB`
  -
* - `ARRAY`
  - `ARRAY`
  - See [](postgresql-array-type-handling) for more information.
* - `GEOMETRY`
  - `GEOMETRY`
  -
::::

No other types are supported.

(postgresql-decimal-type-handling)=
```{include} decimal-type-handling.fragment
```

(postgresql-array-type-handling)=
### Array type handling

The PostgreSQL array implementation does not support fixed dimensions whereas Trino
support only arrays with fixed dimensions.
You can configure how the PostgreSQL connector handles arrays with the `postgresql.array-mapping` configuration property in your catalog file
or the `array_mapping` session property.
The following values are accepted for this property:

- `DISABLED` (default): array columns are skipped.
- `AS_ARRAY`: array columns are interpreted as Trino `ARRAY` type, for array columns with fixed dimensions.
- `AS_JSON`: array columns are interpreted as Trino `JSON` type, with no constraint on dimensions.

```{include} jdbc-type-mapping.fragment
```

## Querying PostgreSQL

The PostgreSQL connector provides a schema for every PostgreSQL schema.
You can see the available PostgreSQL schemas by running `SHOW SCHEMAS`:

```
SHOW SCHEMAS FROM example;
```

If you have a PostgreSQL schema named `web`, you can view the tables
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

If you used a different name for your catalog properties file, use
that catalog name instead of `example` in the above examples.

(postgresql-sql-support)=
## SQL support

The connector provides read access and write access to data and metadata in
PostgreSQL. In addition to the [globally available](sql-globally-available) and
[read operation](sql-read-operations) statements, the connector supports the
following features:

- [](/sql/insert), see also [](postgresql-insert)
- [](/sql/update), see also [](postgresql-update)
- [](/sql/delete), see also [](postgresql-delete)
- [](/sql/merge), see also [](postgresql-merge)
- [](/sql/truncate)
- [](sql-schema-table-management), see also:
  - [](postgresql-alter-table)
  - [](postgresql-alter-schema)
- [](postgresql-procedures)
- [](postgresql-table-functions)

(postgresql-insert)=
```{include} non-transactional-insert.fragment
```

(postgresql-update)=
```{include} sql-update-limitation.fragment
```

(postgresql-delete)=
```{include} sql-delete-limitation.fragment
```

(postgresql-merge)=
```{include} non-transactional-merge.fragment
```

(postgresql-alter-table)=
```{include} alter-table-limitation.fragment
```

(postgresql-alter-schema)=
```{include} alter-schema-limitation.fragment
```

(postgresql-procedures)=
### Procedures

```{include} jdbc-procedures-flush.fragment
```
```{include} procedures-execute.fragment
```

(postgresql-table-functions)=
### Table functions

The connector provides specific {doc}`table functions </functions/table>` to
access PostgreSQL.

(postgresql-query-function)=
#### `query(varchar) -> table`

The `query` function allows you to query the underlying database directly. It
requires syntax native to PostgreSQL, because the full query is pushed down and
processed in PostgreSQL. This can be useful for accessing native features which
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

As a practical example, you can leverage
[frame exclusion from PostgresQL](https://www.postgresql.org/docs/current/sql-expressions.html#SYNTAX-WINDOW-FUNCTIONS)
when using window functions:

```
SELECT
  *
FROM
  TABLE(
    example.system.query(
      query => 'SELECT
        *,
        array_agg(week) OVER (
          ORDER BY
            week
          ROWS
            BETWEEN 2 PRECEDING
            AND 2 FOLLOWING
            EXCLUDE GROUP
        ) AS week,
        array_agg(week) OVER (
          ORDER BY
            day
          ROWS
            BETWEEN 2 PRECEDING
            AND 2 FOLLOWING
            EXCLUDE GROUP
        ) AS all
      FROM
        test.time_data'
    )
  );
```

```{include} query-table-function-ordering.fragment
```

## Performance

The connector includes a number of performance improvements, detailed in the
following sections.

(postgresql-table-statistics)=
### Table statistics

The PostgreSQL connector can use {doc}`table and column statistics
</optimizer/statistics>` for {doc}`cost based optimizations
</optimizer/cost-based-optimizations>`, to improve query processing performance
based on the actual data in the data source.

The statistics are collected by PostgreSQL and retrieved by the connector.

To collect statistics for a table, execute the following statement in
PostgreSQL.

```text
ANALYZE table_schema.table_name;
```

Refer to PostgreSQL documentation for additional `ANALYZE` options.

(postgresql-pushdown)=
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

```{include} pushdown-correctness-behavior.fragment
```

```{include} join-pushdown-enabled-true.fragment
```

### Predicate pushdown support

Predicates are pushed down for most types, including `UUID` and temporal
types, such as `DATE`.

The connector does not support pushdown of range predicates, such as `>`,
`<`, or `BETWEEN`, on columns with {ref}`character string types
<string-data-types>` like `CHAR` or `VARCHAR`.  Equality predicates, such as
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

There is experimental support to enable pushdown of range predicates on columns
with character string types which can be enabled by setting the
`postgresql.experimental.enable-string-pushdown-with-collate` catalog
configuration property or the corresponding
`enable_string_pushdown_with_collate` session property to `true`.
Enabling this configuration will make the predicate of all the queries in the
above example get pushed down.
