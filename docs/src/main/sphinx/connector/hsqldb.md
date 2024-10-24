---
myst:
  substitutions:
    default_domain_compaction_threshold: '`32`'
---

# HsqlDB connector

```{raw} html
<img src="../_static/img/hsqldb.png" class="connector-logo">
```

The HsqlDB connector allows querying and creating tables in an external HsqlDB
database (ie: HyperSQL database).

## Requirements

To connect to HsqlDB, you need:

- HsqlDB version 2.7.3.
- Network access from the Trino coordinator and workers to HsqlDB.
  Port 9001 is the default port.

## Configuration

To configure the HsqlDB connector, create a catalog properties file in
`etc/catalog` named, for example, `example.properties`, to mount the HsqlDB
connector as the `example` catalog. Create the file with the following
contents, replacing the connection properties as appropriate for your setup:

```properties
connector.name=hsqldb
connection-url=jdbc:hsqldb:hsql://<host>:<port>/<dbname>
connection-user=SA
connection-password=
```

The `connection-url` defines the connection information and parameters to pass
to the HsqlDB JDBC driver. The supported parameters for the URL are
available in the [HyperSQL User Guide](https://hsqldb.org/doc/2.0/guide/guide.html#dpc_connection_url).

The `connection-user` and `connection-password` are typically required and
determine the user credentials for the connection, often a service user. You can
use {doc}`secrets </security/secrets>` to avoid actual values in the catalog
properties files.

```{include} jdbc-authentication.fragment
```

```{include} jdbc-common-configurations.fragment
```

```{include} jdbc-domain-compaction-threshold.fragment
```

```{include} jdbc-case-insensitive-matching.fragment
```

```{include} non-transactional-insert.fragment
```

(hsqldb-fte-support)=
### Fault-tolerant execution support

The connector supports {doc}`/admin/fault-tolerant-execution` of query
processing. Read and write operations are both supported with any retry policy.

## Querying HsqlDB

The HsqlDB connector provides access to all schemas visible to the specified
user in the configured database. For the following examples, assume the HsqlDB
catalog is `example`.

You can see the available schemas by running `SHOW SCHEMAS`:

```
SHOW SCHEMAS FROM example;
```

If you have a schema named `web`, you can view the tables
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

Finally, you can query the `clicks` table in the `web` schema:

```
SELECT * FROM example.web.clicks;
```

If you used a different name for your catalog properties file, use
that catalog name instead of `example` in the above examples.

% hsqldb-type-mapping:

## Type mapping

Because Trino and HsqlDB each support types that the other does not, this
connector {ref}`modifies some types <type-mapping-overview>` when reading or
writing data. Data types may not map the same way in both directions between
Trino and the data source. Refer to the following sections for type mapping in
each direction.

### HsqlDB type to Trino type mapping

The connector maps HsqlDB types to the corresponding Trino types according
to the following table:

:::{list-table} HsqlDB type to Trino type mapping
:widths: 30, 30, 50
:header-rows: 1

* - HsqlDB type
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
* - `INT`, `INTEGER`
  - `INTEGER`
  -
* - `BIGINT`
  - `BIGINT`
  -
* - `DOUBLE`, `FLOAT`
  - `DOUBLE`
  -
* - `DECIMAL(p,s)`, `DEC(p,s)`, `NUMERIC(p,s)`
  - `DECIMAL(p,s)`
  -
* - `CHAR(n)`, `CHARACTER(n)`
  - `CHAR(n)`
  -
* - `VARCHAR(n)`, `CHARACTER VARYING(n)`, `CLOB(n)`
  - `VARCHAR(n)`
  -
* - `BLOB`
  - `VARBINARY`
  -
* - `VARBINARY(n)`
  - `VARBINARY`
  -
* - `DATE`
  - `DATE`
  -
* - `TIME(n)`
  - `TIME(n)`
  -
* - `TIME(n) WITH TIME ZONE`
  - `TIME(n) WITH TIME ZONE`
  -
:::

No other types are supported.

### Trino type mapping to HsqlDB type mapping

The connector maps Trino types to the corresponding HsqlDB types according
to the following table:

:::{list-table} Trino type mapping to HsqlDB type mapping
:widths: 30, 25, 50
:header-rows: 1

* - Trino type
  - HsqlDB type
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
  - `INT`, `INTEGER`
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
* - `CHAR(n)`
  - `CHAR(n)`
  -
* - `VARCHAR(n)`
  - `VARCHAR(n)`
  -
* - `VARBINARY(n)`
  - `VARBINARY(n)`
  -
* - `DATE`
  - `DATE`
  -
* - `TIME(n)`
  - `TIME(n)`
  -
* - `TIME(n) WITH TIME ZONE`
  - `TIME(n) WITH TIME ZONE`
  -
:::

No other types are supported.

Complete list of [HsqlDB data types](https://hsqldb.org/doc/2.0/guide/guide.html#sgc_data_type_guide).

```{include} jdbc-type-mapping.fragment
```

(hsqldb-sql-support)=
## SQL support

The connector provides read access and write access to data and metadata in
a HsqlDB database.  In addition to the {ref}`globally available
<sql-globally-available>` and {ref}`read operation <sql-read-operations>`
statements, the connector supports the following features:

- {doc}`/sql/insert`
- {doc}`/sql/update`
- {doc}`/sql/delete`
- {doc}`/sql/truncate`
- {doc}`/sql/create-table`
- {doc}`/sql/create-table-as`
- {doc}`/sql/drop-table`
- {doc}`/sql/alter-table`
- {doc}`/sql/create-schema`
- {doc}`/sql/drop-schema`

```{include} sql-update-limitation.fragment
```

```{include} sql-delete-limitation.fragment
```

### Procedures

```{include} jdbc-procedures-flush.fragment
```
```{include} procedures-execute.fragment
```

### Table functions

The connector provides specific {doc}`table functions </functions/table>` to
access HsqlDB.

(hsqldb-query-function)=
#### `query(varchar) -> table`

The `query` function allows you to query the underlying database directly. It
requires syntax native to HsqlDB, because the full query is pushed down and
processed in HsqlDB. This can be useful for accessing native features which are
not available in Trino or for improving query performance in situations where
running a query natively may be faster.

```{include} query-passthrough-warning.fragment
```

As an example, query the `example` catalog and select the age of employees by
using `TIMESTAMPDIFF` and `CURRENT_DATE`:

```
SELECT
  age
FROM
  TABLE(
    example.system.query(
      query => 'SELECT
        TIMESTAMPDIFF(
          YEAR,
          date_of_birth,
          CURRENT_DATE()
        ) AS age
      FROM
        tiny.employees'
    )
  );
```

```{include} query-table-function-ordering.fragment
```

## Performance

The connector includes a number of performance improvements, detailed in the
following sections.

(hsqldb-pushdown)=
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

```{include} pushdown-correctness-behavior.fragment
```

```{include} no-pushdown-text-type.fragment
```
