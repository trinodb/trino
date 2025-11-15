---
myst:
  substitutions:
    default_domain_compaction_threshold: '`256`'
---

# HsqlDB connector

```{raw} html
<img src="../_static/img/hsqldb.png" class="connector-logo">
```

The HsqlDB connector allows querying and creating tables in an external HsqlDB
database.

## Requirements

To connect to HsqlDB, you need:

- HsqlDB version 2.7.4 or higher.
- Network access from the Trino coordinator and workers to HsqlDB. Port
  9001 is the default port.

## Configuration

To configure the HsqlDB connector, create a catalog properties file in
`etc/catalog` named, for example, `example.properties`, to mount the HsqlDB
connector as the `example` catalog. Create the file with the following
contents, replacing the connection properties as appropriate for your setup:

```text
connector.name=hsqldb
connection-url=jdbc:hsqldb:hsql://localhost:9001/
connection-user=SA
connection-password=
```

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

## Querying HsqlDB

The HsqlDB connector provides a catalog for every HsqlDB *database* (ie: every catalog properties file).
You can see the available HsqlDB databases by running `SHOW CATALOGS`:

```
SHOW CATALOGS;
```

If you have a HsqlDB schema named `web`, you can view the tables
in this schema by running `SHOW TABLES`:

```
SHOW TABLES FROM example.web;
```

You can see a list of the columns in the `clicks` table in the `web`
schema using either of the following:

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
* - `INTEGER` or `INT`
  - `INTEGER`
  -
* - `BIGINT`
  - `BIGINT`
  -
* - `DOUBLE` or `FLOAT`
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
* - `CLOB(n)`
  - `VARCHAR(n)`
  -
* - `BINARY(n)`
  - `VARBINARY`
  -
* - `VARBINARY(n)`
  - `VARBINARY`
  -
* - `BLOB(n)`
  - `VARBINARY`
  -
* - `UUID`
  - `UUID`
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
* - `TIMESTAMP(n)`
  - `TIMESTAMP(n)`
  -
* - `TIMESTAMP(n) WITH TIME ZONE`
  - `TIMESTAMP(n) WITH TIME ZONE`
  - 
* - `INTERVAL`
  - `INTERVAL`
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
* - `CHAR(n)`
  - `CHAR(n)`
  -
* - `VARCHAR(n)`
  - `VARCHAR(n)`
  -
* - `VARBINARY`
  - `VARBINARY(32768)`
  -
* - `UUID`
  - `UUID`
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
* - `TIMESTAMP(n)`
  - `TIMESTAMP(n)`
  - 
* - `TIMESTAMP(n) WITH TIME ZONE`
  - `TIMESTAMP(n) WITH TIME ZONE`
  -
* - `INTERVAL`
  - `INTERVAL`
  -
:::

No other types are supported.

Complete list of [HsqlDB data types](https://hsqldb.org/doc/2.0/guide/guide.html#sgc_data_type_guide).

```{include} jdbc-type-mapping.fragment
```

(hsqldb-sql-support)=
## SQL support

The connector provides read access and write access to data and metadata in a
HsqlDB database. In addition to the [globally
available](sql-globally-available) and [read operation](sql-read-operations)
statements, the connector supports the following features:

- [](/sql/insert), see also [](hsqldb-insert)
- [](/sql/update), see also [](hsqldb-update)
- [](/sql/delete), see also [](hsqldb-delete)
- [](/sql/truncate)
- [](/sql/create-table)
- [](/sql/create-table-as)
- [](/sql/drop-table)
- [](/sql/alter-table)
- [](/sql/create-schema)
- [](/sql/drop-schema)
- [](hsqldb-procedures)
- [](hsqldb-table-functions)

(hsqldb-insert)=
```{include} non-transactional-insert.fragment
```

(hsqldb-update)=
```{include} sql-update-limitation.fragment
```

(hsqldb-delete)=
```{include} sql-delete-limitation.fragment
```

(hsqldb-procedures)=
### Procedures

```{include} jdbc-procedures-flush.fragment
```
```{include} procedures-execute.fragment
```

(hsqldb-table-functions)=
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

As an example, query the `example` catalog and select the age of employees in `public` schema by
using `TIMESTAMPDIFF` and `CURRENT_DATE`:

```
SELECT
  age
FROM
  TABLE(
    example.system.query(
      query => 'SELECT
        TIMESTAMPDIFF(
          SQL_TSI_YEAR,
          date_of_birth,
          CURRENT_DATE
        ) AS age
      FROM
        example.public.employees'
    )
  );
```

```{include} query-table-function-ordering.fragment
```
