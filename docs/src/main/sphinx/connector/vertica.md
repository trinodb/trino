---
myst:
  substitutions:
    default_domain_compaction_threshold: '`32`'
---

# Vertica connector

```{raw} html
<img src="../_static/img/vertica.png" class="connector-logo">
```

The Vertica connector enables you to use an external
[Vertica](https://www.vertica.com/) database as a data source in Trino.

## Requirements

To connect to Vertica, you need:

- Vertica version 9.1 or higher.
- Network access from the Trino coordinator and workers to Vertica. Port 5433 is
  the default port.

## Configuration

Create a catalog properties file that specifies the Vertica connector by setting
the `connector.name` to `vertica`.

For example, to access Vertica as the `example` catalog, create the file
`etc/catalog/example.properties`. Replace the connection properties as
appropriate for your setup:

```text
connector.name=vertica
connection-url=jdbc:vertica://example.net:5433
connection-user=example_user
connection-password=secret
```

The `connection-url` defines the connection information and parameters to pass
to the Vertica JDBC driver.

The `connection-user` and `connection-password` are typically required and
determine the user credentials for the connection, often a service user. You can
use [secrets](/security/secrets) to avoid actual values in the catalog
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

## Querying Vertica

The Vertica connector provides a schema for every Vertica *database*.
You can see the available Vertica databases by running `SHOW SCHEMAS`:

```sql
SHOW SCHEMAS FROM example;
```

If you have a Vertica database named `web`, you can view the tables
in this database by running `SHOW TABLES`:

```sql
SHOW TABLES FROM example.web;
```

You can see a list of the columns in the `clicks` table in the `web`
database using either of the following:

```sql
DESCRIBE example.web.clicks;
SHOW COLUMNS FROM example.web.clicks;
```

Finally, you can access the `clicks` table in the `web` database:

```sql
SELECT * FROM example.web.clicks;
```

If you used a different name for your catalog properties file, use
that catalog name instead of `example` in the above examples.

(postgresql-type-mapping)=

## Type mapping

Because Trino and Vertica each support types that the other does not, the
connector [modifies some types](type-mapping-overview) when reading or
writing data. Data types may not map the same way in both directions between
Trino and the data source. Refer to the following sections for type mapping in
each direction.

### Vertica type to Trino type mapping

The connector maps Vertica types to the corresponding Trino types following
this table:

:::{list-table} Vertica type to Trino type mapping
:widths: 30, 30, 40
:header-rows: 1

* - Vertica type
  - Trino type
  - Notes
:::

No other types are supported.

### Trino type to Vertica type mapping

The connector maps Trino types to the corresponding Vertica types following
this table:

:::{list-table} Trino type to Vertica type mapping
:widths: 30, 30, 40
:header-rows: 1

* - Trino type
  - Vertica type
  - Notes

::::

No other types are supported.

```{include} jdbc-type-mapping.fragment
```

(vertica-sql-support)=
## SQL support

The connector provides read access and write access to data and metadata in a
Vertica database.  In addition to the [globally
available](sql-globally-available) and [read operation](sql-read-operations)
statements, the connector supports the following features:

- [](/sql/insert)
- [](/sql/update)
- [](/sql/delete)
- [](/sql/create-table)
- [](/sql/create-table-as)
- [](/sql/drop-table)
- [](/sql/alter-table)
- [](/sql/create-schema)
- [](/sql/drop-schema)

```{include} sql-update-limitation.fragment
```

```{include} sql-delete-limitation.fragment
```

## Table functions

The connector provides specific [table functions](/functions/table) to access
Vertica.

(vertica-query-function)=
### `query(varchar) -> table`

The `query` function allows you to query the underlying database directly. It
requires syntax native to Vertica, because the full query is pushed down and
processed in Vertica. This can be useful for accessing native features which are
not available in Trino or for improving query performance in situations where
running a query natively may be faster.

```{include} query-passthrough-warning.fragment
```

As an example, query the `example` catalog and select the age of employees by
using `TIMESTAMPDIFF` and `CURDATE`:

```sql
SELECT
  age
FROM
  TABLE(
    example.system.query(
      query => 'SELECT
        TIMESTAMPDIFF(
          YEAR,
          date_of_birth,
          CURDATE()
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

### Pushdown

The connector supports pushdown for a number of operations:

- [](join-pushdown)
- [](limit-pushdown)
- [](topn-pushdown)

```{include} pushdown-correctness-behavior.fragment
```

```{include} no-pushdown-text-type.fragment
```
