---
myst:
  substitutions:
    default_domain_compaction_threshold: '`32`'
---

# Vertica connector

```{raw} html
<img src="../_static/img/vertica.png" class="connector-logo">
```

The Vertica connector allows querying and creating tables in an external Vertica
database.

## Requirements

To connect to Vertica, you need:

- Vertica version 9.1 or higher.
- Network access from the Trino coordinator and workers to Vertica. Port
  5433 is the default port.

## Configuration

To configure the Vertica connector, create a catalog properties file in
`etc/catalog` named, for example, `example.properties`, to mount the Vertica
connector as the `example` catalog. Create the file with the following
contents, replacing the connection properties as appropriate for your setup:

```text
connector.name=vertica
connection-url=jdbc:vertica://example.net:5433
connection-user=root
connection-password=secret
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

```{include} non-transactional-insert.fragment
```

## Querying Vertica

The Vertica connector provides a schema for every Vertica *database*.
You can see the available Vertica databases by running `SHOW SCHEMAS`:

```
SHOW SCHEMAS FROM example;
```

If you have a Vertica database named `web`, you can view the tables
in this database by running `SHOW TABLES`:

```
SHOW TABLES FROM example.web;
```

You can see a list of the columns in the `clicks` table in the `web`
database using either of the following:

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

No other types are supported.

```{include} jdbc-type-mapping.fragment
```

(vertica-sql-support)=

## SQL support

The connector provides read access and write access to data and metadata in
a Vertica database.  In addition to the {ref}`globally available
<sql-globally-available>` and {ref}`read operation <sql-read-operations>`
statements, the connector supports the following features:

- {doc}`/sql/insert`
- {doc}`/sql/update`
- {doc}`/sql/delete`
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

## Table functions

The connector provides specific {doc}`table functions </functions/table>` to
access Vertica.

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

- {ref}`join-pushdown`
- {ref}`limit-pushdown`
- {ref}`topn-pushdown`

```{include} pushdown-correctness-behavior.fragment
```

```{include} no-pushdown-text-type.fragment
```
