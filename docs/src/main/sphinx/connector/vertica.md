---
myst:
  substitutions:
    default_domain_compaction_threshold: '`256`'
---

# Vertica connector

```{raw} html
<img src="../_static/img/vertica.png" class="connector-logo">
```

The Vertica connector allows querying a [Vertica database, also known as OpenText
Analytics Database](https://www.opentext.com/products/analytics-database), as an
external data source.

## Requirements

To connect to Vertica, you need:

- Vertica 9.1.x or higher.
- Network access from the coordinator and workers to the Vertica server.
  Port 5433 is the default port.

## Configuration

Create a catalog properties file in `etc/catalog` named `example.properties` to
access the configured Vertica database in the `example` catalog. Replace example
with your database name or some other descriptive name of the catalog. Configure
the usage of the connector by specifying the name `vertica` and replace the
connection properties as appropriate for your setup.

```properties
connector.name=vertica
connection-url=jdbc:vertica://example.net:5433/test_db
connection-user=root
connection-password=secret
```

The `connection-user` and `connection-password` are typically required and
determine the user credentials for the connection, often a service user. You can
use [secrets](/security/secrets) to avoid actual values in the catalog
properties files.

```{include} jdbc-common-configurations.fragment
```

```{include} query-comment-format.fragment
```

```{include} jdbc-domain-compaction-threshold.fragment
```

```{include} jdbc-case-insensitive-matching.fragment
```

## Type mapping

Because Trino and Vertica each support types that the other does not, this
connector [modifies some types](type-mapping-overview) when reading or writing
data. Data types may not map the same way in both directions between Trino and
the data source. Refer to the following sections for type mapping in each
direction.

### Vertica to Trino type mapping

The connector maps Vertica types to the corresponding Trino types according to
the following table:

:::{list-table} Vertica to Trino type mapping
:widths: 35, 25, 40
:header-rows: 1

* - Vertica type
  - Trino type
  - Notes
* - `BOOLEAN`
  - `BOOLEAN`
  -
* - `BIGINT`
  - `BIGINT`
  - Vertica treats TINYINT, SMALLINT, INTEGER, and BIGINT as synonyms for the
    same 64-bit BIGINT data type
* - `DOUBLE PRECISION (FLOAT)`
  - `DOUBLE`
  - Vertica treats FLOAT and REAL as the same 64-bit IEEE FLOAT
* - `DECIMAL(p, s)`
  - `DECIMAL(p, s)`
  -
* - `CHAR, CHAR(n)`
  - `CHAR, CHAR(n)`
  -
* - `VARCHAR`, `LONG VARCHAR`, `VARCHAR(n)`, `LONG VARCHAR(n)`
  - `VARCHAR(n)`
  -
* - `VARBINARY`, `LONG VARBINARY`, `VARBINARY(n)`, `LONG VARBINARY(n)`
  - `VARBINARY(n)`
  -
* - `DATE`
  - `DATE`
  -
:::

No other types are supported.

Unsupported Vertica types can be converted to `VARCHAR` with the
`vertica.unsupported_type_handling` session property. The default value for
this property is `IGNORE`.

```sql
SET SESSION vertica.unsupported_type_handling='CONVERT_TO_VARCHAR';
```

### Trino to Vertica type mapping

The connector maps Trino types to the corresponding Vertica types according to
the following table:

:::{list-table} Trino to Vertica type mapping
:widths: 50, 50
:header-rows: 1

* - Trino type
  - Vertica type
* - `BOOLEAN`
  - `BOOLEAN`
* - `TINYINT`
  - `BIGINT`
* - `SMALLINT`
  - `BIGINT`
* - `INTEGER`
  - `BIGINT`
* - `BIGINT`
  - `BIGINT`
* - `REAL`
  - `DOUBLE PRECISION`
* - `DOUBLE`
  - `DOUBLE PRECISION`
* - `DECIMAL(p, s)`
  - `DECIMAL(p, s)`
* - `CHAR`
  - `CHAR`
* - `VARCHAR`
  - `VARCHAR`
* - `VARBINARY`
  - `VARBINARY`
* - `DATE`
  - `DATE`
:::

No other types are supported.

```{include} jdbc-type-mapping.fragment
```

(vertica-sql-support)=
## SQL support

The connector provides read and write access to data and metadata in Vertica. In
addition to the [globally available](sql-globally-available) and [read
operation](sql-read-operations) statements, the connector supports the following
features:

- [](sql-data-management)
- [](/sql/create-table)
- [](/sql/create-table-as)
- [](/sql/drop-table)
- [](/sql/alter-table) excluding `DROP COLUMN`, see also [](vertica-alter-table)
- [](/sql/create-schema)
- [](/sql/drop-schema)
- [](vertica-table-functions)

(vertica-alter-table)=
```{include} alter-table-limitation.fragment
```

(vertica-table-functions)=
## Table functions

The connector provides specific [table functions](/functions/table) to
access Vertica.

(vertica-query-function)=
### `query(VARCHAR) -> table`

The `query` function allows you to query the underlying database directly. It
requires syntax native to the data source, because the full query is pushed down
and processed in the data source. This can be useful for accessing native
features or for improving query performance in situations where running a query
natively may be faster.

The `query` table function is available in the `system` schema of any
catalog that uses the Vertica connector, such as `example`. The
following example passes `myQuery` to the data source. `myQuery` has to be a
valid query for the data source, and is required to return a table as a result:

```sql
SELECT
  *
FROM
  TABLE(
    example.system.query(
      query => 'myQuery'
    )
  );
```

```{include} query-table-function-ordering.fragment
```

## Performance

The connector includes a number of performance features, detailed in the
following sections.

### Pushdown

The connector supports pushdown for a number of operations:

- [](join-pushdown)
- [](limit-pushdown)

```{include} join-pushdown-enabled-false.fragment
```

### Table statistics

The [cost-based optimizer](/optimizer/cost-based-optimizations) can use table
statistics from the Vertica database to improve query performance.

Support for table statistics is disabled by default. You can enable it with the
catalog property `statistics.enabled` set to `true`. In addition, the
`connection-user` configured in the catalog must have superuser permissions in
Vertica to gather and populate statistics.

You can view statistics using [](/sql/show-stats).
