---
myst:
  substitutions:
    default_domain_compaction_threshold: "`256`"
---
# Snowflake connector

```{raw} html
<img src="../_static/img/snowflake.png" class="connector-logo">
```

The Snowflake connector allows querying and creating tables in an
external [Snowflake](https://www.snowflake.com/) account. This can be used to join data between
different systems like Snowflake and Hive, or between two different
Snowflake accounts.

## Configuration

To configure the Snowflake connector, create a catalog properties file
in `etc/catalog` named, for example, `example.properties`, to
mount the Snowflake connector as the `snowflake` catalog.
Create the file with the following contents, replacing the
connection properties as appropriate for your setup:

```none
connector.name=snowflake
connection-url=jdbc:snowflake://<account>.snowflakecomputing.com
connection-user=root
connection-password=secret
snowflake.account=account
snowflake.database=database
snowflake.role=role
snowflake.warehouse=warehouse
```

### Arrow serialization support

This is an experimental feature which introduces support for using Apache Arrow
as the serialization format when reading from Snowflake.  Please note there are
a few caveats:

- Using Apache Arrow serialization is disabled by default. In order to enable
  it,  add `--add-opens=java.base/java.nio=ALL-UNNAMED` to the Trino
  {ref}`jvm-config`.

### Multiple Snowflake databases or accounts

The Snowflake connector can only access a single database within
a Snowflake account. Thus, if you have multiple Snowflake databases,
or want to connect to multiple Snowflake accounts, you must configure
multiple instances of the Snowflake connector.

```{include} jdbc-common-configurations.fragment
```

```{include} query-comment-format.fragment
```

```{include} jdbc-domain-compaction-threshold.fragment
```

```{include} jdbc-case-insensitive-matching.fragment
```

```{include} non-transactional-insert.fragment
```

% snowflake-type-mapping:

## Type mapping

Because Trino and Snowflake each support types that the other does not, this
connector {ref}`modifies some types <type-mapping-overview>` when reading or
writing data. Data types may not map the same way in both directions between
Trino and the data source. Refer to the following sections for type mapping in
each direction.

List of [Snowflake data types](https://docs.snowflake.com/en/sql-reference/intro-summary-data-types.html).

### Snowflake type to Trino type mapping

The connector maps Snowflake types to the corresponding Trino types following
this table:

:::{list-table} Snowflake type to Trino type mapping
:widths: 30, 30, 40
:header-rows: 1

* - Snowflake type
  - Trino type
  - Notes
* - `BOOLEAN`
  - `BOOLEAN`
  -
* - `INT`, `INTEGER`, `BIGINT`, `SMALLINT`, `TINYINT`, `BYTEINT`
  - `DECIMAL(38,0)`
  - Synonymous with `NUMBER(38,0)`. See Snowflake
    [data types for fixed point numbers](https://docs.snowflake.com/en/sql-reference/data-types-numeric#data-types-for-fixed-point-numbers)
    for more information.
* - `FLOAT`, `FLOAT4`, `FLOAT8`
  - `DOUBLE`
  - The names `FLOAT`, `FLOAT4`, and `FLOAT8` are for compatibility with other systems; Snowflake treats all three as
    64-bit floating-point numbers. See Snowflake
    [data types for floating point numbers](https://docs.snowflake.com/en/sql-reference/data-types-numeric#data-types-for-floating-point-numbers)
    for more information.
* - `DOUBLE`, `DOUBLE PRECISION`, `REAL`
  - `DOUBLE`
  - Synonymous with `FLOAT`. See Snowflake
    [data types for floating point numbers](https://docs.snowflake.com/en/sql-reference/data-types-numeric#data-types-for-floating-point-numbers)
    for more information.
* - `NUMBER`
  - `DECIMAL`
  - Default precision and scale are (38,0).
* - `DECIMAL`, `NUMERIC`
  - `DECIMAL`
  - Synonymous with `NUMBER`. See Snowflake
    [data types for fixed point numbers](https://docs.snowflake.com/en/sql-reference/data-types-numeric#data-types-for-fixed-point-numbers)
    for more information.
* - `VARCHAR`
  - `VARCHAR`
  -
* - `CHAR`, `CHARACTER`
  - `VARCHAR`
  - Synonymous with `VARCHAR` except default length is `VARCHAR(1)`. See Snowflake
    [String & Binary Data Types](https://docs.snowflake.com/en/sql-reference/data-types-text)
    for more information.
* - `STRING`, `TEXT`
  - `VARCHAR`
  - Synonymous with `VARCHAR`. See Snowflake
    [String & Binary Data Types](https://docs.snowflake.com/en/sql-reference/data-types-text)
    for more information.
* - `BINARY`
  - `VARBINARY`
  -
* - `VARBINARY`
  - `VARBINARY`
  - Synonymous with `BINARY`. See Snowflake
    [String & Binary Data Types](https://docs.snowflake.com/en/sql-reference/data-types-text)
    for more information.
* - `DATE`
  - `DATE`
  -
* - `TIME`
  - `TIME`
  -
* - `TIMESTAMP_NTZ`
  - `TIMESTAMP`
  - TIMESTAMP with no time zone; time zone, if provided, is not stored. See Snowflake
    [Date & Time Data Types](https://docs.snowflake.com/en/sql-reference/data-types-datetime)
    for more information.
* - `DATETIME`
  - `TIMESTAMP`
  - Alias for `TIMESTAMP_NTZ`. See Snowflake
    [Date & Time Data Types](https://docs.snowflake.com/en/sql-reference/data-types-datetime)
    for more information.
* - `TIMESTAMP`
  - `TIMESTAMP`
  - Alias for one of the `TIMESTAMP` variations (`TIMESTAMP_NTZ` by default). This connector always sets `TIMESTAMP_NTZ` as the variant.
* - `TIMESTAMP_TZ`
  - `TIMESTAMP WITH TIME ZONE`
  - TIMESTAMP with time zone.
:::

No other types are supported.

### Trino type to Snowflake type mapping

The connector maps Trino types to the corresponding Snowflake types following
this table:

:::{list-table} Trino type to Snowflake type mapping
:widths: 30, 30, 40
:header-rows: 1

* - Trino type
  - Snowflake type
  - Notes
* - `BOOLEAN`
  - `BOOLEAN`
  -
* - `TINYINT`
  - `NUMBER(3, 0)`
  -
* - `SMALLINT`
  - `NUMBER(5, 0)`
  -
* - `INTEGER`
  - `NUMBER(10, 0)`
  -
* - `BIGINT`
  - `NUMBER(19, 0)`
  -
* - `REAL`
  - `DOUBLE`
  -
* - `DOUBLE`
  - `DOUBLE`
  -
* - `DECIMAL`
  - `NUMBER`
  -
* - `VARCHAR`
  - `VARCHAR`
  -
* - `CHAR`
  - `VARCHAR`
  -
* - `VARBINARY`
  - `BINARY`
  -
* - `VARBINARY`
  - `VARBINARY`
  -
* - `DATE`
  - `DATE`
  -
* - `TIME`
  - `TIME`
  -
* - `TIMESTAMP`
  - `TIMESTAMP_NTZ`
  -
* - `TIMESTAMP WITH TIME ZONE`
  - `TIMESTAMP_TZ`
  -
:::

No other types are supported.

```{include} jdbc-type-mapping.fragment
```

(snowflake-sql-support)=
## SQL support

The connector provides read access and write access to data and metadata in
a Snowflake database.  In addition to the {ref}`globally available
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
access Snowflake.

(snowflake-query-function)=
#### `query(varchar) -> table`

The `query` function allows you to query the underlying database directly. It
requires syntax native to Snowflake, because the full query is pushed down and
processed in Snowflake. This can be useful for accessing native features which
are not available in Trino or for improving query performance in situations
where running a query natively may be faster.

Find details about the SQL support of Snowflake that you can use in the query in
the [Snowflake SQL Command
Reference](https://docs.snowflake.com/en/sql-reference-commands), including
[PIVOT](https://docs.snowflake.com/en/sql-reference/constructs/pivot), [lateral
joins](https://docs.snowflake.com/en/sql-reference/constructs/join-lateral) and
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

As a practical example, you can use the Snowflake SQL support for
[PIVOT](https://docs.snowflake.com/en/sql-reference/constructs/pivot) to pivot
on all distinct column values automatically with a dynamic pivot.

```
SELECT
  *
FROM
  TABLE(
    example.system.query(
      query => '
        SELECT *
        FROM quarterly_sales
          PIVOT(SUM(amount) FOR quarter IN (ANY ORDER BY quarter))
        ORDER BY empid;
      '
    )
  );
```

```{include} query-table-function-ordering.fragment
```

## Performance

The connector includes a number of performance improvements, detailed in the
following sections.

(snowflake-pushdown)=
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
- {func}`covar_pop`
- {func}`covar_samp`
- {func}`corr`
- {func}`regr_intercept`
- {func}`regr_slope`

```{include} pushdown-correctness-behavior.fragment
```
