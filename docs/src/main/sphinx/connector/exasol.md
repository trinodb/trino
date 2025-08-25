---
myst:
  substitutions:
    default_domain_compaction_threshold: '`256`'
---

# Exasol connector

```{raw} html
<img src="../_static/img/exasol.png" class="connector-logo">
```

The Exasol connector allows querying an [Exasol](https://www.exasol.com/) database.

## Requirements

To connect to Exasol, you need:

* Exasol database version 7.1 or higher.
* Network access from the Trino coordinator and workers to Exasol.
  Port 8563 is the default port.

## Configuration

To configure the Exasol connector as the ``example`` catalog, create a file
named ``example.properties`` in ``etc/catalog``. Include the following
connection properties in the file:

```text
connector.name=exasol
connection-url=jdbc:exa:exasol.example.com:8563
connection-user=user
connection-password=secret
```

The ``connection-url`` defines the connection information and parameters to pass
to the JDBC driver. See the
[Exasol JDBC driver documentation](https://docs.exasol.com/db/latest/connect_exasol/drivers/jdbc.htm#ExasolURL)
for more information.

The ``connection-user`` and ``connection-password`` are typically required and
determine the user credentials for the connection, often a service user. You can
use {doc}`secrets </security/secrets>` to avoid using actual values in catalog
properties files.

:::{note}
If your Exasol database uses a self-signed TLS certificate you must
specify the certificate's fingerprint in the JDBC URL using parameter
``fingerprint``, e.g.: ``jdbc:exa:exasol.example.com:8563;fingerprint=ABC123``.
:::

```{include} jdbc-authentication.fragment
```

```{include} jdbc-common-configurations.fragment
```

```{include} jdbc-domain-compaction-threshold.fragment
```

```{include} jdbc-case-insensitive-matching.fragment
```

(exasol-type-mapping)=
## Type mapping

Because Trino and Exasol each support types that the other does not, this
connector {ref}`modifies some types <type-mapping-overview>` when reading data.
Data types may not map the same way in both directions between
Trino and the data source. Refer to the following sections for type mapping in
each direction.

### Exasol to Trino type mapping

Trino supports selecting Exasol database types. This table shows the Exasol to
Trino data type mapping:

```{eval-rst}
.. list-table:: Exasol to Trino type mapping
  :widths: 25, 25, 50
  :header-rows: 1

  * - Exasol database type
    - Trino type
    - Notes
  * - ``BOOLEAN``
    - ``BOOLEAN``
    -
  * - ``DOUBLE PRECISION``
    - ``REAL``
    -
  * - ``DECIMAL(p, s)``
    - ``DECIMAL(p, s)``
    -  See :ref:`exasol-number-mapping`
  * - ``CHAR(n)``
    - ``CHAR(n)``
    -
  * - ``VARCHAR(n)``
    - ``VARCHAR(n)``
    -
  * - ``DATE``
    - ``DATE``
    -
  * - ``INTERVAL YEAR(y) TO MONTH``
    - ``BIGINT``
    -  See :ref:`exasol-interval-year-month-mapping`
  * - ``INTERVAL DAY(d) TO SECOND(s)``
    - ``BIGINT``
    -  See :ref:`exasol-interval-day-second-mapping`
```

No other types are supported.

(exasol-number-mapping)=
### Mapping numeric types

An Exasol `DECIMAL(p, s)` maps to Trino's `DECIMAL(p, s)` and vice versa
except in these conditions:

- No precision is specified for the column (example: `DECIMAL` or
  `DECIMAL(*)`).
- Scale (`s`) is greater than precision.
- Precision (`p`) is greater than 36.
- Scale is negative.

(exasol-character-mapping)=
### Mapping character types

Trino's `VARCHAR(n)` maps to `VARCHAR(n)` and vice versa if `n` is no greater
than 2000000. Exasol does not support longer values.
If no length is specified, the connector uses 2000000.

Trino's `CHAR(n)` maps to `CHAR(n)` and vice versa if `n` is no greater than 2000.
Exasol does not support longer values.

(exasol-interval-year-month-mapping)=
### Mapping `INTERVAL YEAR TO MONTH` Types

Exasol `INTERVAL YEAR(y) TO MONTH` columns are mapped to Trino's `BIGINT` type (number of months)
and vice versa, with the following exceptions:

- **No precision `y` specified**:  
  If the precision `y` is omitted (i.e., the column is defined as `INTERVAL YEAR TO MONTH`
  without `(y)`), Exasol defaults to a precision of 2.

- **Precision `y` greater than 9**:  
  Exasol supports up to 9 digits for number of years.
  If the precision `y` in Exasol exceeds 9, an exception is thrown

- **Negative and zero `y` precisions**:  
  Negative and zero values for precision `y` are invalid. If encountered,
  an exception is thrown.

(exasol-interval-day-second-mapping)=
### Mapping `INTERVAL DAY TO SECOND` Types

Exasol `INTERVAL DAY(d) TO SECOND(s)` columns are mapped to Trino's `BIGINT` type (number of milliseconds)
and vice versa, with the following exceptions:

- **No precision `d` is specified**:  
  If the precision `d` is omitted (i.e., the column is defined as `INTERVAL DAY TO SECOND(s)` 
  without `(d)`), Exasol defaults to a precision of 2.

- **Precision `d` is greater than 9**:
  Exasol supports up to 9 digits for precision `d`.
  If the precision `d` in Exasol exceeds 9, an exception is thrown

- **Negative and zero `d` precisions**:  
  Negative and zero values for precision `d` are invalid. If encountered,
  an exception is thrown.

- **No fractional second precision `s` is specified**:  
  If the fractional second precision `s` is omitted (i.e., the column is defined as `INTERVAL DAY(d) TO SECOND`
  without `(s)`), Exasol defaults to a fractional second precision of 3.

- **Fractional second precision `s` is greater than 9**:
  Exasol supports up to 9 digits for fractional second precision `s`.
  If the fractional second precision `s` in Exasol exceeds 9, an exception is thrown

- **Negative fractional second precision `s`**:  
  Negative values for fractional second precision `s` are invalid. If encountered,
  an exception is thrown.

```{include} jdbc-type-mapping.fragment
```

(exasol-sql-support)=
## SQL support

The connector provides read access to data and metadata in Exasol. In addition
to the [globally available](sql-globally-available) and [read
operation](sql-read-operations) statements, the connector supports the following
features:

- [](exasol-procedures)
- [](exasol-table-functions)

(exasol-procedures)=
### Procedures

```{include} jdbc-procedures-flush.fragment
```
```{include} procedures-execute.fragment
```

(exasol-table-functions)=
### Table functions

The connector provides specific {doc}`table functions </functions/table>` to
access Exasol.

(exasol-query-function)=
#### `query(varchar) -> table`

The `query` function allows you to query the underlying database directly. It
requires syntax native to Exasol, because the full query is pushed down and
processed in Exasol. This can be useful for accessing native features which are
not available in Trino or for improving query performance in situations where
running a query natively may be faster.

```{include} query-passthrough-warning.fragment
```

As a simple example, query the `example` catalog and select an entire table::

```sql
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

As a practical example, you can use the
[WINDOW clause from Exasol](https://docs.exasol.com/db/latest/sql_references/functions/analyticfunctions.htm#AnalyticFunctions):

```sql
SELECT
  *
FROM
  TABLE(
    example.system.query(
      query => 'SELECT
        id, department, hire_date, starting_salary,
        AVG(starting_salary) OVER w2 AVG,
        MIN(starting_salary) OVER w2 MIN_STARTING_SALARY,
        MAX(starting_salary) OVER (w1 ORDER BY hire_date)
      FROM employee_table
      WINDOW w1 as (PARTITION BY department), w2 as (w1 ORDER BY hire_date)
      ORDER BY department, hire_date'
    )
  );
```

```{include} query-table-function-ordering.fragment
```
