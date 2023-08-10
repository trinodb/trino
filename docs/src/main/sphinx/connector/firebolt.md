# Firebolt connector

```{raw} html
<img src="../_static/img/firebolt.png" class="connector-logo">
```

The Firebolt connector allows querying and creating tables in a
[Firebolt](https://www.firebolt.io/) database. This can be used to join data between
different systems like Firebolt and PostgreSQL, or between different
Firebolt instances.

## Requirements

To connect to Firebolt, you need:

- [Firebolt account](https://www.firebolt.io/getting-started-now).

## Configuration

Create a catalog
properties file that specifies the Firebolt connector by setting the
`connector.name` to `firebolt`.

For example, to access a database as the `example` catalog, create the file
`etc/catalog/example.properties`. Replace the connection properties as
appropriate for your setup:

```text
connector.name=firebolt
connection-url=jdbc:firebolt://api.app.firebolt.io/my_database
connection-user=me@company.com
connection-password=my_password
```

The `connection-url` specifies the JDBC connection string. Minimal required information here is `api.app.firebolt.io` and the database name. Other optional arguments are described in [Firebolt JDBC docs](https://docs.firebolt.io/developing-with-firebolt/connecting-with-jdbc.html#connecting-to-firebolt-with-the-jdbc-driver).

The `connection-user` and `connection-password` are typically required and
determine the user credentials for the connection, often a service user. You can
use {doc}`secrets </security/secrets>` to avoid actual values in the catalog
properties files.

## Type mapping

Because Trino and Firebolt each support types that the other does not, this
connector {ref}`modifies some types <type-mapping-overview>` when reading or
writing data. Data types may not map the same way in both directions between
Trino and the data source. Refer to the following sections for type mapping in
each direction.

### Firebolt type to Trino type mapping

The connector maps Firebolt types to the corresponding Trino types following
this table:

```{eval-rst}
.. list-table:: Firebolt type to Trino type mapping
  :widths: 30, 20, 50
  :header-rows: 1

  * - Firebolt type
    - Trino type
    - Notes
  * - ``BOOLEAN``
    - ``BOOLEAN``
    -
  * - ``INTEGER``
    - ``INTEGER``
    -
  * - ``BIGINT``
    - ``BIGINT``
    -
  * - ``REAL``
    - ``REAL``
    -
  * - ``DOUBLE PRECISION``
    - ``DOUBLE``
    -
  * - ``NUMERIC(p, s)``
    - ``DECIMAL(p, s)``
    - ``Not supported yet.``
  * - ``TEXT``
    - ``VARCHAR``
    -
  * - ``BYTEA``
    - ``VARBINARY``
    - ``Not supported yet.``
  * - ``DATE``
    - ``DATE``
    -
  * - ``TIMESTAMP``
    - ``TIMESTAMP``
    -
  * - ``TIMESTAMPTZ``
    - ``TIMESTAMP WITH TIME ZONE``
    - ``Not supported yet.``
  * - ``ARRAY``
    - ``ARRAY``
    - ``Not supported yet.``
```

No other types are supported.

### Trino type to Firebolt type mapping

The connector maps Trino types to the corresponding Firebolt types following
this table:

```{eval-rst}
.. list-table:: Trino type to Firebolt type mapping
  :widths: 30, 20, 50
  :header-rows: 1

  * - Trino type
    - Firebolt type
    - Notes
  * - ``BOOLEAN``
    - ``BOOLEAN``
    -
  * - ``SMALLINT``
    - ``INTEGER``
    -
  * - ``TINYINT``
    - ``INTEGER``
    -
  * - ``INTEGER``
    - ``INTEGER``
    -
  * - ``BIGINT``
    - ``BIGINT``
    -
  * - ``DOUBLE``
    - ``DOUBLE PRECISION``
    -
  * - ``DECIMAL(p, s)``
    - ``NUMERIC(p, s)``
    - ``Not supported yet.``
  * - ``CHAR(n)``
    - ``TEXT``
    -
  * - ``VARCHAR(n)``
    - ``TEXT``
    -
  * - ``VARBINARY``
    - ``BYTEA``
    - ``Not supported yet.``
  * - ``DATE``
    - ``DATE``
    -
  * - ``TIMESTAMP(n)``
    - ``TIMESTAMP``
    -
  * - ``TIMESTAMP(n) WITH TIME ZONE``
    - ``TIMESTAMPTZ``
    - ``Not supported yet.``
  * - ``ARRAY``
    - ``ARRAY``
    - ``Not supported yet.``
```

No other types are supported.

## Querying Firebolt

Firebolt does not currently support different schemas. The only schema supported is `public`. There are also `catalog` and `information_schema` metadata schemas.

You can see the available Firebolt schemas by running `SHOW SCHEMAS`:

```
SHOW SCHEMAS FROM firebolt;
```

You can view the tables by running `SHOW TABLES`:

```
SHOW TABLES FROM firebolt.public;
```

You can see a list of the columns in the `dummy` table using either of the following:

```
DESCRIBE firebolt.public.dummy;
SHOW COLUMNS FROM firebolt.public.dummy;
```

Finally, you can access the `dummy` table:

```
SELECT * FROM firebolt.public.dummy;
```

If you used a different name for your catalog properties file, use
that catalog name instead of `firebolt` in the above examples.

## SQL support

The connector provides read and write access to data and metadata in
Firebolt.  In addition to the {ref}`globally available
<sql-globally-available>` and {ref}`read operation <sql-read-operations>`
statements, the connector supports the following features:

- {doc}`/sql/insert`
- {doc}`/sql/delete`

```{eval-rst}
.. include:: sql-delete-limitation.fragment

```

(firebolt-fte-support)=

## Fault-tolerant execution support

The connector supports {doc}`/admin/fault-tolerant-execution` of query
processing. Read and write operations are both supported with any retry policy.

## Performance

The connector includes a number of performance improvements, detailed in the
following sections.

(firebolt-pushdown)=

### Pushdown

The connector supports pushdown for a number of operations:

- {ref}`limit-pushdown`
- {ref}`topn-pushdown`

{ref}`Aggregate pushdown <aggregation-pushdown>` for the following functions:

- {func}`avg`
- {func}`count`
- {func}`max`
- {func}`min`
- {func}`sum`
- {func}`stddev`
- {func}`stddev_samp`

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
