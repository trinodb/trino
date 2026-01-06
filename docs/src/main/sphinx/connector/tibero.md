# Tibero connector

The Tibero connector allows querying and creating tables in an external Tibero
database. Connectors let Trino join data provided by different databases,
like Tibero and Hive, or different Tibero database instances.

## Requirements

To connect to Tibero, you need:

- Tibero 6 or higher.
- Network access from the Trino coordinator and workers to Tibero.
  Port 8629 is the default port.
- Tibero JDBC driver (tibero7-jdbc.jar) manually installed in the plugin directory.

## JDBC driver installation

The Tibero JDBC driver is not available in public Maven repositories and must
be installed manually:

1. Obtain the Tibero JDBC driver (tibero7-jdbc.jar) from TmaxSoft.
2. Copy the driver to the Trino plugin directory:
   ```
   cp tibero7-jdbc.jar $TRINO_HOME/plugin/tibero/
   ```
3. Restart the Trino server.

## Configuration

To configure the Tibero connector as the `example` catalog, create a file
named `example.properties` in `etc/catalog`. Include the following
connection properties in the file:

```text
connector.name=tibero
connection-url=jdbc:tibero:thin:@example.net:8629:tibero
connection-user=tibero
connection-password=secret
```

The `connection-url` defines the connection information and parameters to pass
to the JDBC driver. The Tibero connector uses the Tibero JDBC Thin driver.
The format is:

```
jdbc:tibero:thin:@<host>:<port>:<sid>
```

The `connection-user` and `connection-password` are typically required and
determine the user credentials for the connection, often a service user. You can
use {doc}`secrets </security/secrets>` to avoid actual values in the catalog
properties files.

### Multiple Tibero servers

If you want to connect to multiple Tibero servers, configure another instance of
the Tibero connector as a separate catalog.

To add another Tibero catalog, create a new properties file. For example, if
you name the property file `sales.properties`, Trino creates a catalog named
`sales`.

```{include} jdbc-common-configurations.fragment
```

```{include} query-comment-format.fragment
```

```{include} jdbc-case-insensitive-matching.fragment
```

## Querying Tibero

The Tibero connector provides a schema for every Tibero database.

Run `SHOW SCHEMAS` to see the available Tibero databases:

```
SHOW SCHEMAS FROM example;
```

If you used a different name for your catalog properties file, use that catalog
name instead of `example`.

:::{note}
The Tibero user must have access to the table in order to access it from Trino.
The user configuration, in the connection properties file, determines your
privileges in these schemas.
:::

### Examples

If you have a Tibero database named `web`, run `SHOW TABLES` to see the
tables it contains:

```
SHOW TABLES FROM example.web;
```

To see a list of the columns in the `clicks` table in the `web`
database, run either of the following:

```
DESCRIBE example.web.clicks;
SHOW COLUMNS FROM example.web.clicks;
```

To access the clicks table in the web database, run the following:

```
SELECT * FROM example.web.clicks;
```

(tibero-type-mapping)=
## Type mapping

Because Trino and Tibero each support types that the other does not, this
connector {ref}`modifies some types <type-mapping-overview>` when reading or
writing data. Data types may not map the same way in both directions between
Trino and the data source. Refer to the following sections for type mapping in
each direction.

### Tibero to Trino type mapping

Trino supports selecting Tibero database types. This table shows the Tibero to
Trino data type mapping:

:::{list-table} Tibero to Trino type mapping
:widths: 30, 25, 50
:header-rows: 1

* - Tibero database type
  - Trino type
  - Notes
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
* - `NUMBER(p, s)`
  - `DECIMAL(p, s)`
  -
* - `CHAR(n)`
  - `CHAR(n)`
  -
* - `VARCHAR(n)`
  - `VARCHAR(n)`
  -
* - `NVARCHAR(n)`
  - `VARCHAR(n)`
  -
* - `CLOB`
  - `VARCHAR`
  -
* - `NCLOB`
  - `VARCHAR`
  -
* - `RAW(n)`
  - `VARBINARY`
  -
* - `BLOB`
  - `VARBINARY`
  -
* - `DATE`
  - `TIMESTAMP(0)`
  - See [](tibero-datetime-mapping)
* - `TIMESTAMP(p)`
  - `TIMESTAMP(p)`
  - See [](tibero-datetime-mapping)
:::

No other types are supported.

### Trino to Tibero type mapping

Trino supports creating tables with the following types in a Tibero database.
The table shows the mappings from Trino to Tibero data types:

:::{list-table} Trino to Tibero Type Mapping
:widths: 30, 25, 50
:header-rows: 1

* - Trino type
  - Tibero database type
  - Notes
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
  - `DOUBLE PRECISION`
  -
* - `VARCHAR(n)`
  - `VARCHAR(n)`
  -
* - `CHAR(n)`
  - `CHAR(n)`
  -
:::

No other types are supported.

(tibero-datetime-mapping)=
### Mapping datetime types

Writing a timestamp with fractional second precision (`p`) greater than 9
rounds the fractional seconds to nine digits.

Tibero `DATE` type stores year, month, day, hour, minute, and seconds, so it
is mapped to Trino `TIMESTAMP(0)`.

```{include} jdbc-type-mapping.fragment
```

(tibero-sql-support)=
## SQL support

The connector provides read access and write access to data and metadata in
Tibero. In addition to the [globally available](sql-globally-available) and
[read operation](sql-read-operations) statements, the connector supports the
following features:

- [](/sql/insert)
- [](/sql/delete)
- [](/sql/truncate)
- [](/sql/create-table)
- [](/sql/create-table-as)
- [](/sql/drop-table)
- [](/sql/alter-table)
- [](/sql/comment)

(tibero-table-functions)=
### Table functions

The connector provides specific {doc}`table functions </functions/table>` to
access Tibero.

(tibero-query-function)=
#### `query(varchar) -> table`

The `query` function allows you to query the underlying database directly. It
requires syntax native to Tibero, because the full query is pushed down and
processed in Tibero. This can be useful for accessing native features which are
not available in Trino or for improving query performance in situations where
running a query natively may be faster.

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

```{include} query-table-function-ordering.fragment
```

## Limitations

The following SQL statements are not yet supported:

- [](/sql/update)
- [](/sql/merge)
- [](/sql/grant)
- [](/sql/revoke)
