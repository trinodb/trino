# Teradata connector

```{raw} html
<img src="../_static/img/teradata.png" class="connector-logo">
```

The Teradata connector allows querying and creating tables in an 
external [Teradata](https://www.teradata.com/) database. This can be used to join data between
different systems like Teradata and Hive, or between different
Teradata instances.

## Requirements

To connect to Teradata, you need:

- Teradata database 16.20 or higher.
- Network access from the Trino coordinator and workers to Teradata. 
- Port 1025 is the default port.

## Configuration

The connector can query a database on a given Teradata instance. Create a catalog
properties file that specifies the Teradata connector by setting the
`connector.name` to `teradata`.

For example, to access a database as the `example` catalog, create the file
`etc/catalog/example.properties`. Replace the connection properties as
appropriate for your setup:

```properties
connector.name=teradata
connection-url=jdbc:teradata://example.teradata.com/CHARSET=UTF8,TMODE=ANSI,LOGMECH=TD2
connection-user=root
connection-password=secret
```

The `connection-url` defines the connection information and parameters to pass
to the Teradata JDBC driver. The parameters for the URL are available in the
[Teradata JDBC documentation](https://teradata-docs.s3.amazonaws.com/doc/connectivity/jdbc/reference/current/jdbcug_chapter_2.html#BABJIHBJ).

The `connection-user` and `connection-password` are typically required and
determine the user credentials for the connection, often a service user. You can
use {doc}`secrets </security/secrets>` to avoid actual values in the catalog
properties files.

### Connection security

If you have TLS configured with a globally-trusted certificate installed on 
your data source, you can enable TLS between your cluster and the data 
source by appending parameters to the JDBC connection string set in the 
connection-url catalog configuration property.

For example, to specify `SSLMODE`:

```properties
connection-url=jdbc:teradata://example.teradata.com/SSLMODE=REQUIRED
```

For more information on TLS configuration options, see the
Teradata [JDBC documentation](https://teradata-docs.s3.amazonaws.com/doc/connectivity/jdbc/reference/current/jdbcug_chapter_2.html#URL_SSLMODE).

```{include} jdbc-authentication.fragment
```

### Multiple Teradata databases

The Teradata connector can only access a single Teradata database within
a single catalog. Thus, if you have multiple Teradata databases,
or want to connect to multiple Teradata instances, you must configure
multiple instances of the Teradata connector.

To add another catalog, simply add another properties file to `etc/catalog`
with a different name, making sure it ends in `.properties`. For example,
if you name the property file `sales.properties`, Trino creates a
catalog named `sales` using the configured connector.

## Type mapping

Because Trino and Teradata each support types that the other does not, this
connector {ref}`modifies some types <type-mapping-overview>` when reading data.
Refer to the following sections for type mapping when reading data from
Teradata to Trino.

### Teradata type to Trino type mapping

The connector maps Teradata types to the corresponding Trino types following
this table:

:::{list-table} Teradata type to Trino type mapping
:widths: 40, 40, 20
:header-rows: 1

* - Teradata type
  - Trino type
  - Notes
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
* - `REAL`
  - `DOUBLE`
  -
* - `DOUBLE`
  - `DOUBLE`
  -
* - `FLOAT`
  - `DOUBLE`
  -
* - `NUMBER(p, s)`
  - `DECIMAL(p, s)`
  - 
* - `NUMERIC(p, s)`
  - `DECIMAL(p, s)`
  - 
* - `DECIMAL(p, s)`
  - `DECIMAL(p, s)`
  - 
* - `CHAR(n)`
  - `CHAR(n)`
  -
* - `CHARACTER(n)`
  - `CHAR(n)`
  -
* - `VARCHAR(n)`
  - `VARCHAR(n)`
  - 
* - `DATE`
  - `DATE`
  -
:::

No other types are supported.

### Trino type to Teradata type mapping

The connector maps Trino types to the corresponding Teradata types following
this table:

:::{list-table} Trino type to Teradata type mapping
:widths: 40, 40, 20
:header-rows: 1

* - Trino type
  - Teradata type
  - Notes
* - `TINYINT`
  - `SMALLINT`
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
  - `FLOAT`
  -
* - `DOUBLE`
  - `DOUBLE`
  - 
* - `DATE`
  - `DATE`
  -
::::

No other types are supported.

```{include} jdbc-type-mapping.fragment
```

## SQL support

The connector provides read and limited write access to data and metadata in
a Teradata database.  In addition to the [globally available](sql-globally-available) and
[read operation](sql-read-operations) statements, the connector supports the
following features:

- [](/sql/create-schema)
- [](/sql/drop-schema)
- [](/sql/create-table)
- [](/sql/drop-table)
