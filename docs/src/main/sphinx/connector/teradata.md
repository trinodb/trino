# Teradata connector

```{raw} html
<img src="../_static/img/teradata.png" class="connector-logo">
```

The Teradata connector allows querying and creating tables in an external
[Teradata](https://www.teradata.com/) database. This can be used to join 
data between different systems like Teradata and Hive, or between different Teradata instances.

## Requirements

To connect to Teradata, you need:

- Teradata Database
- Network access from the Trino coordinator and workers to Teradata. Port 
  1025 is the default port

## Configuration

To configure the Teradata connector, create a catalog properties file in
`etc/catalog` named, for example, `teradata.properties`, to mount the Teradata
connector as the `teradata` catalog. Create the file with the following 
contents, replacing the connection properties as appropriate for your setup:

```properties
connector.name=teradata
connection-url=jdbc:teradata://example.teradata.com/CHARSET=UTF8,TMODE=ANSI,LOGMECH=TD2
connection-user=***
connection-password=***
```

The `connection-url` defines the connection information and parameters to pass
to the Teradata JDBC driver. The supported parameters for the URL are 
available in the 
[Teradata JDBC documentation](https://teradata-docs.s3.amazonaws.com/doc/connectivity/jdbc/reference/current/jdbcug_chapter_2.html#BABJIHBJ).
For example, the following `connection-url` configures character encoding, 
transaction mode, and authentication.

```properties
connection-url=jdbc:teradata://example.teradata.com/CHARSET=UTF8,TMODE=ANSI,LOGMECH=TD2
```

The `connection-user` and `connection-password` are typically required and 
determine the user credentials for the connection, often a service user.

### Connection security

If you have TLS configured with a globally-trusted certificate installed on 
your data source, you can enable TLS between your cluster and the data 
source by appending parameters to the JDBC connection string set in the 
connection-url catalog configuration property.

For example, to specify SSLMODE:

```properties
connection-url=jdbc:teradata://example.teradata.com/SSLMODE=REQUIRED
```

For more information on TLS configuration options, see the
Teradata [JDBC documentation](https://teradata-docs.s3.amazonaws.com/doc/connectivity/jdbc/reference/current/jdbcug_chapter_2.html#URL_SSLMODE).

```{include} jdbc-authentication.fragment
```

### Multiple Teradata databases

You can have as many catalogs as you need, so if you have additional Teradata
databases, simply add another properties file to etc/catalog with a different
name, making sure it ends in .properties. 
For example, if you name the property file sales.properties, Trino creates a 
catalog named sales using the configured connector.

## Type mapping

Because Trino and Teradata each support types that the other does not, this
connector {ref}`modifies some types <type-mapping-overview>` when reading data.
Refer to the following sections for type mapping in when reading data from
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

```{include} jdbc-type-mapping.fragment
```

## Querying Teradata

The Teradata connector provides a schema for every Teradata database. You can
see the available Teradata databases by running SHOW SCHEMAS:

```
SHOW SCHEMAS FROM teradata;
```

If you have a Teradata database named sales, you can view the tables in this
database by running SHOW TABLES:

```
SHOW TABLES FROM teradata.sales;
```

You can see a list of the columns in the orders table in the sales database
using either of the following:

```
DESCRIBE teradata.sales.orders;
SHOW COLUMNS FROM teradata.sales.orders;
```

Finally, you can access the orders table in the sales database:

```
SELECT * FROM teradata.sales.orders;
```

## SQL support

The connector provides read access access to data and metadata in
a Teradata database.  The connector supports the {ref}`globally available
<sql-globally-available>` and {ref}`read operation <sql-read-operations>`
statements.

