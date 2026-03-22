# Tibero connector

The Tibero connector allows querying and creating tables in an external
[Tibero](https://www.tmaxtibero.com/product/productView.do?prod_cd=tibero&detail_gubun=prod_main) database.
Tibero is an enterprise-grade relational database management system developed
by TmaxSoft.

## Requirements

To connect to Tibero, you need:

- Tibero 6 or higher.
- Tibero JDBC driver (ex. tibero7-jdbc.jar) manually installed in the plugin directory.

## JDBC driver installation

The Tibero JDBC driver is not available in public Maven repositories and must
be installed manually:

1. Obtain the Tibero JDBC driver (ex. tibero7-jdbc.jar):

   - **If you have Tibero database installed**: The JDBC driver is included in
     the Tibero installation at `$TB_HOME/client/lib/jar/`. See the
     [Tibero JDBC documentation](https://docs.tibero.com/tibero/en/topics/development/jdbc-developers-guide/introduction-to-tibero-jdbc#default-path)
     for more details.

   - **If you don't have Tibero installed**: Download the Tibero distribution
     from [TechNet](https://technet.tibero.com/en/front/download/findDownloadList.do).
     After extracting the tar.gz file, the JDBC driver can be found at
     `$TB_HOME/client/lib/jar/`.

   Contact TmaxSoft for licensing information and to ensure the driver can be
   used in your environment.

2. Add the driver dependency to the `plugin/trino-tibero/pom.xml` file:

   ```xml
   <dependency>
       <groupId>com.tmax.tibero</groupId>
       <artifactId>tibero7-jdbc</artifactId>
       <version>7.2.3</version>
       <scope>system</scope>
       <systemPath>/path/to/tibero7-jdbc.jar</systemPath>
   </dependency>
   ```

   Replace `/path/to/tibero7-jdbc.jar` with the actual path to your Tibero
   JDBC driver file. For example, if you place the driver in the Trino source
   root's `lib` directory, use:

   ```xml
   <systemPath>${project.basedir}/../../lib/tibero7-jdbc.jar</systemPath>
   ```

> **Note:**
Ensure you have the appropriate license from TmaxSoft to use the Tibero JDBC
driver in your environment. The driver's usage is subject to TmaxSoft's
licensing terms.


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


## Querying Tibero

The Tibero connector provides a schema for every Tibero database.

Run `SHOW SCHEMAS` to see the available Tibero databases:

```
SHOW SCHEMAS FROM example;
```

If you used a different name for your catalog properties file, use that catalog
name instead of `example`.

> **Note:**
The Tibero user must have access to the table in order to access it from Trino.
The user configuration, in the connection properties file, determines your
privileges in these schemas.


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


## Type mapping

Because Trino and Tibero each support types that the other does not, this
connector {ref}`modifies some types <type-mapping-overview>` when reading or
writing data. Data types may not map the same way in both directions between
Trino and the data source. Refer to the following sections for type mapping in
each direction.

### Tibero to Trino type mapping

Trino supports reading the following Tibero database types:

| Tibero type | Trino type | Notes |
|-------------|------------|-------|
| `SMALLINT` | `SMALLINT` | |
| `INTEGER` | `INTEGER` | |
| `BIGINT` | `BIGINT` | |
| `REAL` | `REAL` | |
| `DOUBLE` | `DOUBLE` | |
| `NUMBER(p, s)`, `DECIMAL(p, s)` | `DECIMAL(p, s)` | Default precision is 38 if not specified |
| `CHAR(n)` | `CHAR(n)` | |
| `VARCHAR(n)`, `NVARCHAR(n)` | `VARCHAR(n)` | |
| `CLOB`, `NCLOB` | `VARCHAR` | Unbounded VARCHAR |
| `RAW(n)`, `BLOB` | `VARBINARY` | |
| `DATE` | `TIMESTAMP(0)` | Tibero DATE includes time components |
| `TIMESTAMP(p)` | `TIMESTAMP(p)` | Precision up to 9 digits |

No other types are supported.

### Trino to Tibero type mapping

Trino supports creating tables with the following types in Tibero:

| Trino type | Tibero type | Notes |
|------------|-------------|-------|
| `SMALLINT` | `SMALLINT` | |
| `INTEGER` | `INTEGER` | |
| `BIGINT` | `BIGINT` | |
| `REAL` | `REAL` | |
| `DOUBLE` | `DOUBLE PRECISION` | |
| `CHAR(n)` | `CHAR(n)` | |
| `VARCHAR(n)`, `VARCHAR` | `VARCHAR(n)`, `VARCHAR` | Unbounded VARCHAR is supported |

No other types are supported.


### Mapping datetime types

Writing a timestamp with fractional second precision (`p`) greater than 9
rounds the fractional seconds to nine digits.

Tibero `DATE` type stores year, month, day, hour, minute, and seconds, so it
is mapped to Trino `TIMESTAMP(0)`.

```{include} jdbc-type-mapping.fragment
```


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
