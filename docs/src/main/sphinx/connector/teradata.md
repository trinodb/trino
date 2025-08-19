# Teradata connector

<img src="../_static/img/teradata.png" class="connector-logo">

The Teradata connector allows querying and creating tables in an external Teradata database. 
This can be used to join data between different systems like Teradata and Hive, or between different Teradata instances.

## Requirements

To connect to Teradata, you need:

* Teradata Database
* Network access from the Trino coordinator and workers to Teradata. Port 1025 is the default port
* Teradata JDBC driver (`terajdbc4.jar`)

## Configuration

To configure the Teradata connector, create a catalog properties file in `etc/catalog` named, for example, `teradata.properties`, to mount the Teradata connector as the `teradata` catalog. Create the file with the following contents, replacing the connection properties as appropriate for your setup:

```properties
connector.name=teradata
connection-url=jdbc:teradata://example.teradata.com/
connection-user=***
connection-password=***
```

The connection-url defines the connection information and parameters to pass to the Teradata JDBC driver. The supported parameters for the URL are available in the Teradata JDBC documentation.

For example, the following connection-url configures connection pooling and character encoding:
```example
connection-url=jdbc:teradata://example.teradata.com/CHARSET=UTF8,TMODE=ANSI
```
The connection-user and connection-password are typically required and determine the user credentials for the connection, often a service user.

ClearScape Analytics configuration
For ClearScape Analytics environments, you can configure additional properties:
```clearscape properties
connector.name=teradata
connection-url=jdbc:teradata://clearscape-host.teradata.com/
connection-user=demo_user
connection-password=***
teradata.clearscape.url=https://api.clearscape.teradata.com
teradata.clearscape.region=asia-south
```
## Connection security
If you have TLS configured with a globally-trusted certificate installed on your data source, you can enable TLS between your cluster and the data source by appending parameters to the JDBC connection string set in the connection-url catalog configuration property.

For example, to enable SSL encryption:
```
connection-url=jdbc:teradata://example.teradata.com/ENCRYPTDATA=ON
```
For more information on security configuration options, see the Teradata JDBC security documentation.


## Multiple Teradata databases
You can have as many catalogs as you need, so if you have additional Teradata databases, simply add another properties file to etc/catalog with a different name, making sure it ends in .properties. For example, if you name the property file sales.properties, Trino creates a catalog named sales using the configured connector.


## Table properties
Table property usage example:
```
CREATE TABLE person (
  id INTEGER NOT NULL,
  name VARCHAR(100),
  age INTEGER,
  birthday DATE
)
WITH (
  primary_index = ARRAY['id'],
  table_kind = 'SET'
);
```
The following are supported Teradata table properties:

* - Property name
  - Required
  - Description
* - primary_index
  - No
  - The primary index of the table. Can specify multiple columns.
* - table_kind
  - No
  - Specifies whether the table is a SET (default) or MULTISET table.
* - fallback
  - No
  - Enables or disables fallback protection. Values: true, false.
## Type mapping
Because Trino and Teradata each support types that the other does not, this connector modifies some types <type-mapping-overview> when reading or writing data. Data types may not map the same way in both directions between Trino and the data source.


### Teradata to Trino type mapping
The connector maps Teradata types to the corresponding Trino types following this table:


* - Teradata database type
  - Trino type

* - BYTEINT
  - TINYINT

* - SMALLINT
  - SMALLINT

* - INTEGER
  - INTEGER

* - BIGINT
  - BIGINT

* - DECIMAL(p,s)
  - DECIMAL(p,s)

* - NUMERIC(p,s)
  - DECIMAL(p,s)

* - FLOAT
  - DOUBLE

* - REAL
  - REAL

* - DOUBLE PRECISION
  - DOUBLE

* - CHAR(n)
  - CHAR(n)

* - VARCHAR(n)
  - VARCHAR(n)

* - LONG VARCHAR
  - VARCHAR

* - CLOB
  - VARCHAR

* - BYTE(n)
  - VARBINARY

* - VARBYTE(n)
  - VARBINARY

* - BLOB
  - VARBINARY

* - DATE
  - DATE

* - TIME
  - TIME

* - TIME WITH TIME ZONE
  - TIME WITH TIME ZONE

* - TIMESTAMP
  - TIMESTAMP

* - TIMESTAMP WITH TIME ZONE
  - TIMESTAMP WITH TIME ZONE

* - INTERVAL YEAR TO MONTH
  - INTERVAL YEAR TO MONTH

* - INTERVAL DAY TO SECOND
  - INTERVAL DAY TO SECOND

* - JSON
  - JSON

No other types are supported.


### Trino to Teradata type mapping
The connector maps Trino types to the corresponding Teradata types following this table:


* - Trino type
  - Teradata type

* - TINYINT
  - BYTEINT

* - SMALLINT
  - SMALLINT

* - INTEGER
  - INTEGER

* - BIGINT
  - BIGINT

* - DECIMAL(p,s)
  - DECIMAL(p,s)

* - REAL
  - REAL

* - DOUBLE
  - DOUBLE PRECISION

* - CHAR(n)
  - CHAR(n)

* - VARCHAR(n)
  - VARCHAR(n)

* - VARBINARY
  - VARBYTE

* - DATE
  - DATE

* - TIME
  - TIME

* - TIME WITH TIME ZONE
  - TIME WITH TIME ZONE

* - TIMESTAMP
  - TIMESTAMP

* - TIMESTAMP WITH TIME ZONE
  - TIMESTAMP WITH TIME ZONE

* - JSON
  - JSON

No other types are supported.


## Querying Teradata
The Teradata connector provides a schema for every Teradata database. You can see the available Teradata databases by running SHOW SCHEMAS:
```
SHOW SCHEMAS FROM teradata;
```
If you have a Teradata database named sales, you can view the tables in this database by running SHOW TABLES:
```
SHOW TABLES FROM teradata.sales;
```
You can see a list of the columns in the orders table in the sales database using either of the following:
```
DESCRIBE teradata.sales.orders;
SHOW COLUMNS FROM teradata.sales.orders;
```
Finally, you can access the orders table in the sales database:
```
SELECT * FROM teradata.sales.orders;
```
## SQL support
The connector provides read access to data and metadata in the Teradata database. In addition to the [globally available](https://trino.io/docs/current/language/sql-support.html#globally-available-statements) and [read operation](https://trino.io/docs/current/language/sql-support.html#read-operations) statements, the connector supports the following features:

## Procedures
The connector supports the following procedures:


system.flush_metadata_cache()
Flush the metadata cache for all schemas or a specified schema.

Example usage:
```
CALL teradata.system.flush_metadata_cache();
CALL teradata.system.flush_metadata_cache(schema_name => 'sales');
```
## Table functions
The connector provides specific [table functions](https://trino.io/docs/current/functions/table.html) to access Teradata.


query(varchar) -> table
The query function allows you to query the underlying database directly. It requires syntax native to Teradata, because the full query is pushed down and processed in Teradata. This can be useful for accessing native features which are not available in Trino or for improving query performance in situations where running a query natively may be faster.

For example, query the teradata catalog and use Teradata-specific functions:
```
SELECT
  *
FROM
  TABLE(
    teradata.system.query(
      query => 'SELECT
        employee_id,
        RANK() OVER (ORDER BY salary DESC) as salary_rank
      FROM
        hr.employees
      ORDER BY salary_rank'
    )
  );
```
## Performance
The connector includes a number of performance improvements, detailed in the following sections.


### Table statistics
The Teradata connector can use [table and column statistics](https://trino.io/docs/current/optimizer/statistics.html) for [cost based optimizations](https://trino.io/docs/current/optimizer/cost-based-optimizations.html), to improve query processing performance based on the actual data in the data source.
The statistics are collected by Teradata and retrieved by the connector. The table and column statistics are based on Teradata's Data Dictionary views.

You can update statistics in Teradata by running:
```
COLLECT STATISTICS ON table_name;
```
### Pushdown
The connector supports pushdown for a number of operations:


[join-pushdown](https://trino.io/docs/current/optimizer/pushdown.html#join-pushdown)

[limit-pushdown](https://trino.io/docs/current/optimizer/pushdown.html#limit-pushdown)

[topn-pushdown](https://trino.io/docs/current/optimizer/pushdown.html#top-n-pushdown)

[Aggregate pushdown](https://trino.io/docs/current/optimizer/pushdown.html#aggregation-pushdown) for the following functions:

avg

count

max

min

sum

stddev

stddev_pop

stddev_samp

variance

var_pop

var_samp

## Query pass-through
The connector supports query pass-through for Teradata-specific SQL features not available in standard SQL. Use the query table function to execute native Teradata queries directly.
