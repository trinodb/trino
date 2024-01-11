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

% snowflake-type-mapping:

## Type mapping

Trino supports the following Snowflake data types:

| Snowflake Type | Trino Type     |
| -------------- | -------------- |
| `boolean`      | `boolean`      |
| `tinyint`      | `bigint`       |
| `smallint`     | `bigint`       |
| `byteint`      | `bigint`       |
| `int`          | `bigint`       |
| `integer`      | `bigint`       |
| `bigint`       | `bigint`       |
| `float`        | `real`         |
| `real`         | `real`         |
| `double`       | `double`       |
| `decimal`      | `decimal(P,S)` |
| `varchar(n)`   | `varchar(n)`   |
| `char(n)`      | `varchar(n)`   |
| `binary(n)`    | `varbinary`    |
| `varbinary`    | `varbinary`    |
| `date`         | `date`         |
| `time`         | `time`         |
| `timestampntz` | `timestamp`    |

Complete list of [Snowflake data types](https://docs.snowflake.com/en/sql-reference/intro-summary-data-types.html).

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
