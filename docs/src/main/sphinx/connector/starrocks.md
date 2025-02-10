---
myst:
  substitutions:
    default_domain_compaction_threshold: '`32`'
---

# StarRocks connector

```{raw} html
<img src="../_static/img/starrocks.png" class="connector-logo">
```

The StarRocks connector allows querying and creating tables in an external
[StarRocks](https://www.starrocks.io/) instance, enabling you to join StarRocks data with any other [connector that Trino supports](/connector).

## Requirements

To connect to StarRocks, you need:

- StarRocks 3.1 or higher.
- Network access from the Trino coordinator and workers to StarRocks FE&BE service.
  Port 9030 is the metadata default port.
  Port 8030 is the HTTP query plan default port.

## Configuration

To configure the StarRocks connector, create a catalog properties file in
`etc/catalog` named, for example, `example.properties`, to mount the StarRocks
catalog. Note: The catalog name will display `example` derived from the property file name.

Here is a standard StarRocks connector properties file.

```text
connector.name=starrocks
jdbc-url=jdbc:mysql://fe:9030
scan-url=fe:8030
username=root
password=your_password
```

StarRocks implements the Flink client pulling protocol.
The StarRocks connector uses the MySQL JDBC driver to interact with StarRocks to fetch metadata.
Using metadata to generate query plan and sending to BE instance to pulling data.
The `jdbc-url` defines the connection information and parameters to pass
to the MySQL JDBC driver. The supported parameters for the URL are
available in the [MySQL Developer Guide](https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-configuration-properties.html).
Some parameters can have adverse effects on the connector behavior or not work with the connector.

The `username` and `password` are typically required and
determine the user credentials for the connection, often a service user. You can
use :doc:`secrets </security/secrets>` to avoid actual values in the catalog
properties files.

:::{list-table} StarRocks connector properties
:header-rows: 1

*
    - Property
    - Description
    - Default
*
    - ``starrocks.dynamic_filtering_wait_timeout``
    - The timeout for waiting for dynamic filtering to finish.
    - ``10s``
*
    - ``starrocks.tuple_domain_limit``
    - The maximum number of tuple domain to be sent to StarRocks FE. When the number of tuple domain becomes larger than this value, the connector will simplify the tuple domain.
    - ``1000``

:::

### Multiple StarRocks servers

To connect to multiple StarRocks servers, add another properties file to `etc/catalog` with a different name, for example, `starrocks.properties` to add the catalog `starrocks` for
the second StarRocks instance.

### Multiple StarRocks instance

To connect to multiple StarRocks instance, you can add different address in `scan-url` properties.

```text
connector.name=starrocks
jdbc-url=jdbc:mysql://fe:9030
scan-url=fe:8030,fe2:8030,fe3:8030
username=root
password=your_password
```

### Type mapping

StarRocks supports these data types <https://docs.starrocks.io/en-us/latest/sql-reference/sql-statements/data-types>

:::{list-table}
:widths: 30, 30, 20
:header-rows: 1

*
    - StarRocks SQL data type name
    - Map to Trino type
    - notes
*
    - ``BOOLEAN``
    - ``BOOLEAN``
    - Possible values: ``True`` and ``False``
*
    - ``TINYINT``
    - ``TINYINT``
    - Possible values: ``-128``, ``127``, etc.
*
    - ``SMALLINT``
    - ``SMALLINT``
    - Possible values: ``-32768``, ``32767``, etc.
*
    - ``INT``
    - ``INTEGER``
    - Possible values: ``-2147483648``, ``2147483647``, etc.
*
    - ``BIGINT``
    - ``BIGINT``
    - Possible values: ``-9223372036854775808``, ``9223372036854775807``, etc.
*
    - ``DECIMAL(p,s)``
    - ``DECIMAL(p,s)``
    - The range of p is [1,38], and the range of S is [0, p]. The default value of s is 0.
*
    - ``DOUBLE``
    - ``DOUBLE``
    - Possible values: ``3.14``, ``-10.24``, etc.
*
    - ``FLOAT``
    - ``REAL``
    - Possible values: ``3.14``, ``-10.24``, etc.
*
    - ``CHAR(n)``
    - ``CHAR(n)``
    - Length n must between 1 and 255, unit is bytes Possible values: ``hello``, ``Trino``, etc.
*
    - ``VARCHAR(n)``
    - ``VARCHAR(n)``
    - Length n must between 1 and 1048576, unit is bytes. Possible values: ``hello``, ``Trino``, etc.
*
    - ``DATE``
    - ``DATE``
    - ``1972-01-01``, ``2021-07-15``, etc.
*
    - ``DATETIME``
    - ``TIMESTAMP``
    - Possible values: ``2021-07-15 12:00:00``, ``2021-07-15 12:00:00.123``, etc.
*
    - ``LARGEINT``
    - ``DECIMAL``
    - Possible values: ``-2^(127)-1``, ``2^(127)-1``, etc.
*
    - ``STRUCT``
    - ``ROW``
    - Possible values: ``{"a": 1, "b": 2}``
*
    - ``ARRAY``
    - ``ARRAY``
    - Possible values: ``[1, 2, 3]``
*
    - ``MAP``
    - ``MAP``
    - Possible values: ``{"a": 1, "b": 2}``
*
    - ``JSON``
    - ``JSON``
    - Possible values: ``{"a": 1, "b": 2}``

:::

No other types are supported.

## Querying StarRocks

The StarRocks connector provides a schema for every StarRocks database.
You can see the available StarRocks databases by running `SHOW SCHEMAS`:

```
SHOW SCHEMAS FROM example;
```

If you have a StarRocks database named `web`, you can view the tables
in this database by running `SHOW TABLES`:

```
SHOW TABLES FROM example.web;
```

You can see a list of the columns in the `clicks` table in the `web` database
using either of the following:

```
DESCRIBE example.web.clicks;
SHOW COLUMNS FROM example.web.clicks;
```

Finally, you can access the `clicks` table in the `web` database::

```
SELECT * FROM example.web.clicks;
```

If you used a different name for your catalog properties file, use
that catalog name instead of `example` in the above examples.

## SQL support

The connector ONLY provides read access to data and metadata in the
StarRocks database. For {ref}`read operation <sql-read-operations>` statements, the connector supports
the following statements:

## Performance

The connector includes a number of performance improvements, detailed in the
following sections.