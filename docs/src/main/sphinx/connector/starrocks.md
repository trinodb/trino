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

- StarRocks 2.5 or higher.
- Network access from the Trino coordinator and workers to StarRocks.
  Port 9030 is the default port.

## Configuration

To configure the StarRocks connector, create a catalog properties file in
`etc/catalog` named, for example, `example.properties`, to mount the StarRocks
catalog. Note: The catalog name will display `example` derived from the property file name.

Here is a standard StarRocks connector properties file.

```text
connector.name=starrocks
connection-url=jdbc:mysql://example.net:9030
connection-user=root
connection-password=secret
```

StarRocks implements the MySQL client protocol. The StarRocks connector uses the MySQL JDBC driver to interact with StarRocks.
The `connection-url` defines the connection information and parameters to pass
to the MySQL JDBC driver. The supported parameters for the URL are
available in the [MySQL Developer Guide](https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-configuration-properties.html).
Some parameters can have adverse effects on the connector behavior or not work with the connector.

The `connection-user` and `connection-password` are typically required and
determine the user credentials for the connection, often a service user. You can
use :doc:`secrets </security/secrets>` to avoid actual values in the catalog
properties files.

### Multiple StarRocks servers

To connect to multiple StarRocks servers, add another properties file to `etc/catalog` with a different name, for example, `starrocks.properties` to add the catalog `starrocks` for the second StarRocks instance. 

```{include} jdbc-common-configurations.fragment
```

```{include} jdbc-domain-compaction-threshold.fragment
```

```{include} jdbc-procedures.fragment
```

```{include} jdbc-case-insensitive-matching.fragment
```

```{include} non-transactional-insert.fragment
```

## Table properties


Table property usage example:

```
    CREATE TABLE example.test (
      id bigint,
      name varchar(128),
      age bigint,
      logdate date
    )
    WITH (
      distribution_desc = ARRAY['id', 'name']
      engine = 'OLAP',
      replication = 4
    );
```

StarRocks supports these [create table properties](https://docs.starrocks.io/en-us/latest/sql-reference/sql-statements/data-definition/CREATE%20TABLE)_.

```{eval-rst}
.. list-table::
    :widths: 30, 10, 100
    :header-rows: 1

    * - Property name
      - Required
      - Description
    * - ``distribution_desc``
      - No
      - ``The distribution key of the table, can chose multi columns as the table distribution key, the FLOAT and DOUBLE type can not be defined as part of the distribution keys.``
    * - ``engine``
      - No
      - ``The engine type of the table``
    * - ``replication_num``
      - No
      - ``The number of replicas in the specified partition. Default number: 3``
```

### distribution_desc

This is a list of columns to be used as the table's distribution key, the column type can not be `FLOAT` or `DOUBLE`. If not specified,
Trino will chose first column exists in the table definition as the distribution key.

(starrocks-type-mapping)=

### Type mapping

StarRocks supports these data types <https://docs.starrocks.io/en-us/latest/sql-reference/sql-statements/data-types>_.

```{eval-rst}
.. list-table::
  :widths: 30, 30, 20
  :header-rows: 1

  * - StarRocks SQL data type name
    - Map to Trino type
    - notes
  * - ``BOOLEAN``
    - ``BOOLEAN``
    - Possible values: ``TRUE`` and ``FALSE``
  * - ``TINYINT``
    - ``TINYINT``
    - Possible values: ``-128``, ``127``, etc.
  * - ``SMALLINT``
    - ``SMALLINT``
    - Possible values: ``-32768``, ``32767``, etc.
  * - ``INT``
    - ``INTEGER``
    - Possible values: ``-2147483648``, ``2147483647``, etc.
  * - ``BIGINT``
    - ``BIGINT``
    - Possible values: ``-9223372036854775808``, ``9223372036854775807``, etc.
  * - ``DECIMAL(p,s)``
    - ``DECIMAL(p,s)``
    - The range of p is [1,38], and the range of S is [0, p]. The default value of s is 0.
  * - ``DOUBLE``
    - ``DOUBLE``
    - Possible values: ``3.14``, ``-10.24``, etc.
  * - ``FLOAT``
    - ``REAL```
    - Possible values: ``3.14``, ``-10.24``, etc.
  * - ``CHAR(n)``
    - ``CHAR(n)``
    - Length n must between 1 and 255, unit is bytes Possible values: ``hello``, ``Trino``, etc.
  * - ``VARCHAR(n)``
    - ``VARCHAR(n)``
    - Length n must between 1 and 1048576, unit is bytes. Possible values: ``hello``, ``Trino``, etc.
  * - ``DATE``
    - ``DATE``
    - ``1972-01-01``, ``2021-07-15``, etc.
```

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

(starrocks-sql-support)=

## SQL support

The connector provides read access and write access to data and metadata in the
StarRocks database. In addition to the :ref:`globally available <sql-globally-available>` and
:ref:`read operation <sql-read-operations>` statements, the connector supports
the following statements:

- {doc}`/sql/insert`
- {doc}`/sql/create-table`
- {doc}`/sql/create-table-as`
- {doc}`/sql/drop-table`
- {doc}`/sql/create-schema`
- {doc}`/sql/drop-schema`

## Performance

The connector includes a number of performance improvements, detailed in the
following sections.

(starrocks-pushdown)=

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

```{include} pushdown-correctness-behavior.fragment
```

```{include} no-pushdown-text-type.fragment
```
