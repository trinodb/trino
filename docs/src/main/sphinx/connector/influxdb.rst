=======================
InfluxDB connector
=======================

The InfluxDB Connector allows access to `InfluxDB <https://www.influxdata.com/>`_ data from Trino.
This document describes how to setup the InfluxDB Connector to run SQL queries against InfluxDB.

Requirements
------------

To connect to InfluxDB, you need:

* InfluxDB v1.8.0 or higher.(v2.x not currently supported)
* Network access from the Trino coordinator and workers to InfluxDB.
  Port 8086 is the default port.

Configuration
-------------

To configure the InfluxDB connector, create a catalog properties file
``etc/catalog/example.properties`` with the following contents,
replacing the properties as appropriate:

.. code-block:: properties

    connector.name=influxdb
    influx.endpoint=http://localhost:8086
    influx.username=username
    influx.password=password

Configuration properties
^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table:: The following configuration properties are available
    :widths: 35, 55, 10
    :header-rows: 1

    * - Property name
      - Description
      - Default
    * - ``influx.endpoint``
      - Endpoint of the InfluxDB server to connect to. This property is required.
      -
    * - ``influx.username``
      - User name to use to connect to InfluxDB.
      -
    * - ``influx.password``
      - Password to use to connect to InfluxDB.
      -
    * - ``influx.connect-timeout``
      - The socket connect timeout to InfluxDB server.
      - ``10s``
    * - ``influx.write-timeout``
      - The socket write timeout to InfluxDB server.
      - ``10s``
    * - ``influx.read-timeout``
      - The socket read timeout to InfluxDB server.
      - ``60s``

.. _influxdb-type-mapping:

Type mapping
------------

Because Trino and InfluxDB each support types that the other does not, this
connector :ref:`maps some types <type-mapping-overview>` when reading data.


InfluxDB type to Trino type mapping
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The connector maps InfluxDB types to the corresponding Trino types
according to the following table:

.. list-table:: InfluxDB type to Trino type mapping
  :widths: 30, 30, 50
  :header-rows: 1

  * - InfluxDB type
    - Trino type
    - Notes
  * - ``TIMESTAMP``
    - ``TIMESTAMP``
    -  ``Timestamp`` key ``time`` is unix nanosecond timestamp in influxdb. see `data types <https://docs.influxdata.com/influxdb/v1.8/write_protocols/line_protocol_reference/#data-types>`_.
  * - ``BOOLEAN``
    - ``BOOLEAN``
    -
  * - ``FLOAT``
    - ``DOUBLE``
    - ``FLOAT`` is 64-bit floating-point numbers in influxdb. see `data types <https://docs.influxdata.com/influxdb/v1.8/write_protocols/line_protocol_reference/#data-types>`_.
  * - ``INTEGER``
    - ``BIGINT``
    - ``INTEGER`` is 64-bit integers in influxdb. see `data types <https://docs.influxdata.com/influxdb/v1.8/write_protocols/line_protocol_reference/#data-types>`_.
  * - ``STRING``
    - ``VARCHAR``
    -

No other types are supported.


SQL support
-----------

The connector provides :ref:`globally available <sql-globally-available>` and
:ref:`read operation <sql-read-operations>` statements to access data and
metadata in the InfluxDB catalog.

.. _influxdb-pushdown:

Pushdown support
^^^^^^^^^^^^^^^^

The connector supports pushdown for a number of operations:

* :ref:`limit-pushdown`

* :ref:`projection-pushdown`

* :ref:`predicate-pushdown`

But there are some special limitations for predicate-pushdown:

To understand easily, think a measurement "student" in influxdb

.. code-block:: text

    SHOW TAG KEYS:
    tagKey
    ------
    grade
    class

    SHOW FIELD KEYS:
    fieldKey fieldType
    -------- ---------
    name     string
    age      integer
    score    float


Predicate pushdown of keys of ``STRING``, ``BOOLEAN``, ``INTEGER`` and ``FLOAT`` types are supported:

For ``TIMESTAMP`` key, supports equality predicates ``=``, range predicates, such as ``>``, ``<``.

For keys of ``STRING`` or ``BOOLEAN`` type (both tag set and field keys in InfluxDB), supports equality predicates ``=`` only.

For keys of ``INTEGER`` or ``FLOAT`` type (field keys in InfluxDB), supports
equality predicates ``=``, inequality predicates ``!=``, and range predicates, such as ``>``, ``<``, or ``BETWEEN``.

.. note::
    Decimal integer literals ``int_lit = ("1"…"9"){digit}`` pushdown works, hexadecimal and octal literals are not currently supported.
    Floating-point literals ``float_lit = int_lit"."int_lit`` pushdown works, exponents are not currently supported.

.. code-block:: sql

    -- Not pushed down
    SELECT * FROM student WHERE name > 'CLOUD';
    SELECT * FROM student WHERE score <> 1.0E2;
    -- Pushed down
    SELECT * FROM student WHERE name = 'CLOUD';
    SELECT * FROM student WHERE name != 'CLOUD';
    SELECT * FROM student WHERE age > 12 and age < 14;
    SELECT * FROM student WHERE score > 80.0 and score != 100;
    SELECT * FROM student WHERE time >= timestamp '2020-01-01 00:00:00' and time <= timestamp '2020-01-01 23:59:59';
