====================
ClickHouse connector
====================

.. raw:: html

  <img src="../_static/img/clickhouse.png" class="connector-logo">

The ClickHouse connector allows querying tables in an external
`Yandex ClickHouse <https://clickhouse.tech/>`_ server. This can be used to
query data in the databases on that server, or combine it with other data
from different catalogs accessing ClickHouse or any other supported data source.

Requirements
------------

To connect to a ClickHouse server, you need:

* ClickHouse (version 21.3 or higher) or Altinity (version 20.8 or higher).
* Network access from the Trino coordinator and workers to the ClickHouse
  server. Port 8123 is the default port.

Configuration
-------------

The connector can query a ClickHouse server. Create a catalog properties file
that specifies the ClickHouse connector by setting the ``connector.name`` to
``clickhouse``.

For example, to access a server as ``clickhouse``, create the file
``etc/catalog/clickhouse.properties``. Replace the connection properties as
appropriate for your setup:

.. code-block:: none

    connector.name=clickhouse
    connection-url=jdbc:clickhouse://host1:8123/
    connection-user=exampleuser
    connection-password=examplepassword

.. note::

    Trino uses the new ClickHouse driver(``com.clickhouse.jdbc.ClickHouseDriver``)
    by default, but the new driver only supports ClickHouse server with version >= 20.7.

    For compatibility with ClickHouse server versions < 20.7,
    you can temporarily continue to use the old ClickHouse driver(``ru.yandex.clickhouse.ClickHouseDriver``)
    by adding the following catalog property: ``clickhouse.legacy-driver=true``.

.. _clickhouse-tls:

Connection security
^^^^^^^^^^^^^^^^^^^

If you have TLS configured with a globally-trusted certificate installed on your
data source, you can enable TLS between your cluster and the data
source by appending a parameter to the JDBC connection string set in the
``connection-url`` catalog configuration property.

For example, with version 2.6.4 of the ClickHouse JDBC driver, enable TLS by
appending the ``ssl=true`` parameter to the ``connection-url`` configuration
property:

.. code-block:: properties

  connection-url=jdbc:clickhouse://host1:8123/?ssl=true

For more information on TLS configuration options, see the `Clickhouse JDBC
driver documentation <https://clickhouse.com/docs/en/interfaces/jdbc/>`_

Multiple ClickHouse servers
^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you have multiple ClickHouse servers you need to configure one
catalog for each server. To add another catalog:

* Add another properties file to ``etc/catalog``
* Save it with a different name that ends in ``.properties``

For example, if you name the property file ``sales.properties``, Trino uses the
configured connector to create a catalog named ``sales``.

.. include:: jdbc-common-configurations.fragment

.. include:: jdbc-procedures.fragment

.. include:: jdbc-case-insensitive-matching.fragment

.. include:: non-transactional-insert.fragment

Querying ClickHouse
-------------------

The ClickHouse connector provides a schema for every ClickHouse *database*.
run ``SHOW SCHEMAS`` to see the available ClickHouse databases::

    SHOW SCHEMAS FROM myclickhouse;

If you have a ClickHouse database named ``web``, run ``SHOW TABLES`` to view the
tables in this database::

    SHOW TABLES FROM myclickhouse.web;

Run ``DESCRIBE`` or ``SHOW COLUMNS`` to list the columns in the ``clicks`` table
in the ``web`` databases::

    DESCRIBE myclickhouse.web.clicks;
    SHOW COLUMNS FROM clickhouse.web.clicks;

Run ``SELECT`` to access the ``clicks`` table in the ``web`` database::

    SELECT * FROM myclickhouse.web.clicks;

.. note::

    If you used a different name for your catalog properties file, use
    that catalog name instead of ``myclickhouse`` in the above examples.

Table properties
----------------

Table property usage example::

    CREATE TABLE default.trino_ck (
      id int NOT NULL,
      birthday DATE NOT NULL,
      name VARCHAR,
      age BIGINT,
      logdate DATE NOT NULL
    )
    WITH (
      engine = 'MergeTree',
      order_by = ARRAY['id', 'birthday'],
      partition_by = ARRAY['toYYYYMM(logdate)'],
      primary_key = ARRAY['id'],
      sample_by = 'id'
    );

The following are supported ClickHouse table properties from `<https://clickhouse.tech/docs/en/engines/table-engines/mergetree-family/mergetree/>`_

=========================== ================ ==============================================================================================================
Property Name               Default Value    Description
=========================== ================ ==============================================================================================================
``engine``                  ``Log``          Name and parameters of the engine.

``order_by``                (none)           Array of columns or expressions to concatenate to create the sorting key. Required if ``engine`` is ``MergeTree``.

``partition_by``            (none)           Array of columns or expressions to use as nested partition keys. Optional.

``primary_key``             (none)           Array of columns or expressions to concatenate to create the primary key. Optional.

``sample_by``               (none)           An expression to use for `sampling <https://clickhouse.tech/docs/en/sql-reference/statements/select/sample/>`_.
                                             Optional.

=========================== ================ ==============================================================================================================

Currently the connector only supports ``Log`` and ``MergeTree`` table engines
in create table statement. ``ReplicatedMergeTree`` engine is not yet supported.

.. _clickhouse-type-mapping:

Type mapping
------------

The data type mappings are as follows:

================= ================= ===================================================================================================
ClickHouse        Trino             Notes
================= ================= ===================================================================================================
``Int8``          ``TINYINT``       ``TINYINT``, ``BOOL``, ``BOOLEAN`` and ``INT1`` are aliases of ``Int8``
``Int16``         ``SMALLINT``      ``SMALLINT`` and ``INT2`` are aliases of ``Int16``
``Int32``         ``INTEGER``       ``INT``, ``INT4`` and ``INTEGER`` are aliases of ``Int32``
``Int64``         ``BIGINT``        ``BIGINT`` is an alias of ``Int64``
``UInt8``         ``SMALLINT``
``UInt16``        ``INTEGER``
``UInt32``        ``BIGINT``
``UInt64``        ``DECIMAL(20,0)``
``Float32``       ``REAL``          ``FLOAT`` is an alias of ``Float32``
``Float64``       ``DOUBLE``        ``DOUBLE`` is an alias of ``Float64``
``Decimal``       ``DECIMAL``
``FixedString``   ``VARBINARY``     Enabling ``clickhouse.map-string-as-varchar`` config property changes the mapping to ``VARCHAR``
``String``        ``VARBINARY``     Enabling ``clickhouse.map-string-as-varchar`` config property changes the mapping to ``VARCHAR``
``Date``          ``DATE``
``DateTime``      ``TIMESTAMP``
``IPv4``          ``IPADDRESS``
``IPv6``          ``IPADDRESS``
``Enum8``         ``VARCHAR``
``Enum16``        ``VARCHAR``
``UUID``          ``UUID``
================= ================= ===================================================================================================

.. include:: jdbc-type-mapping.fragment

.. _clickhouse-pushdown:

Pushdown
--------

The connector supports pushdown for a number of operations:

* :ref:`limit-pushdown`

:ref:`Aggregate pushdown <aggregation-pushdown>` for the following functions:

* :func:`avg`
* :func:`count`
* :func:`max`
* :func:`min`
* :func:`sum`

.. include:: no-pushdown-text-type.fragment

.. _clickhouse-sql-support:

SQL support
-----------

The connector provides read and write access to data and metadata in
a ClickHouse catalog. In addition to the :ref:`globally available
<sql-globally-available>` and :ref:`read operation <sql-read-operations>`
statements, the connector supports the following features:

* :doc:`/sql/insert`
* :doc:`/sql/truncate`
* :ref:`sql-schema-table-management`

.. include:: alter-schema-limitation.fragment
