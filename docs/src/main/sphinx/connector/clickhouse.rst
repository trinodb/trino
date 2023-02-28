====================
ClickHouse connector
====================

.. raw:: html

  <img src="../_static/img/clickhouse.png" class="connector-logo">

The ClickHouse connector allows querying tables in an external
`ClickHouse <https://clickhouse.com/>`_ server. This can be used to
query data in the databases on that server, or combine it with other data
from different catalogs accessing ClickHouse or any other supported data source.

Requirements
------------

To connect to a ClickHouse server, you need:

* ClickHouse (version 21.8 or higher) or Altinity (version 20.8 or higher).
* Network access from the Trino coordinator and workers to the ClickHouse
  server. Port 8123 is the default port.

Configuration
-------------

The connector can query a ClickHouse server. Create a catalog properties file
that specifies the ClickHouse connector by setting the ``connector.name`` to
``clickhouse``.

For example, create the file ``etc/catalog/example.properties``. Replace the
connection properties as appropriate for your setup:

.. code-block:: none

    connector.name=clickhouse
    connection-url=jdbc:clickhouse://host1:8123/
    connection-user=exampleuser
    connection-password=examplepassword

The ``connection-url`` defines the connection information and parameters to pass
to the ClickHouse JDBC driver. The supported parameters for the URL are
available in the `ClickHouse JDBC driver configuration
<https://github.com/ClickHouse/clickhouse-jdbc/tree/master/clickhouse-jdbc#Configuration>`_.

The ``connection-user`` and ``connection-password`` are typically required and
determine the user credentials for the connection, often a service user. You can
use :doc:`secrets </security/secrets>` to avoid actual values in the catalog
properties files.

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

  connection-url=jdbc:clickhouse://host1:8443/?ssl=true

For more information on TLS configuration options, see the `Clickhouse JDBC
driver documentation <https://clickhouse.com/docs/en/interfaces/jdbc/>`_

.. include:: jdbc-authentication.fragment

Multiple ClickHouse servers
^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you have multiple ClickHouse servers you need to configure one
catalog for each server. To add another catalog:

* Add another properties file to ``etc/catalog``
* Save it with a different name that ends in ``.properties``

For example, if you name the property file ``sales.properties``, Trino uses the
configured connector to create a catalog named ``sales``.

.. include:: jdbc-common-configurations.fragment

.. |default_domain_compaction_threshold| replace:: ``1000``
.. include:: jdbc-domain-compaction-threshold.fragment

.. include:: jdbc-procedures.fragment

.. include:: jdbc-case-insensitive-matching.fragment

.. include:: non-transactional-insert.fragment

Querying ClickHouse
-------------------

The ClickHouse connector provides a schema for every ClickHouse *database*.
Run ``SHOW SCHEMAS`` to see the available ClickHouse databases::

    SHOW SCHEMAS FROM example;

If you have a ClickHouse database named ``web``, run ``SHOW TABLES`` to view the
tables in this database::

    SHOW TABLES FROM example.web;

Run ``DESCRIBE`` or ``SHOW COLUMNS`` to list the columns in the ``clicks`` table
in the ``web`` databases::

    DESCRIBE example.web.clicks;
    SHOW COLUMNS FROM example.web.clicks;

Run ``SELECT`` to access the ``clicks`` table in the ``web`` database::

    SELECT * FROM example.web.clicks;

.. note::

    If you used a different name for your catalog properties file, use
    that catalog name instead of ``example`` in the above examples.

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
Property name               Default value    Description
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

Because Trino and ClickHouse each support types that the other does not, this
connector :ref:`modifies some types <type-mapping-overview>` when reading or
writing data. Data types may not map the same way in both directions between
Trino and the data source. Refer to the following sections for type mapping in
each direction.

ClickHouse type to Trino type mapping
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The connector maps ClickHouse types to the corresponding Trino types according
to the following table:

.. list-table:: ClickHouse type to Trino type mapping
  :widths: 30, 25, 50
  :header-rows: 1

  * - ClickHouse type
    - Trino type
    - Notes
  * - ``Int8``
    - ``TINYINT``
    - ``TINYINT``, ``BOOL``, ``BOOLEAN``, and ``INT1`` are aliases of ``Int8``
  * - ``Int16``
    - ``SMALLINT``
    -  ``SMALLINT`` and ``INT2`` are aliases of ``Int16``
  * - ``Int32``
    - ``INTEGER``
    - ``INT``, ``INT4``, and ``INTEGER`` are aliases of ``Int32``
  * - ``Int64``
    - ``BIGINT``
    - ``BIGINT`` is an alias of ``Int64``
  * - ``UInt8``
    - ``SMALLINT``
    -
  * - ``UInt16``
    - ``INTEGER``
    -
  * - ``UInt32``
    - ``BIGINT``
    -
  * - ``UInt64``
    - ``DECIMAL(20,0)``
    -
  * - ``Float32``
    - ``REAL``
    - ``FLOAT`` is an alias of ``Float32``
  * - ``Float64``
    - ``DOUBLE``
    - ``DOUBLE`` is an alias of ``Float64``
  * - ``Decimal``
    - ``DECIMAL``
    -
  * - ``FixedString``
    - ``VARBINARY``
    - Enabling ``clickhouse.map-string-as-varchar`` config property changes the
      mapping to ``VARCHAR``
  * - ``String``
    - ``VARBINARY``
    - Enabling ``clickhouse.map-string-as-varchar`` config property changes the
      mapping to ``VARCHAR``
  * - ``Date``
    - ``DATE``
    -
  * - ``DateTime[(timezone)]``
    - ``TIMESTAMP(0) [WITH TIME ZONE]``
    -
  * - ``IPv4``
    - ``IPADDRESS``
    -
  * - ``IPv6``
    - ``IPADDRESS``
    -
  * - ``Enum8``
    - ``VARCHAR``
    -
  * - ``Enum16``
    - ``VARCHAR``
    -
  * - ``UUID``
    - ``UUID``
    -

No other types are supported.

Trino type to ClickHouse type mapping
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The connector maps Trino types to the corresponding ClickHouse types according
to the following table:

.. list-table:: Trino type to ClickHouse type mapping
  :widths: 30, 25, 50
  :header-rows: 1

  * - Trino type
    - ClickHouse type
    - Notes
  * - ``BOOLEAN``
    - ``UInt8``
    -
  * - ``TINYINT``
    - ``Int8``
    - ``TINYINT``, ``BOOL``, ``BOOLEAN``, and ``INT1`` are aliases of ``Int8``
  * - ``SMALLINT``
    - ``Int16``
    -  ``SMALLINT`` and ``INT2`` are aliases of ``Int16``
  * - ``INTEGER``
    - ``Int32``
    - ``INT``, ``INT4``, and ``INTEGER`` are aliases of ``Int32``
  * - ``BIGINT``
    - ``Int64``
    - ``BIGINT`` is an alias of ``Int64``
  * - ``REAL``
    - ``Float32``
    - ``FLOAT`` is an alias of ``Float32``
  * - ``DOUBLE``
    - ``Float64``
    - ``DOUBLE`` is an alias of ``Float64``
  * - ``DECIMAL(p,s)``
    - ``Decimal(p,s)``
    -
  * - ``VARCHAR``
    - ``String``
    -
  * - ``CHAR``
    - ``String``
    -
  * - ``VARBINARY``
    - ``String``
    - Enabling ``clickhouse.map-string-as-varchar`` config property changes the
      mapping to ``VARCHAR``
  * - ``DATE``
    - ``Date``
    -
  * - ``TIMESTAMP(0)``
    - ``DateTime``
    -
  * - ``UUID``
    - ``UUID``
    -

No other types are supported.

.. include:: jdbc-type-mapping.fragment

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

Performance
-----------

The connector includes a number of performance improvements, detailed in the
following sections.

.. _clickhouse-pushdown:

Pushdown
^^^^^^^^

The connector supports pushdown for a number of operations:

* :ref:`limit-pushdown`

:ref:`Aggregate pushdown <aggregation-pushdown>` for the following functions:

* :func:`avg`
* :func:`count`
* :func:`max`
* :func:`min`
* :func:`sum`

.. include:: pushdown-correctness-behavior.fragment

.. include:: no-pushdown-text-type.fragment
