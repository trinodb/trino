================
Ignite connector
================

.. raw:: html

  <img src="../_static/img/ignite.png" class="connector-logo">

The Ignite connector allows querying an `Apache Ignite <https://ignite.apache.org/>`_
database from Trino.

Requirements
------------

To connect to a Ignite server, you need:

* Ignite version 2.8.0 or latter
* Network access from the Trino coordinator and workers to the Ignite
  server. Port 10800 is the default port.

Configuration
-------------

The Ignite connector default has two schemas: ``sys`` and ``public``. The ``sys``
schema, which contains a number of system views with information about cluster nodes,
user can not create or modify any data among this schema. The ``public`` schema,
which is used by default whenever a schema is not specified.

The connector can query a Ignite instance. Create a catalog properties file
that specifies the Ignite connector by setting the ``connector.name`` to
``ignite``.If you have multiple Ignite instances you need to configure one catalog for
each instance.

For example, to access an instance as ``myignite``, create the file
``etc/catalog/myignite.properties``. Replace the connection properties as
appropriate for your setup:

.. code-block:: none

    connector.name=ignite
    connection-url=jdbc:ignite:thin://host1:10800/
    connection-user=exampleuser
    connection-password=examplepassword

.. note::

    The connector can only use the thin Ignite JDBC driver. Make sure the
    ``connection-url`` starts with ``jdbc:ignite:thin``.

Multiple Ignite servers
^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you have multiple Ignite servers you need to configure one
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

Table properties
----------------

Table property usage example::

    CREATE TABLE public.person (
      id bigint NOT NULL,
      birthday DATE NOT NULL,
      name VARCHAR(26),
      age BIGINT,
      logdate DATE
    )
    WITH (
      primary_key = ARRAY['id', 'birthday'],
      affinity_key = 'birthday',
      template = 'REPLICATED',
      write_synchronization_mode = 'FULL_ASYNC'
    );

The following are supported Ignite table properties from `<https://ignite.apache.org/docs/latest/sql-reference/ddl>`_

=============================== ==============================================================================================================
Property name                   Description
=============================== ==============================================================================================================
``primary_key``                 The primary key of the table, can chose multi columns as the table primary key. Table at least contains one
                                column not in primary key. Required.

``affinity_key``                 The affinity_key of the table. Must be one of the ``primary_key``. Optional.

``template``                    The storage template about the table. Available values are ``REPLICATED`` and ``PARTITIONED`. Default value is
                                ``PARTITIONED``. Optional.

``backups``                     The replication numbers for the table, must greater than 0. Default value is 1. Optional.

``cache_group``                 Specifies the group name the underlying cache belongs to. Default value is prefix ``SQL_PUBLIC_`` append with
                                UPPERCASE of the table name. Optional.

``cache_name``                  The name of the underlying cache created by the command. Default value is prefix ``SQL_PUBLIC_`` append with
                                UPPERCASE of the table name. Optional.

``write_synchronization_mode``  Sets the write synchronization mode for the underlying cache. If neither this nor the ``TEMPLATE`` parameter
                                is set, then the cache is created with ``FULL_SYNC`` mode enabled. Available values are ``PRIMARY_SYNC``,
                                ``FULL_SYNC`` and ``FULL_ASYNC``. Optional.

``data_region``                 Name of the data region where table entries should be stored. By default, Ignite stores all the data in a
                                default region. Optional.
=============================== ==============================================================================================================

.. _ignite-type-mapping:

Type mapping
------------

The following are supported Ignite SQL data types from `<https://ignite.apache.org/docs/latest/sql-reference/data-types>`_

.. list-table::
    :widths: 30, 30, 30, 20
    :header-rows: 1

    * - Ignite SQL data type name
      - Map to Trino type
      - Map to Java/JDBC type
      - Possible values
    * - ``BOOLEAN``
      - ``BOOLEAN``
      - ``java.lang.Boolean``
      - ``TRUE`` and ``FALSE``
    * - ``BIGINT``
      - ``BIGINT``
      - ``java.lang.Long``
      - ``-9223372036854775808``, ``9223372036854775807``, etc.
    * - ``DECIMAL``
      - ``DECIMAL``
      - ``java.math.BigDecimal``
      - Data type with fixed precision and scale
    * - ``DOUBLE``
      - ``DOUBLE``
      - ``java.lang.Double``.
      - ``3.14``, ``-10.24``, etc.
    * - ``INT``
      - ``INT``
      - ``java.lang.Integer``.
      - ``-2147483648``, ``2147483647``, etc.
    * - ``REAL``
      - ``REAL```
      - ``java.lang.Float``.
      - ``3.14``, ``-10.24``, etc.
    * - ``SMALLINT``
      - ``SMALLINT``
      - ``java.lang.Short``.
      - ``-32768``, ``32767``, etc.
    * - ``TINYINT``
      - ``TINYINT``
      - ``java.lang.Byte``.
      - ``-128``, ``127``, etc.
    * - ``CHAR``
      - ``CHAR``
      - ``java.lang.String``.
      - ``hello``, ``Trino``, etc.
    * - ``VARCHAR``
      - ``VARCHAR``
      - ``java.lang.String``.
      - ``hello``, ``Trino``, etc.
    * - ``DATE``
      - ``DATE``
      - ``java.sql.Date``.
      - ``1972-01-01``, ``2021-07-15``, etc.
    * - ``BINARY``
      - ``VARBINARY``
      - ``byte[]``.
      - Represents a byte array.

.. _ignite-sql-support:

SQL support
-----------
The connector provides read access and write access to data and metadata in
Ignite.  In addition to the :ref:`globally available
<sql-globally-available>` and :ref:`read operation <sql-read-operations>`
statements, the connector supports the following features:

* :doc:`/sql/insert`
* :ref:`sql-schema-table-management`

.. include:: alter-table-limitation.fragment

.. _ignite-pushdown:

Pushdown
^^^^^^^^

The connector supports pushdown for a number of operations:

* :ref:`join-pushdown`
* :ref:`limit-pushdown`
* :ref:`topn-pushdown`

:ref:`Aggregate pushdown <aggregation-pushdown>` for the following functions:

* :func:`avg`
* :func:`count`
* :func:`max`
* :func:`min`
* :func:`sum`

.. include:: no-pushdown-text-type.fragment
