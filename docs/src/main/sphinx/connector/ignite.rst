====================
Ignite connector
====================

The Ignite connector allows querying an `Apache Ignite <https://ignite.apache.org/>`_
database from Trino.Support Ignite version 2.10.0.

Configuration
-------------

The connector can query a Ignite instance. Create a catalog properties file
that specifies the Ignite connector by setting the ``connector.name`` to
``ignite``.

For example, to access an instance as ``myignite``, create the file
``etc/catalog/myignite.properties``. Replace the connection properties as
appropriate for your setup:

.. code-block:: none

    connector.name=ignite
    connection-url=jdbc:ignite:thin://host1:10800/
    connection-user=exampleuser
    connection-password=examplepassword

.. note::

    Currently, Trino only use Ignite JDBC Thin Driver to communicate with Ignite
    database, make sure the ``connection-url`` starts with ``jdbc:ignite:thin``.

Multiple Ignite servers
^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you have multiple Ignite instances you need to configure one catalog for
each instance. To add another catalog:

* Add another properties file to ``etc/catalog``
* Save it with a different name that ends in ``.properties``

For example, if you name the property file ``sales.properties``, Trino uses the
configured connector to create a catalog named ``sales``.

Querying Ignite
-------------------

The Ignite connector default has two schemas: ``sys`` and ``public``. The ``sys``
schema, which contains a number of system views with information about cluster nodes,
user can not create or modify any data among this schema. The ``public`` schema,
which is used by default whenever a schema is not specified.
Run ``SHOW SCHEMAS`` to see the available Ignite catalog ``myignite``::

    SHOW SCHEMAS FROM myignte;

Run ``SHOW TABLES`` to view the tables in the database::

    SHOW TABLES FROM myignite.public;

Run ``DESCRIBE`` or ``SHOW COLUMNS`` to list the columns in the ``person`` table::

    DESCRIBE myignite.public.person;
    SHOW COLUMNS FROM myignite.public.person;

Run ``SELECT`` to access the ``person`` table::

    SELECT * FROM myignite.public.person;

.. note::

    If you used a different name for your catalog properties file, use
    that catalog name instead of ``myignite`` in the above examples.

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
Property Name                   Description
=============================== ==============================================================================================================
``primary_key``                 The primary key of the table, can chose multi columns as the table primary key. Table at least contains one
                                column not in primary key. Required.

``affinity_key``                The affinity_key of the table. Must be one of the ``primary_key``. Optional.

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

``data_region``                 ame of the data region where table entries should be stored. By default, Ignite stores all the data in a
                                default region. Optional.
=============================== ==============================================================================================================

Pushdown
--------

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

Limitations
-----------

The following SQL statements aren't  supported:

* :doc:`/sql/alter-schema`
* :doc:`/sql/alter-view`
* :doc:`/sql/comment`
* :doc:`/sql/create-role`
* :doc:`/sql/create-schema`
* :doc:`/sql/create-table-as`
* :doc:`/sql/create-view`
* :doc:`/sql/drop-schema`
* :doc:`/sql/drop-view`
* :doc:`/sql/drop-role`
* :doc:`/sql/grant`
* :doc:`/sql/revoke`
* :doc:`/sql/show-grants`
* :doc:`/sql/show-roles`
* :doc:`/sql/show-role-grants`
