=================
MariaDB connector
=================

.. raw:: html

  <img src="../_static/img/mariadb.png" class="connector-logo">

The MariaDB connector allows querying and creating tables in an external MariaDB
database.

Requirements
------------

To connect to MariaDB, you need:

* MariaDB version 10.2 or higher.
* Network access from the Trino coordinator and workers to MariaDB. Port
  3306 is the default port.

Configuration
-------------

To configure the MariaDB connector, create a catalog properties file
in ``etc/catalog`` named, for example, ``mariadb.properties``, to
mount the MariaDB connector as the ``mariadb`` catalog.
Create the file with the following contents, replacing the
connection properties as appropriate for your setup:

.. code-block:: text

    connector.name=mariadb
    connection-url=jdbc:mariadb://example.net:3306
    connection-user=root
    connection-password=secret

.. include:: jdbc-common-configurations.fragment

.. include:: jdbc-case-insensitive-matching.fragment

.. include:: non-transactional-insert.fragment

Querying MariaDB
----------------

The MariaDB connector provides a schema for every MariaDB *database*.
You can see the available MariaDB databases by running ``SHOW SCHEMAS``::

    SHOW SCHEMAS FROM mariadb;

If you have a MariaDB database named ``web``, you can view the tables
in this database by running ``SHOW TABLES``::

    SHOW TABLES FROM mariadb.web;

You can see a list of the columns in the ``clicks`` table in the ``web``
database using either of the following::

    DESCRIBE mariadb.web.clicks;
    SHOW COLUMNS FROM mariadb.web.clicks;

Finally, you can access the ``clicks`` table in the ``web`` database::

    SELECT * FROM mariadb.web.clicks;

If you used a different name for your catalog properties file, use
that catalog name instead of ``mariadb`` in the above examples.

.. mariadb-type-mapping:

Type mapping
------------

Trino supports the following MariaDB data types:

==================================  =============================== =============================================================================================================
MariaDB type                        Trino type                      Notes
==================================  =============================== =============================================================================================================
``boolean``                         ``tinyint``
``tinyint``                         ``tinyint``
``smallint``                        ``smallint``
``int``                             ``integer``
``bigint``                          ``bigint``
``tinyint unsigned``                ``smallint``
``smallint unsigned``               ``integer``
``mediumint unsigned``              ``integer``
``integer unsigned``                ``bigint``
``bigint unsigned``                 ``decimal(20,0)``
``float``                           ``real``
``double``                          ``double``
``decimal(p,s)``                    ``decimal(p,s)``
``char(n)``                         ``char(n)``
``tinytext``                        ``varchar(255)``
``text``                            ``varchar(65535)``
``mediumtext``                      ``varchar(16777215)``
``longtext``                        ``varchar``
``varchar(n)``                      ``varchar(n)``
``tinyblob``                        ``varbinary``
``blob``                            ``varbinary``
``mediumblob``                      ``varbinary``
``longblob``                        ``varbinary``
``varbinary(n)``                    ``varbinary``
``date``                            ``date``
``time(n)``                         ``time(n)``
``timestamp(n)``                    ``timestamp(n)``                MariaDB stores the current timestamp by default.
                                                                    Please enable `explicit_defaults_for_timestamp <https://mariadb.com/docs/reference/mdb/system-variables/explicit_defaults_for_timestamp/>`_
                                                                    to avoid implicit default values.
==================================  =============================== =============================================================================================================

Complete list of `MariaDB data types
<https://mariadb.com/kb/en/data-types/>`_.


.. include:: jdbc-type-mapping.fragment

.. _mariadb-sql-support:

SQL support
-----------

The connector provides read access and write access to data and metadata in
a MariaDB database.  In addition to the :ref:`globally available
<sql-globally-available>` and :ref:`read operation <sql-read-operations>`
statements, the connector supports the following features:

* :doc:`/sql/insert`
* :doc:`/sql/delete`
* :doc:`/sql/truncate`
* :doc:`/sql/create-table`
* :doc:`/sql/create-table-as`
* :doc:`/sql/drop-table`
* :doc:`/sql/alter-table`
* :doc:`/sql/create-schema`
* :doc:`/sql/drop-schema`

.. include:: sql-delete-limitation.fragment

Performance
-----------

The connector includes a number of performance improvements, detailed in the
following sections.

.. _mariadb-pushdown:

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
* :func:`stddev`
* :func:`stddev_pop`
* :func:`stddev_samp`
* :func:`variance`
* :func:`var_pop`
* :func:`var_samp`

.. include:: no-pushdown-text-type.fragment
