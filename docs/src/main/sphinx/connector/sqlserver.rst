====================
SQL Server connector
====================

The SQL Server connector allows querying and creating tables in an external
`Microsoft SQL Server <https://www.microsoft.com/sql-server/>`_ database. This
can be used to join data between different systems like SQL Server and Hive, or
between two different SQL Server instances.

Requirements
------------

To connect to SQL Server, you need:

* SQL Server 2012 or higher, or Azure SQL Database.
* Network access from the Trino coordinator and workers to SQL Server.
  Port 1433 is the default port.

Configuration
-------------

The connector can query a single database on an SQL server instance. Create a
catalog properties file that specifies the SQL server connector by setting the
``connector.name`` to ``sqlserver``.

For example, to access a database as ``sqlserverdb``, create the file
``etc/catalog/sqlserverdb.properties``. Replace the connection properties as
appropriate for your setup:

.. code-block:: properties

    connector.name=sqlserver
    connection-url=jdbc:sqlserver://<host>:<port>;database=<database>
    connection-user=root
    connection-password=secret

The ``connection-url`` defines the connection information and parameters to pass
to the SQL Server JDBC driver. The supported parameters for the URL are
available in the `SQL Server JDBC driver documentation
<https://docs.microsoft.com/en-us/sql/connect/jdbc/building-the-connection-url>`_.

The ``connection-user`` and ``connection-password`` are typically required and
determine the user credentials for the connection, often a service user. You can
use :doc:`secrets </security/secrets>` to avoid actual values in the catalog
properties files.

Multiple SQL Server databases or servers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The SQL Server connector can't access more than one database using a single
catalog.

If you have multiple databases, or want to access multiple instances
of SQL Server, you need to configure one catalog for each instance.

To add another catalog:

- Add another properties file to ``etc/catalog``
- Save it with a different name that ends in ``.properties``

For example, if you name the property file ``sales.properties``, Trino uses the
configured connector to create a catalog named ``sales``.

Querying SQL Server
-------------------

The SQL Server connector provides access to all schemas visible to the specified user in the configured database.
For the following examples, assume the SQL Server catalog is ``sqlserver``.

You can see the available schemas by running ``SHOW SCHEMAS``::

    SHOW SCHEMAS FROM sqlserver;

If you have a schema named ``web``, you can view the tables
in this schema by running ``SHOW TABLES``::

    SHOW TABLES FROM sqlserver.web;

You can see a list of the columns in the ``clicks`` table in the ``web`` database
using either of the following::

    DESCRIBE sqlserver.web.clicks;
    SHOW COLUMNS FROM sqlserver.web.clicks;

Finally, you can query the ``clicks`` table in the ``web`` schema::

    SELECT * FROM sqlserver.web.clicks;

If you used a different name for your catalog properties file, use
that catalog name instead of ``sqlserver`` in the above examples.

.. _sqlserver-type-mapping:

Type mapping
------------

Trino supports the following SQL Server data types:

==================================  ===============================
SQL Server Type                     Trino Type
==================================  ===============================
``bigint``                          ``bigint``
``smallint``                        ``smallint``
``int``                             ``integer``
``float``                           ``double``
``char(n)``                         ``char(n)``
``varchar(n)``                      ``varchar(n)``
``date``                            ``date``
``datetime2(n)``                    ``timestamp(n)``
==================================  ===============================

Complete list of `SQL Server data types
<https://msdn.microsoft.com/en-us/library/ms187752.aspx>`_.

.. include:: jdbc-type-mapping.fragment

SQL support
-----------

The following SQL statements are not yet supported:

* :doc:`/sql/delete`
* :doc:`/sql/grant`
* :doc:`/sql/revoke`

.. _sqlserver-pushdown:

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
* :func:`stddev`
* :func:`stddev_pop`
* :func:`stddev_samp`
* :func:`variance`
* :func:`var_pop`
* :func:`var_samp`

Data compression
----------------

You can specify the `data compression policy for SQL Server tables
<https://docs.microsoft.com/en-us/sql/relational-databases/data-compression/data-compression>`_
with the ``data_compression`` table property. Valid policies are ``NONE``, ``ROW`` or ``PAGE``.

Example::

    CREATE TABLE myschema.scientists (
      recordkey VARCHAR,
      name VARCHAR,
      age BIGINT,
      birthday DATE
    )
    WITH (
      data_compression = 'ROW'
    );
