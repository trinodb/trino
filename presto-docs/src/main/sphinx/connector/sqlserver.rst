====================
SQL Server Connector
====================

The SQL Server connector allows querying and creating tables in an
external `Microsoft SQL Server <https://www.microsoft.com/sql-server/>`_ database. This can be used to join data between
different systems like SQL Server and Hive, or between two different
SQL Server instances.

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

Multiple SQL Server Databases or Servers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The SQL Server connector can't access more than one database using a single
catalog.

If you have multiple databases, or want to access multiple instances
of SQL Server, you need to configure one catalog for each instance.

To add another catalog:

- Add another properties file to ``etc/catalog``
- Save it with a different name that ends in ``.properties``

For example, if you name the property file ``sales.properties``, Presto uses the
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

.. _sqlserver-pushdown:

Pushdown
--------

The connector supports :doc:`pushdown </optimizer/pushdown>` for processing the
following aggregate functions:

* :func:`avg`
* :func:`count`
* :func:`max`
* :func:`min`
* :func:`sum`

Limitations
-----------

Presto supports connecting to SQL Server 2016, SQL Server 2014, SQL Server 2012
and Azure SQL Database.

Presto supports the following SQL Server data types.
The following table shows the mappings between SQL Server and Presto data types.

============================= ============================
SQL Server Type               Presto Type
============================= ============================
``bigint``                    ``bigint``
``smallint``                  ``smallint``
``int``                       ``integer``
``float``                     ``double``
``char(n)``                   ``char(n)``
``varchar(n)``                ``varchar(n)``
``date``                      ``date``
============================= ============================

Complete list of `SQL Server data types
<https://msdn.microsoft.com/en-us/library/ms187752.aspx>`_.

The following SQL statements are not yet supported:

* :doc:`/sql/delete`
* :doc:`/sql/grant`
* :doc:`/sql/revoke`
