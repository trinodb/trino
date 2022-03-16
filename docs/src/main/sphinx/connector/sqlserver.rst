====================
SQL Server connector
====================

.. raw:: html

  <img src="../_static/img/sqlserver.png" class="connector-logo">

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

For example, to access a database as ``sqlserver``, create the file
``etc/catalog/sqlserver.properties``. Replace the connection properties as
appropriate for your setup:

.. code-block:: properties

    connector.name=sqlserver
    connection-url=jdbc:sqlserver://<host>:<port>;database=<database>;encrypt=false
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

.. _sqlserver-tls:

Connection security
^^^^^^^^^^^^^^^^^^^

The JDBC driver, and therefore the connector, automatically use Transport Layer
Security (TLS) encryption and certificate validation. This requires a suitable
TLS certificate configured on your SQL Server database host.

If you do not have the necessary configuration established, you can disable
encryption in the connection string with the ``encrypt`` property:

.. code-block:: properties

  connection-url=jdbc:sqlserver://<host>:<port>;database=<database>;encrypt=false

Further parameters like ``trustServerCertificate``, ``hostNameInCertificate``,
``trustStore``, and ``trustStorePassword`` are details in the `TLS section of
SQL Server JDBC driver documentation
<https://docs.microsoft.com/en-us/sql/connect/jdbc/using-ssl-encryption>`_.

Multiple SQL Server databases or servers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The SQL Server connector can only access a single SQL Server database
within a single catalog. Thus, if you have multiple SQL Server databases,
or want to connect to multiple SQL Server instances, you must configure
multiple instances of the SQL Server connector.

To add another catalog, simply add another properties file to ``etc/catalog``
with a different name, making sure it ends in ``.properties``. For example,
if you name the property file ``sales.properties``, Trino creates a
catalog named ``sales`` using the configured connector.

.. include:: jdbc-common-configurations.fragment

.. include:: jdbc-procedures.fragment

.. include:: jdbc-case-insensitive-matching.fragment

.. include:: non-transactional-insert.fragment

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
``tinyint``                         ``smallint``
``smallint``                        ``smallint``
``int``                             ``integer``
``float``                           ``double``
``char(n)``                         ``char(n)``
``varchar(n)``                      ``varchar(n)``
``date``                            ``date``
``datetime2(n)``                    ``timestamp(n)``
``datetimeoffset(n)``               ``timestamp(n) with time zone``
==================================  ===============================

Complete list of `SQL Server data types
<https://msdn.microsoft.com/en-us/library/ms187752.aspx>`_.

.. include:: jdbc-type-mapping.fragment

.. _sqlserver-sql-support:

SQL support
-----------

The connector provides read access and write access to data and metadata in SQL
Server. In addition to the :ref:`globally available <sql-globally-available>`
and :ref:`read operation <sql-read-operations>` statements, the connector
supports the following features:

* :doc:`/sql/insert`
* :doc:`/sql/delete`
* :doc:`/sql/truncate`
* :ref:`sql-schema-table-management`

.. include:: sql-delete-limitation.fragment

.. include:: alter-table-limitation.fragment

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

.. include:: no-pushdown-text-type.fragment

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
