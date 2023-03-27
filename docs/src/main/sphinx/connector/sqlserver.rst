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

The connector can query a single database on a given SQL Server instance. Create
a catalog properties file that specifies the SQL server connector by setting the
``connector.name`` to ``sqlserver``.

For example, to access a database as ``example``, create the file
``etc/catalog/example.properties``. Replace the connection properties as
appropriate for your setup:

.. code-block:: properties

    connector.name=sqlserver
    connection-url=jdbc:sqlserver://<host>:<port>;databaseName=<databaseName>;encrypt=false
    connection-user=root
    connection-password=secret

The ``connection-url`` defines the connection information and parameters to pass
to the SQL Server JDBC driver. The supported parameters for the URL are
available in the `SQL Server JDBC driver documentation
<https://docs.microsoft.com/sql/connect/jdbc/building-the-connection-url>`_.

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

  connection-url=jdbc:sqlserver://<host>:<port>;databaseName=<databaseName>;encrypt=false

Further parameters like ``trustServerCertificate``, ``hostNameInCertificate``,
``trustStore``, and ``trustStorePassword`` are details in the `TLS section of
SQL Server JDBC driver documentation
<https://docs.microsoft.com/sql/connect/jdbc/using-ssl-encryption>`_.

.. include:: jdbc-authentication.fragment

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

.. |default_domain_compaction_threshold| replace:: ``32``
.. include:: jdbc-domain-compaction-threshold.fragment

Specific configuration properties
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The SQL Server connector supports additional catalog properties to configure the
behavior of the connector and the issues queries to the database.

.. list-table::
  :widths: 45, 55
  :header-rows: 1

  * - Property name
    - Description
  * - ``sqlserver.snapshot-isolation.disabled``
    - Control the automatic use of snapshot isolation for transactions issued by
      Trino in SQL Server. Defaults to ``false``, which means that snapshot
      isolation is enabled.

.. include:: jdbc-procedures.fragment

.. include:: jdbc-case-insensitive-matching.fragment

.. include:: non-transactional-insert.fragment

Querying SQL Server
-------------------

The SQL Server connector provides access to all schemas visible to the specified
user in the configured database. For the following examples, assume the SQL
Server catalog is ``example``.

You can see the available schemas by running ``SHOW SCHEMAS``::

    SHOW SCHEMAS FROM example;

If you have a schema named ``web``, you can view the tables
in this schema by running ``SHOW TABLES``::

    SHOW TABLES FROM example.web;

You can see a list of the columns in the ``clicks`` table in the ``web`` database
using either of the following::

    DESCRIBE example.web.clicks;
    SHOW COLUMNS FROM example.web.clicks;

Finally, you can query the ``clicks`` table in the ``web`` schema::

    SELECT * FROM example.web.clicks;

If you used a different name for your catalog properties file, use
that catalog name instead of ``example`` in the above examples.

.. _sqlserver-type-mapping:

Type mapping
------------

Because Trino and SQL Server each support types that the other does not, this
connector :ref:`modifies some types <type-mapping-overview>` when reading or
writing data. Data types may not map the same way in both directions between
Trino and the data source. Refer to the following sections for type mapping in
each direction.

SQL Server type to Trino type mapping
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The connector maps SQL Server types to the corresponding Trino types following this table:

.. list-table:: SQL Server type to Trino type mapping
  :widths: 30, 20, 50
  :header-rows: 1

  * - SQL Server database type
    - Trino type
    - Notes
  * - ``BIT``
    - ``BOOLEAN``
    -
  * - ``TINYINT``
    - ``SMALLINT``
    - SQL Server ``TINYINT`` is actually ``unsigned tinyint``
  * - ``SMALLINT``
    - ``SMALLINT``
    -
  * - ``INTEGER``
    - ``INTEGER``
    -
  * - ``BIGINT``
    - ``BIGINT``
    -
  * - ``DOUBLE PRECISION``
    - ``DOUBLE``
    -
  * - ``FLOAT[(n)]``
    - ``REAL`` or ``DOUBLE``
    -  See :ref:`sqlserver-numeric-mapping`
  * - ``REAL``
    - ``REAL``
    -
  * - ``DECIMAL[(p[, s])]``, ``NUMERIC[(p[, s])]``
    - ``DECIMAL(p, s)``
    -
  * - ``CHAR[(n)]``
    - ``CHAR(n)``
    - ``1 <= n <= 8000``
  * - ``NCHAR[(n)]``
    - ``CHAR(n)``
    - ``1 <= n <= 4000``
  * - ``VARCHAR[(n | max)]``, ``NVARCHAR[(n | max)]``
    - ``VARCHAR(n)``
    - ``1 <= n <= 8000``, ``max = 2147483647``
  * - ``TEXT``
    - ``VARCHAR(2147483647)``
    -
  * - ``NTEXT``
    - ``VARCHAR(1073741823)``
    -
  * - ``VARBINARY[(n | max)]``
    - ``VARBINARY``
    - ``1 <= n <= 8000``, ``max = 2147483647``
  * - ``DATE``
    - ``DATE``
    -
  * - ``TIME[(n)]``
    - ``TIME(n)``
    - ``0 <= n <= 7``
  * - ``DATETIME2[(n)]``
    - ``TIMESTAMP(n)``
    - ``0 <= n <= 7``
  * - ``SMALLDATETIME``
    - ``TIMESTAMP(0)``
    -
  * - ``DATETIMEOFFSET[(n)]``
    - ``TIMESTAMP(n) WITH TIME ZONE``
    - ``0 <= n <= 7``

Trino type to SQL Server type mapping
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The connector maps Trino types to the corresponding SQL Server types following this table:

.. list-table:: Trino type to SQL Server type mapping
  :widths: 30, 20, 50
  :header-rows: 1

  * - Trino type
    - SQL Server type
    - Notes
  * - ``BOOLEAN``
    - ``BIT``
    -
  * - ``TINYINT``
    - ``TINYINT``
    - Trino only supports writing values belonging to ``[0, 127]``
  * - ``SMALLINT``
    - ``SMALLINT``
    -
  * - ``INTEGER``
    - ``INTEGER``
    -
  * - ``BIGINT``
    - ``BIGINT``
    -
  * - ``REAL``
    - ``REAL``
    -
  * - ``DOUBLE``
    - ``DOUBLE PRECISION``
    -
  * - ``DECIMAL(p, s)``
    - ``DECIMAL(p, s)``
    -
  * - ``CHAR(n)``
    - ``NCHAR(n)`` or ``NVARCHAR(max)``
    -  See :ref:`sqlserver-character-mapping`
  * - ``VARCHAR(n)``
    - ``NVARCHAR(n)`` or ``NVARCHAR(max)``
    -  See :ref:`sqlserver-character-mapping`
  * - ``VARBINARY``
    - ``VARBINARY(max)``
    -
  * - ``DATE``
    - ``DATE``
    -
  * - ``TIME(n)``
    - ``TIME(n)``
    - ``0 <= n <= 7``
  * - ``TIMESTAMP(n)``
    - ``DATETIME2(n)``
    - ``0 <= n <= 7``

Complete list of `SQL Server data types
<https://msdn.microsoft.com/library/ms187752.aspx>`_.

.. _sqlserver-numeric-mapping:

Numeric type mapping
^^^^^^^^^^^^^^^^^^^^

For SQL Server ``FLOAT[(n)]``:

- If ``n`` is not specified maps to Trino ``Double``
- If ``1 <= n <= 24`` maps to Trino ``REAL``
- If ``24 < n <= 53`` maps to Trino ``DOUBLE``

.. _sqlserver-character-mapping:

Character type mapping
^^^^^^^^^^^^^^^^^^^^^^

For Trino ``CHAR(n)``:

- If ``1 <= n <= 4000`` maps SQL Server ``NCHAR(n)``
- If ``n > 4000`` maps SQL Server ``NVARCHAR(max)``


For Trino ``VARCHAR(n)``:

- If ``1 <= n <= 4000`` maps SQL Server ``NVARCHAR(n)``
- If ``n > 4000`` maps SQL Server ``NVARCHAR(max)``

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

Table functions
---------------

The connector provides specific :doc:`table functions </functions/table>` to
access SQL Server.

.. _sqlserver-query-function:

``query(varchar) -> table``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``query`` function allows you to query the underlying database directly. It
requires syntax native to SQL Server, because the full query is pushed down and
processed in SQL Server. This can be useful for accessing native features which
are not implemented in Trino or for improving query performance in situations
where running a query natively may be faster.

.. include:: polymorphic-table-function-ordering.fragment

For example, query the ``example`` catalog and select the top 10 percent of
nations by population::

    SELECT
      *
    FROM
      TABLE(
        example.system.query(
          query => 'SELECT
            TOP(10) PERCENT *
          FROM
            tpch.nation
          ORDER BY
            population DESC'
        )
      );


Performance
-----------

The connector includes a number of performance improvements, detailed in the
following sections.

.. _sqlserver-table-statistics:

Table statistics
^^^^^^^^^^^^^^^^

The SQL Server connector can use :doc:`table and column statistics
</optimizer/statistics>` for :doc:`cost based optimizations
</optimizer/cost-based-optimizations>`, to improve query processing performance
based on the actual data in the data source.

The statistics are collected by SQL Server and retrieved by the connector.

The connector can use information stored in single-column statistics. SQL Server
Database can automatically create column statistics for certain columns. If
column statistics are not created automatically for a certain column, you can
create them by executing the following statement in SQL Server Database.

.. code-block:: sql

    CREATE STATISTICS example_statistics_name ON table_schema.table_name (column_name);

SQL Server Database routinely updates the statistics. In some cases, you may
want to force statistics update (e.g. after defining new column statistics or
after changing data in the table). You can do that by executing the following
statement in SQL Server Database.

.. code-block:: sql

    UPDATE STATISTICS table_schema.table_name;

Refer to SQL Server documentation for information about options, limitations and
additional considerations.

.. _sqlserver-pushdown:

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

.. include:: pushdown-correctness-behavior.fragment

.. include:: join-pushdown-enabled-true.fragment

.. include:: no-pushdown-text-type.fragment

.. _sqlserver-bulk-insert:

Bulk insert
^^^^^^^^^^^

You can optionally use the `bulk copy API
<https://docs.microsoft.com/sql/connect/jdbc/use-bulk-copy-api-batch-insert-operation>`_
to drastically speed up write operations.

Enable bulk copying and a lock on the destination table to meet `minimal
logging requirements
<https://docs.microsoft.com/sql/relational-databases/import-export/prerequisites-for-minimal-logging-in-bulk-import>`_.

The following table shows the relevant catalog configuration properties and
their default values:

.. list-table:: Bulk load properties
  :widths: 30, 60, 10
  :header-rows: 1

  * - Property name
    - Description
    - Default
  * - ``sqlserver.bulk-copy-for-write.enabled``
    - Use the SQL Server bulk copy API for writes. The corresponding catalog
      session property is ``bulk_copy_for_write``.
    - ``false``
  * - ``sqlserver.bulk-copy-for-write.lock-destination-table``
    - Obtain a bulk update lock on the destination table for write operations.
      The corresponding catalog session property is
      ``bulk_copy_for_write_lock_destination_table``. Setting is only used when
      ``bulk-copy-for-write.enabled=true``.
    - ``false``

Limitations:

* Column names with leading and trailing spaces are not supported.


Data compression
----------------

You can specify the `data compression policy for SQL Server tables
<https://docs.microsoft.com/sql/relational-databases/data-compression/data-compression>`_
with the ``data_compression`` table property. Valid policies are ``NONE``, ``ROW`` or ``PAGE``.

Example::

    CREATE TABLE example_schema.scientists (
      recordkey VARCHAR,
      name VARCHAR,
      age BIGINT,
      birthday DATE
    )
    WITH (
      data_compression = 'ROW'
    );
