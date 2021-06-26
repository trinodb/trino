=====================
CockroachDB connector
=====================

The CockroachDB connector ...

Requirements
------------

To connect to CockroachDB, you need:

* CockroachDB 21.1.2 or higher.
* Network access from the Trino coordinator and workers to CockroachDB.
  Port 26257 is the default port.

Configuration
-------------

The connector can query a single database on an SQL server instance. Create a
catalog properties file that specifies the SQL server connector by setting the
``connector.name`` to ``sqlserver``.

For example, to access a database as ``sqlserverdb``, create the file
``etc/catalog/sqlserverdb.properties``. Replace the connection properties as
appropriate for your setup:

.. code-block:: properties

    connector.name=cockroachdb
    connection-url=jdbc:postgresql://localhost:26257?sslmode=disable
    connection-user=admin
    connection-password=secret

The ``connection-url`` defines the connection information and parameters to pass
to the JDBC driver. The supported parameters for the URL are
available in the ??? JDBC driver documentation.

The ``connection-user`` and ``connection-password`` are typically required, and
determine the user credentials for the connection, often a service user. You can
use :doc:`secrets </security/secrets>` to avoid actual values in the catalog
properties files.

.. _cockroachdb-type-mapping:

Type mapping
------------

Both CockroachDB and Trino have types that are not supported by the CockroachDB
connector. The following sections explain their type mapping.

CockroachDB to Trino type mapping
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Trino supports selecting CockroachDB database types. This table shows the
CockroachDB to Trino data type mapping:


.. list-table:: CockroachDB to Trino type mapping
  :widths: 50, 50
  :header-rows: 1

  * - CockroachDB type
    - Trino type
  * -
    - ``DECIMAL(p, s)``
  * -
    - ``DECIMAL(p, 0)``
  * -
    - ``DOUBLE``
  * -
    - ``REAL``
  * -
    - ``VARCHAR(n)``
  * -
    - ``CHAR(n)``
  * -
    - ``CHAR(n)``
  * -
    - ``TIMESTAMP``
  * -
    - ``TIMESTAMP WITH TIME ZONE``

Trino to CockroachDB type mapping
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Trino supports creating tables with the following types in an CockroachDB database.
The table shows the mappings from Trino to CockroachDB data types:

.. list-table:: Trino to CockroachDB type mapping
  :widths: 50, 50
  :header-rows: 1

  * - Trino type
    - CockroachDB type
  * - ``TINYINT``
    -
  * - ``SMALLINT``
    -
  * - ``INTEGER``
    -
  * - ``BIGINT``
    -
  * - ``DECIMAL(p, s)``
    -
  * - ``REAL``
    -
  * - ``DOUBLE``
    -
  * - ``VARCHAR``
    -
  * - ``VARCHAR(n)``
    -
  * - ``CHAR(n)``
    -
  * - ``VARBINARY``
    -
  * - ``DATE``
    -
  * - ``TIMESTAMP``
    -
  * - ``TIMESTAMP WITH TIME ZONE``
    -

.. include:: jdbc-type-mapping.fragment

SQL support
-----------

TBD

Performance
-----------

TBD

Security
--------

TBD
