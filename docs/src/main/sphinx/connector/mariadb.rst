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

To configure the MariaDB connector, create a catalog properties file in
``etc/catalog`` named, for example, ``example.properties``, to mount the MariaDB
connector as the ``example`` catalog. Create the file with the following
contents, replacing the connection properties as appropriate for your setup:

.. code-block:: text

    connector.name=mariadb
    connection-url=jdbc:mariadb://example.net:3306
    connection-user=root
    connection-password=secret

The ``connection-user`` and ``connection-password`` are typically required and
determine the user credentials for the connection, often a service user. You can
use :doc:`secrets </security/secrets>` to avoid actual values in the catalog
properties files.

.. include:: jdbc-authentication.fragment

.. include:: jdbc-common-configurations.fragment

.. |default_domain_compaction_threshold| replace:: ``32``
.. include:: jdbc-domain-compaction-threshold.fragment

.. include:: jdbc-case-insensitive-matching.fragment

.. include:: non-transactional-insert.fragment

Querying MariaDB
----------------

The MariaDB connector provides a schema for every MariaDB *database*.
You can see the available MariaDB databases by running ``SHOW SCHEMAS``::

    SHOW SCHEMAS FROM example;

If you have a MariaDB database named ``web``, you can view the tables
in this database by running ``SHOW TABLES``::

    SHOW TABLES FROM example.web;

You can see a list of the columns in the ``clicks`` table in the ``web``
database using either of the following::

    DESCRIBE example.web.clicks;
    SHOW COLUMNS FROM example.web.clicks;

Finally, you can access the ``clicks`` table in the ``web`` database::

    SELECT * FROM example.web.clicks;

If you used a different name for your catalog properties file, use
that catalog name instead of ``example`` in the above examples.

.. mariadb-type-mapping:

Type mapping
------------

Because Trino and MariaDB each support types that the other does not, this
connector :ref:`modifies some types <type-mapping-overview>` when reading or
writing data. Data types may not map the same way in both directions between
Trino and the data source. Refer to the following sections for type mapping in
each direction.

MariaDB type to Trino type mapping
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The connector maps MariaDB types to the corresponding Trino types according
to the following table:

.. list-table:: MariaDB type to Trino type mapping
  :widths: 30, 30, 50
  :header-rows: 1

  * - MariaDB type
    - Trino type
    - Notes
  * - ``BOOLEAN``
    - ``TINYINT``
    - ``BOOL`` and ``BOOLEAN`` are aliases of ``TINYINT(1)``
  * - ``TINYINT``
    - ``TINYINT``
    -
  * - ``TINYINT UNSIGNED``
    - ``SMALLINT``
    -
  * - ``SMALLINT``
    - ``SMALLINT``
    -
  * - ``SMALLINT UNSIGNED``
    - ``INTEGER``
    -
  * - ``INT``
    - ``INTEGER``
    -
  * - ``INT UNSIGNED``
    - ``BIGINT``
    -
  * - ``BIGINT``
    - ``BIGINT``
    -
  * - ``BIGINT UNSIGNED``
    - ``DECIMAL(20, 0)``
    -
  * - ``FLOAT``
    - ``REAL``
    -
  * - ``DOUBLE``
    - ``DOUBLE``
    -
  * - ``DECIMAL(p,s)``
    - ``DECIMAL(p,s)``
    -
  * - ``CHAR(n)``
    - ``CHAR(n)``
    -
  * - ``TINYTEXT``
    - ``VARCHAR(255)``
    -
  * - ``TEXT``
    - ``VARCHAR(65535)``
    -
  * - ``MEDIUMTEXT``
    - ``VARCHAR(16777215)``
    -
  * - ``LONGTEXT``
    - ``VARCHAR``
    -
  * - ``VARCHAR(n)``
    - ``VARCHAR(n)``
    -
  * - ``TINYBLOB``
    - ``VARBINARY``
    -
  * - ``BLOB``
    - ``VARBINARY``
    -
  * - ``MEDIUMBLOB``
    - ``VARBINARY``
    -
  * - ``LONGBLOB``
    - ``VARBINARY``
    -
  * - ``VARBINARY(n)``
    - ``VARBINARY``
    -
  * - ``DATE``
    - ``DATE``
    -
  * - ``TIME(n)``
    - ``TIME(n)``
    -
  * - ``TIMESTAMP(n)``
    - ``TIMESTAMP(n)``
    - MariaDB stores the current timestamp by default. Enable
      `explicit_defaults_for_timestamp
      <https://mariadb.com/docs/reference/mdb/system-variables/explicit_defaults_for_timestamp/>`_
      to avoid implicit default values and use ``NULL`` as the default value.

No other types are supported.

Trino type mapping to MariaDB type mapping
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The connector maps Trino types to the corresponding MariaDB types according
to the following table:

.. list-table:: Trino type mapping to MariaDB type mapping
  :widths: 30, 25, 50
  :header-rows: 1

  * - Trino type
    - MariaDB type
    - Notes
  * - ``BOOLEAN``
    - ``BOOLEAN``
    -
  * - ``TINYINT``
    - ``TINYINT``
    -
  * - ``SMALLINT``
    - ``SMALLINT``
    -
  * - ``INTEGER``
    - ``INT``
    -
  * - ``BIGINT``
    - ``BIGINT``
    -
  * - ``REAL``
    - ``FLOAT``
    -
  * - ``DOUBLE``
    - ``DOUBLE``
    -
  * - ``DECIMAL(p,s)``
    - ``DECIMAL(p,s)``
    -
  * - ``CHAR(n)``
    - ``CHAR(n)``
    -
  * - ``VARCHAR(255)``
    - ``TINYTEXT``
    - Maps on ``VARCHAR`` of length 255 or less.
  * - ``VARCHAR(65535)``
    - ``TEXT``
    - Maps on ``VARCHAR`` of length between 256 and 65535, inclusive.
  * - ``VARCHAR(16777215)``
    - ``MEDIUMTEXT``
    - Maps on ``VARCHAR`` of length between 65536 and 16777215, inclusive.
  * - ``VARCHAR``
    - ``LONGTEXT``
    - ``VARCHAR`` of length greater than 16777215 and unbounded ``VARCHAR`` map
      to ``LONGTEXT``.
  * - ``VARBINARY``
    - ``MEDIUMBLOB``
    -
  * - ``DATE``
    - ``DATE``
    -
  * - ``TIME(n)``
    - ``TIME(n)``
    -
  * - ``TIMESTAMP(n)``
    - ``TIMESTAMP(n)``
    - MariaDB stores the current timestamp by default. Enable
      `explicit_defaults_for_timestamp
      <https://mariadb.com/docs/reference/mdb/system-variables/explicit_defaults_for_timestamp/>`_
      to avoid implicit default values and use ``NULL`` as the default value.

No other types are supported.


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

Table functions
---------------

The connector provides specific :doc:`table functions </functions/table>` to
access MariaDB.

.. _mariadb-query-function:

``query(varchar) -> table``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``query`` function allows you to query the underlying database directly. It
requires syntax native to MariaDB, because the full query is pushed down and
processed in MariaDB. This can be useful for accessing native features which are
not available in Trino or for improving query performance in situations where
running a query natively may be faster.

.. include:: query-passthrough-warning.fragment

As an example, query the ``example`` catalog and select the age of employees by
using ``TIMESTAMPDIFF`` and ``CURDATE``::

    SELECT
      age
    FROM
      TABLE(
        example.system.query(
          query => 'SELECT
            TIMESTAMPDIFF(
              YEAR,
              date_of_birth,
              CURDATE()
            ) AS age
          FROM
            tiny.employees'
        )
      );

.. include:: query-table-function-ordering.fragment

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

.. include:: pushdown-correctness-behavior.fragment

.. include:: no-pushdown-text-type.fragment
