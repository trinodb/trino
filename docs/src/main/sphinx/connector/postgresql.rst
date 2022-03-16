====================
PostgreSQL connector
====================

.. raw:: html

  <img src="../_static/img/postgresql.png" class="connector-logo">

The PostgreSQL connector allows querying and creating tables in an
external `PostgreSQL <https://www.postgresql.org/>`_ database. This can be used to join data between
different systems like PostgreSQL and Hive, or between different
PostgreSQL instances.

Requirements
------------

To connect to PostgreSQL, you need:

* PostgreSQL 10.x or higher.
* Network access from the Trino coordinator and workers to PostgreSQL.
  Port 5432 is the default port.

Configuration
-------------

The connector can query a database on a PostgreSQL server. Create a catalog
properties file that specifies the PostgreSQL connector by setting the
``connector.name`` to ``postgresql``.

For example, to access a database as the ``postgresql`` catalog, create the
file ``etc/catalog/postgresql.properties``. Replace the connection properties
as appropriate for your setup:

.. code-block:: text

    connector.name=postgresql
    connection-url=jdbc:postgresql://example.net:5432/database
    connection-user=root
    connection-password=secret

The ``connection-url`` defines the connection information and parameters to pass
to the PostgreSQL JDBC driver. The parameters for the URL are available in the
`PostgreSQL JDBC driver documentation
<https://jdbc.postgresql.org/documentation/head/connect.html>`_. Some parameters
can have adverse effects on the connector behavior or not work with the
connector.

The ``connection-user`` and ``connection-password`` are typically required and
determine the user credentials for the connection, often a service user. You can
use :doc:`secrets </security/secrets>` to avoid actual values in the catalog
properties files.

.. _postgresql-tls:

Connection security
^^^^^^^^^^^^^^^^^^^

If you have TLS configured with a globally-trusted certificate installed on your
data source, you can enable TLS between your cluster and the data
source by appending a parameter to the JDBC connection string set in the
``connection-url`` catalog configuration property.

For example, with version 42 of the PostgreSQL JDBC driver, enable TLS by
appending the ``ssl=true`` parameter to the ``connection-url`` configuration
property:

.. code-block:: properties

  connection-url=jdbc:postgresql://example.net:5432/database?ssl=true

For more information on TLS configuration options, see the `PostgreSQL JDBC
driver documentation <https://jdbc.postgresql.org/documentation/head/connect.html>`_.

Multiple PostgreSQL databases or servers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The PostgreSQL connector can only access a single database within
a PostgreSQL server. Thus, if you have multiple PostgreSQL databases,
or want to connect to multiple PostgreSQL servers, you must configure
multiple instances of the PostgreSQL connector.

To add another catalog, simply add another properties file to ``etc/catalog``
with a different name, making sure it ends in ``.properties``. For example,
if you name the property file ``sales.properties``, Trino creates a
catalog named ``sales`` using the configured connector.

.. include:: jdbc-common-configurations.fragment

.. include:: jdbc-procedures.fragment

.. include:: jdbc-case-insensitive-matching.fragment

.. include:: non-transactional-insert.fragment

.. _postgresql-type-mapping:

Type mapping
------------

The data type mappings are as follows:

=================== ================================ =======================================================================
PostgreSQL          Trino                            Notes
=================== ================================ =======================================================================
``BIT``             ``BOOLEAN``
``BOOLEAN``         ``BOOLEAN``
``SMALLINT``        ``SMALLINT``
``INTEGER``         ``INTEGER``
``BIGINT``          ``BIGINT``
``REAL``            ``DATE``
``DOUBLE``          ``DOUBLE``
``NUMERIC(p, s)``   ``DECIMAL(p, s)``                ``DECIMAL(p, s)`` is an alias of  ``NUMERIC(p, s)``.
                                                     See :ref:`postgresql-decimal-type-handling` for more information.
``CHAR(n)``         ``CHAR(n)``
``VARCHAR(n)``      ``VARCHAR(n)``
``ENUM``            ``VARCHAR``
``BINARY``          ``VARBINARY``
``DATE``            ``DATE``
``TIME(n)``         ``TIME(n)``
``TIMESTAMP(n)``    ``TIMESTAMP(n)``
``TIMESTAMPTZ(n)``  ``TIMESTAMP(n) WITH TIME ZONE``
``MONEY``           ``VARCHAR``
``UUID``            ``UUID``
``JSON``            ``JSON``
``JSONB``           ``JSON``
``HSTORE``          ``MAP(VARCHAR, VARCHAR)``
``ARRAY``           Disabled, ``ARRAY`` or ``JSON``  See :ref:`postgresql-array-type-handling` for more information.
=================== ================================ =======================================================================

.. _postgresql-decimal-type-handling:

Decimal type handling
^^^^^^^^^^^^^^^^^^^^^

``DECIMAL`` types with precision larger than 38 can be mapped to a Trino ``DECIMAL``
by setting the ``decimal-mapping`` configuration property or the ``decimal_mapping`` session property to
``allow_overflow``. The scale of the resulting type is controlled via the ``decimal-default-scale``
configuration property or the ``decimal-rounding-mode`` session property. The precision is always 38.

By default, values that require rounding or truncation to fit will cause a failure at runtime. This behavior
is controlled via the ``decimal-rounding-mode`` configuration property or the ``decimal_rounding_mode`` session
property, which can be set to ``UNNECESSARY`` (the default),
``UP``, ``DOWN``, ``CEILING``, ``FLOOR``, ``HALF_UP``, ``HALF_DOWN``, or ``HALF_EVEN``
(see `RoundingMode <https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/math/RoundingMode.html#enum.constant.summary>`_).

.. _postgresql-array-type-handling:

Array type handling
^^^^^^^^^^^^^^^^^^^

The PostgreSQL array implementation does not support fixed dimensions whereas Trino
support only arrays with fixed dimensions.
You can configure how the PostgreSQL connector handles arrays with the ``postgresql.array-mapping`` configuration property in your catalog file
or the ``array_mapping`` session property.
The following values are accepted for this property:

* ``DISABLED`` (default): array columns are skipped.
* ``AS_ARRAY``: array columns are interpreted as Trino ``ARRAY`` type, for array columns with fixed dimensions.
* ``AS_JSON``: array columns are interpreted as Trino ``JSON`` type, with no constraint on dimensions.

.. include:: jdbc-type-mapping.fragment

Querying PostgreSQL
-------------------

The PostgreSQL connector provides a schema for every PostgreSQL schema.
You can see the available PostgreSQL schemas by running ``SHOW SCHEMAS``::

    SHOW SCHEMAS FROM postgresql;

If you have a PostgreSQL schema named ``web``, you can view the tables
in this schema by running ``SHOW TABLES``::

    SHOW TABLES FROM postgresql.web;

You can see a list of the columns in the ``clicks`` table in the ``web`` database
using either of the following::

    DESCRIBE postgresql.web.clicks;
    SHOW COLUMNS FROM postgresql.web.clicks;

Finally, you can access the ``clicks`` table in the ``web`` schema::

    SELECT * FROM postgresql.web.clicks;

If you used a different name for your catalog properties file, use
that catalog name instead of ``postgresql`` in the above examples.

.. _postgresql-sql-support:

SQL support
-----------

The connector provides read access and write access to data and metadata in
PostgreSQL.  In addition to the :ref:`globally available
<sql-globally-available>` and :ref:`read operation <sql-read-operations>`
statements, the connector supports the following features:

* :doc:`/sql/insert`
* :doc:`/sql/delete`
* :doc:`/sql/truncate`
* :ref:`sql-schema-table-management`

.. include:: sql-delete-limitation.fragment

.. include:: alter-table-limitation.fragment

.. include:: alter-schema-limitation.fragment

.. _postgresql-pushdown:

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
* :func:`covar_pop`
* :func:`covar_samp`
* :func:`corr`
* :func:`regr_intercept`
* :func:`regr_slope`

Predicate pushdown support
^^^^^^^^^^^^^^^^^^^^^^^^^^

The connector does not support pushdown of range predicates, such as ``>``,
``<``, or ``BETWEEN``, on columns with :ref:`character string types
<string-data-types>` like ``CHAR`` or ``VARCHAR``.  Equality predicates, such as
``IN`` or ``=``, and inequality predicates, such as ``!=`` on columns with
textual types are pushed down. This ensures correctness of results since the
remote data source may sort strings differently than Trino.

In the following example, the predicate of the first query is not pushed down
since ``name`` is a column of type ``VARCHAR`` and ``>`` is a range predicate.
The other queries are pushed down.

.. code-block:: sql

    -- Not pushed down
    SELECT * FROM nation WHERE name > 'CANADA';
    -- Pushed down
    SELECT * FROM nation WHERE name != 'CANADA';
    SELECT * FROM nation WHERE name = 'CANADA';

There is experimental support to enable pushdown of range predicates on columns
with character string types which can be enabled by setting the
``postgresql.experimental.enable-string-pushdown-with-collate`` catalog
configuration property or the corresponding
``enable_string_pushdown_with_collate`` session property to ``true``.
Enabling this configuration will make the predicate of all the queries in the
above example get pushed down.
