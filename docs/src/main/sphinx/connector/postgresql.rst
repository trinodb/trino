====================
PostgreSQL connector
====================

The PostgreSQL connector allows querying and creating tables in an
external `PostgreSQL <https://www.postgresql.org/>`_ database. This can be used to join data between
different systems like PostgreSQL and Hive, or between different
PostgreSQL instances.

Requirements
------------

To connect to PostgreSQL, you need:

* PostgreSQL 9.6 or higher.
* Network access from the Trino coordinator and workers to PostgreSQL.
  Port 5432 is the default port.

Configuration
-------------

The connector can query a database on a PostgreSQL server. Create a catalog
properties file that specifies the PostgreSQL connector by setting the
``connector.name`` to ``postgresql``.

For example, to access a database as the ``postgresqlsdb`` catalog, create the
file ``etc/catalog/postgresqlsdb.properties``. Replace the connection properties
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

.. _postgresql-type-mapping:

Type mapping
------------

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

Limitations
-----------

The following SQL statements are not yet supported:

* :doc:`/sql/delete`
* :doc:`/sql/grant`
* :doc:`/sql/revoke`
* :doc:`/sql/show-grants`
* :doc:`/sql/show-roles`
* :doc:`/sql/show-role-grants`
