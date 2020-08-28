====================
PostgreSQL Connector
====================

The PostgreSQL connector allows querying and creating tables in an
external `PostgreSQL <https://www.postgresql.org/>`_ database. This can be used to join data between
different systems like PostgreSQL and Hive, or between different
PostgreSQL instances.

Configuration
-------------

To configure the PostgreSQL connector, create a catalog properties file
in ``etc/catalog`` named, for example, ``postgresql.properties``, to
mount the PostgreSQL connector as the ``postgresql`` catalog.
Create the file with the following contents, replacing the
connection properties as appropriate for your setup:

.. code-block:: none

    connector.name=postgresql
    connection-url=jdbc:postgresql://example.net:5432/database
    connection-user=root
    connection-password=secret

Multiple PostgreSQL Databases or Servers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The PostgreSQL connector can only access a single database within
a PostgreSQL server. Thus, if you have multiple PostgreSQL databases,
or want to connect to multiple PostgreSQL servers, you must configure
multiple instances of the PostgreSQL connector.

To add another catalog, simply add another properties file to ``etc/catalog``
with a different name, making sure it ends in ``.properties``. For example,
if you name the property file ``sales.properties``, Presto creates a
catalog named ``sales`` using the configured connector.

Decimal Type Handling
---------------------

``DECIMAL`` types with precision larger than 38 can be mapped to a Presto ``DECIMAL``
by setting the ``decimal-mapping`` configuration property or the ``decimal_mapping`` session property to
``allow_overflow``. The scale of the resulting type is controlled via the ``decimal-default-scale``
configuration property or the ``decimal-rounding-mode`` session property. The precision is always 38.

By default, values that require rounding or truncation to fit will cause a failure at runtime. This behavior
is controlled via the ``decimal-rounding-mode`` configuration property or the ``decimal_rounding_mode`` session
property, which can be set to ``UNNECESSARY`` (the default),
``UP``, ``DOWN``, ``CEILING``, ``FLOOR``, ``HALF_UP``, ``HALF_DOWN``, or ``HALF_EVEN``
(see `RoundingMode <https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/math/RoundingMode.html#enum.constant.summary>`_).

Array Type Handling
-------------------

The PostgreSQL array implementation does not support fixed dimensions whereas Presto
support only arrays with fixed dimensions.
You can configure how the PostgreSQL connector handles arrays with the ``postgresql.array-mapping`` configuration property in your catalog file
or the ``array_mapping`` session property.
The following values are accepted for this property:

* ``DISABLED`` (default): array columns are skipped.
* ``AS_ARRAY``: array columns are interpreted as Presto ``ARRAY`` type, for array columns with fixed dimensions.
* ``AS_JSON``: array columns are interpreted as Presto ``JSON`` type, with no constraint on dimensions.

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

The following SQL statements are not yet supported:

* :doc:`/sql/delete`
* :doc:`/sql/grant`
* :doc:`/sql/revoke`
* :doc:`/sql/show-grants`
* :doc:`/sql/show-roles`
* :doc:`/sql/show-role-grants`
