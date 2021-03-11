====================
ClickHouse connector
====================

The ClickHouse connector allows querying tables in an external
`Yandex ClickHouse <https://clickhouse.tech/>`_ instance. This can be used to
query data in the databases on that instance, or combine it with other data
from different catalogs accessing ClickHouse or any other supported data source.

Configuration
-------------

The connector can query a ClickHouse instance. Create a catalog properties file
that specifies the ClickHouse connector by setting the ``connector.name`` to
``clickhouse``.

For example, to access an instance as ``myclickhouse``, create the file
``etc/catalog/myclickhouse.properties``. Replace the connection properties as
appropriate for your setup:

.. code-block:: none

    connector.name=clickhouse
    connection-url=jdbc:clickhouse://host1:8123/
    connection-user=exampleuser
    connection-password=examplepassword

Multiple ClickHouse servers
^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you have multiple ClickHouse instances you need to configure one catalog for
each instance. To add another catalog:

* Add another properties file to ``etc/catalog``
* Save it with a different name that ends in ``.properties``

For example, if you name the property file ``sales.properties``, Trino uses the
configured connector to create a catalog named ``sales``.

Querying ClickHouse
-------------------

The ClickHouse connector provides a schema for every ClickHouse *database*.
run ``SHOW SCHEMAS`` to see the available ClickHouse databases::

    SHOW SCHEMAS FROM myclickhouse;

If you have a ClickHouse database named ``web``, run ``SHOW TABLES`` to view the
tables in this database::

    SHOW TABLES FROM myclickhouse.web;

Run ``DESCRIBE`` or ``SHOW COLUMNS`` to list the columns in the ``clicks`` table in the
``web`` databases::

    DESCRIBE myclickhouse.web.clicks;
    SHOW COLUMNS FROM clickhouse.web.clicks;

Run ``SELECT`` to access the ``clicks`` table in the ``web`` database::

    SELECT * FROM myclickhouse.web.clicks;

.. note::

    If you used a different name for your catalog properties file, use
    that catalog name instead of ``myclickhouse`` in the above examples.

Limitations
-----------

The following SQL statements aren't  supported:

* :doc:`/sql/grant`
* :doc:`/sql/revoke`
* :doc:`/sql/show-grants`
* :doc:`/sql/show-roles`
* :doc:`/sql/show-role-grants`
