====================
ClickHouse Connector
====================

The ClickHouse connector allows querying tables in an external
`Yandex ClickHouse <https://clickhouse.tech/>`_ instance. This can be used to join data between different
systems like ClickHouse and Hive, or between two different ClickHouse instances.

Configuration
-------------

To configure the ClickHouse connector, create a catalog properties file ``etc/catalog/clickhouse.properties``,
replace the connection properties as needed for your setup:

.. code-block:: none

    connector.name=clickhouse
    connection-url=jdbc:clickhouse://host1:8123/
    connection-user=default
    connection-password=


Multiple ClickHouse servers
^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you have multiple ClickHouse servers you need to configure one catalog for each instance.
To add another catalog:

* Add another properties file to ``etc/catalog``
* Save it with a different name that ends in ``.properties``

For example, if you name the property file ``sales.properties``, Trino uses the configured
connector to create a catalog named ``sales``.

Querying ClickHouse
-------------------

The ClickHouse connector provides a schema for every ClickHouse *database*.
run ``SHOW SCHEMAS`` to see the available ClickHouse databases::

    SHOW SCHEMAS FROM clickhouse;

If you have a ClickHouse database named ``web``, run ``SHOW TABLES`` to view the tables
in this database::

    SHOW TABLES FROM clickhouse.web;

Run ``DESCRIBE`` or ``SHOW COLUMNS`` to list the columns in the ``clicks`` table in the
``web`` databases::

    DESCRIBE clickhouse.web.clicks;
    SHOW COLUMNS FROM clickhouse.web.clicks;

Run ``SELECT`` to access the ``clicks`` table in the ``web`` database::

    SELECT * FROM clickhouse.web.clicks;

.. note::

    If you used a different name for your catalog properties file, use
    that catalog name instead of ``ClickHouse`` in the above examples.


ClickHouse Connector Limitations
--------------------------------

The following SQL statements aren't  supported:

* :doc:`/sql/grant`
* :doc:`/sql/revoke`
* :doc:`/sql/show-grants`
* :doc:`/sql/show-roles`
* :doc:`/sql/show-role-grants`
