====================
ClickHouse Connector
====================

The ClickHouse connector allows querying tables in an external
`Yandex ClickHouse <https://clickhouse.tech/>`_ instance. This can be used to join data between different
systems like ClickHouse and Hive, or between two different ClickHouse instances.

Configuration
-------------

To configure the ClickHouse connector, create a catalog properties file
``etc/catalog/clickhouse.properties`` with the following contents,
replacing the connection properties as appropriate for your setup:

.. code-block:: none

    connector.name=clickhouse
    connection-url=jdbc:clickhouse:host1:8123:/default
    connection-user=default
    connection-password=


Multiple ClickHouse Server
^^^^^^^^^^^^^^^^^^^^^^^^^^

You can have as many catalogs as you need, so if you have additional
ClickHouse servers, simply add another properties file to ``etc/catalog``
with a different name (making sure it ends in ``.properties``). For
example, if you name the property file ``sales.properties``, Presto
will create a catalog named ``sales`` using the configured connector.

Querying ClickHouse
-------------------

The ClickHouse connector provides a schema for every ClickHouse *database*.
You can see the available ClickHouse databases by running ``SHOW SCHEMAS``::

    SHOW SCHEMAS FROM clichouse;

If you have a ClickHouse database named ``web``, you can view the tables
in this database by running ``SHOW TABLES``::

    SHOW TABLES FROM clichouse.web;

You can see a list of the columns in the ``clicks`` table in the ``web`` database
using either of the following::

    DESCRIBE clichouse.web.clicks;
    SHOW COLUMNS FROM clichouse.web.clicks;

Finally, you can access the ``clicks`` table in the ``web`` database::

    SELECT * FROM clichouse.web.clicks;

If you used a different name for your catalog properties file, use
that catalog name instead of ``ClickHouse`` in the above examples.

ClickHouse Connector Limitations
--------------------------------

The following SQL statements are not yet supported:

* :doc:`/sql/grant`
* :doc:`/sql/revoke`
* :doc:`/sql/show-grants`
* :doc:`/sql/show-roles`
* :doc:`/sql/show-role-grants`

