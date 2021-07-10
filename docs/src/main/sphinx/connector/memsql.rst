==============================
SingleStore (MemSQL) connector
==============================

The SingleStore (formerly known as MemSQL) connector allows querying and
creating tables in an external SingleStore database. The SingleStore connector
is very similar to the MySQL connector with the only difference being the
underlying driver.

Configuration
-------------

To configure the SingleStore connector, create a catalog properties file
in ``etc/catalog`` named, for example, ``memsql.properties``, to
mount the SingleStore connector as the ``memsql`` catalog.
Create the file with the following contents, replacing the
connection properties as appropriate for your setup:

.. code-block:: text

    connector.name=memsql
    connection-url=jdbc:mariadb://example.net:3306
    connection-user=root
    connection-password=secret

Multiple SingleStore servers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can have as many catalogs as you need, so if you have additional
SingleStore servers, simply add another properties file to ``etc/catalog``
with a different name (making sure it ends in ``.properties``). For
example, if you name the property file ``sales.properties``, Trino
will create a catalog named ``sales`` using the configured connector.

Querying SingleStore
--------------------

The SingleStore connector provides a schema for every SingleStore *database*.
You can see the available SingleStore databases by running ``SHOW SCHEMAS``::

    SHOW SCHEMAS FROM memsql;

If you have a SingleStore database named ``web``, you can view the tables
in this database by running ``SHOW TABLES``::

    SHOW TABLES FROM memsql.web;

You can see a list of the columns in the ``clicks`` table in the ``web``
database using either of the following::

    DESCRIBE memsql.web.clicks;
    SHOW COLUMNS FROM memsql.web.clicks;

Finally, you can access the ``clicks`` table in the ``web`` database::

    SELECT * FROM memsql.web.clicks;

If you used a different name for your catalog properties file, use
that catalog name instead of ``memsql`` in the above examples.

.. _memsql-type-mapping:

Type mapping
------------

.. include:: jdbc-type-mapping.fragment

.. _memsql-pushdown:

Pushdown
--------

The connector supports pushdown for a number of operations:

* :ref:`join-pushdown`
* :ref:`limit-pushdown`
* :ref:`topn-pushdown`

Limitations
-----------

The following SQL statements are not yet supported:

* :doc:`/sql/delete`
* :doc:`/sql/grant`
* :doc:`/sql/revoke`
* :doc:`/sql/show-grants`
* :doc:`/sql/show-roles`
* :doc:`/sql/show-role-grants`
