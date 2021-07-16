=================
Vertica Connector
=================

The Vertica connector allows querying and creating tables in an
external `Vertica <https://www.vertica.com/>`_ database. This can be used to join data between
different systems like Vertica and Hive, or between different
Vertica instances.

Configuration
-------------

To configure the Vertica connector, create a catalog properties file
in ``etc/catalog`` named, for example, ``vertica.properties``, to
mount the Vertica connector as the ``vertica`` catalog.
Create the file with the following contents, replacing the
connection properties as appropriate for your setup:

.. code-block:: none

    connector.name=vertica
    connection-url=jdbc:vertica://example.net:5433
    connection-user=root
    connection-password=secret

Querying Vertica
----------------

The Vertica connector provides a schema for every Vertica *database*.
You can see the available Vertica databases by running ``SHOW SCHEMAS``::

    SHOW SCHEMAS FROM vertica;

If you have a Vertica database named ``web``, you can view the tables
in this database by running ``SHOW TABLES``::

    SHOW TABLES FROM vertica.web;

You can see a list of the columns in the ``clicks`` table in the ``web`` database
using either of the following::

    DESCRIBE vertica.web.clicks;
    SHOW COLUMNS FROM vertica.web.clicks;

Finally, you can access the ``clicks`` table in the ``web`` database::

    SELECT * FROM vertica.web.clicks;

If you used a different name for your catalog properties file, use
that catalog name instead of ``vertica`` in the above examples.

Limitations
-----------

The following SQL statements are not yet supported:

* Drop a column with :doc:`/sql/alter-table`
* Rename a table across schemas with :doc:`/sql/alter-table`
* :doc:`/sql/delete`
* :doc:`/sql/grant`
* :doc:`/sql/revoke`
* :doc:`/sql/show-grants`
* :doc:`/sql/show-roles`
* :doc:`/sql/show-role-grants`
