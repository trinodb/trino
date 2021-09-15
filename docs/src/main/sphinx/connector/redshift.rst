==================
Redshift connector
==================

The Redshift connector allows querying and creating tables in an
external `Amazon Redshift <https://aws.amazon.com/redshift/>`_ cluster. This can be used to join data between
different systems like Redshift and Hive, or between two different
Redshift clusters.

Requirements
------------

To connect to Redshift, you need:

* Network access from the Trino coordinator and workers to Redshift.
  Port 5439 is the default port.

Configuration
-------------

To configure the Redshift connector, create a catalog properties file
in ``etc/catalog`` named, for example, ``redshift.properties``, to
mount the Redshift connector as the ``redshift`` catalog.
Create the file with the following contents, replacing the
connection properties as appropriate for your setup:

.. code-block:: text

    connector.name=redshift
    connection-url=jdbc:redshift://example.net:5439/database
    connection-user=root
    connection-password=secret

Multiple Redshift databases or clusters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Redshift connector can only access a single database within
a Redshift cluster. Thus, if you have multiple Redshift databases,
or want to connect to multiple Redshift clusters, you must configure
multiple instances of the Redshift connector.

To add another catalog, simply add another properties file to ``etc/catalog``
with a different name, making sure it ends in ``.properties``. For example,
if you name the property file ``sales.properties``, Trino creates a
catalog named ``sales`` using the configured connector.

.. include:: jdbc-common-configurations.fragment

.. include:: non-transactional-insert.fragment

Querying Redshift
-----------------

The Redshift connector provides a schema for every Redshift schema.
You can see the available Redshift schemas by running ``SHOW SCHEMAS``::

    SHOW SCHEMAS FROM redshift;

If you have a Redshift schema named ``web``, you can view the tables
in this schema by running ``SHOW TABLES``::

    SHOW TABLES FROM redshift.web;

You can see a list of the columns in the ``clicks`` table in the ``web`` database
using either of the following::

    DESCRIBE redshift.web.clicks;
    SHOW COLUMNS FROM redshift.web.clicks;

Finally, you can access the ``clicks`` table in the ``web`` schema::

    SELECT * FROM redshift.web.clicks;

If you used a different name for your catalog properties file, use
that catalog name instead of ``redshift`` in the above examples.

.. _redshift-type-mapping:

Type mapping
------------

.. include:: jdbc-type-mapping.fragment

.. _redshift-sql-support:

SQL support
-----------

The connector provides read access and write access to data and metadata in
Redshift. In addition to the :ref:`globally available
<sql-globally-available>` and :ref:`read operation <sql-read-operations>`
statements, the connector supports the following features:

* :doc:`/sql/insert`
* :doc:`/sql/delete`
* :doc:`/sql/create-table`
* :doc:`/sql/create-table-as`
* :doc:`/sql/drop-table`
* :doc:`/sql/create-schema`
* :doc:`/sql/drop-schema`
* :doc:`/sql/comment`

.. include:: sql-delete-limitation.fragment
