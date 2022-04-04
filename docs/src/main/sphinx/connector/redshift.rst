==================
Redshift connector
==================

.. raw:: html

  <img src="../_static/img/redshift.png" class="connector-logo">

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

.. _redshift-tls:

Connection security
^^^^^^^^^^^^^^^^^^^

If you have TLS configured with a globally-trusted certificate installed on your
data source, you can enable TLS between your cluster and the data
source by appending a parameter to the JDBC connection string set in the
``connection-url`` catalog configuration property.

For example, on version 2.1 of the Redshift JDBC driver, TLS/SSL is enabled by
default with the ``SSL`` parameter. You can disable or further configure TLS
by appending parameters to the ``connection-url`` configuration property:

.. code-block:: properties

  connection-url=jdbc:redshift://example.net:5439/database;SSL=TRUE;

For more information on TLS configuration options, see the `Redshift JDBC driver
documentation
<https://docs.aws.amazon.com/redshift/latest/mgmt/jdbc20-configuration-options.html#jdbc20-ssl-option>`_.

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

.. include:: jdbc-procedures.fragment

.. include:: jdbc-case-insensitive-matching.fragment

.. include:: non-transactional-insert.fragment

.. include:: jdbc-extra-credentials.fragment

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
* :doc:`/sql/truncate`
* :ref:`sql-schema-table-management`

.. include:: sql-delete-limitation.fragment

.. include:: alter-table-limitation.fragment

.. include:: alter-schema-limitation.fragment
