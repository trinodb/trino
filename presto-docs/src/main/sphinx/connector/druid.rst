===============
Druid Connector
===============

The Druid connector allows querying an `Apache Druid <https://druid.apache.org/>`_
database from Presto.

Configuration
-------------

Create a catalog properties file that specifies the Druid connector by setting
the ``connector.name`` to ``druid`` and configuring the ``connection-url`` with
the JDBC string to connect to Druid.

For example, to access a database as ``druiddb``, create the file
``etc/catalog/druiddb.properties``. Replace ``BROKER:8082`` with the correct
host and port of your Druid broker.

.. code-block:: properties

    connector.name=druid
    connection-url=jdbc:avatica:remote:url=http://BROKER:8082/druid/v2/sql/avatica/

You can add authentication details to connect to a Druid deployment that is
secured by basic authentication by updating the URL and adding credentials:

.. code-block:: properties

    connection-url=jdbc:avatica:remote:url=http://BROKER:port/druid/v2/sql/avatica/;authentication=BASIC
    connection-user=root
    connection-password=secret

Now you can access your Druid database in Presto with the ``druiddb`` catalog
name from the properties file.
