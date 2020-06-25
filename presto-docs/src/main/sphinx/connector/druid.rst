===============
Druid Connector
===============

The Druid Connector allows querying `Apache Druid <https://druid.apache.org/>`_ data from Presto.

Configuration
-------------

To configure the Druid connector, create a catalog properties file
``etc/catalog/druid.properties`` with the following contents, replacing
``BROKER:8082`` with the correct host and port of your Druid Broker.

.. code-block:: none

    connector.name=druid
    connection-url=jdbc:avatica:remote:url=http://BROKER:8082/druid/v2/sql/avatica/

