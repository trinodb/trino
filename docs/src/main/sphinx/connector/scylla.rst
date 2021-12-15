================
Scylla connector
================

The Scylla connector allows querying data stored in
`Scylla <https://www.scylladb.com//>`_.

Requirements
------------

To connect to Scylla, you need:

* Scylla version 3.0.0 or higher.
* Network access from the Trino coordinator and workers to Scylla.
  Port 9042 is the default port.

Configuration
-------------

To configure the Scylla connector, create a catalog properties file
``etc/catalog/scylla.properties`` with the following contents,
replacing ``host1,host2`` with a comma-separated list of the Scylla
nodes, used to discovery the cluster topology:

.. code-block:: text

    connector.name=scylla
    cassandra.contact-points=host1,host2

You also need to set ``cassandra.native-protocol-port``, if your
Scylla nodes are not using the default port 9042.

Compatibility with Cassandra connector
--------------------------------------

The Scylla connector is very similar to the Cassandra connector with the
only difference being the underlying driver.
See :doc:`Cassandra connector <cassandra>` for more details.
