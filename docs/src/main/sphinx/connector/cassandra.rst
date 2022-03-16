===================
Cassandra connector
===================

.. raw:: html

  <img src="../_static/img/cassandra.png" class="connector-logo">

The Cassandra connector allows querying data stored in
`Apache Cassandra <https://cassandra.apache.org/>`_.

Requirements
------------

To connect to Cassandra, you need:

* Cassandra version 2.2 or higher.
* Network access from the Trino coordinator and workers to Cassandra.
  Port 9042 is the default port.

Configuration
-------------

To configure the Cassandra connector, create a catalog properties file
``etc/catalog/cassandra.properties`` with the following contents,
replacing ``host1,host2`` with a comma-separated list of the Cassandra
nodes, used to discovery the cluster topology:

.. code-block:: text

    connector.name=cassandra
    cassandra.contact-points=host1,host2

You also need to set ``cassandra.native-protocol-port``, if your
Cassandra nodes are not using the default port 9042.

Multiple Cassandra clusters
^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can have as many catalogs as you need, so if you have additional
Cassandra clusters, simply add another properties file to ``etc/catalog``
with a different name, making sure it ends in ``.properties``. For
example, if you name the property file ``sales.properties``, Trino
creates a catalog named ``sales`` using the configured connector.

Configuration properties
------------------------

The following configuration properties are available:

================================================== ======================================================================
Property Name                                      Description
================================================== ======================================================================
``cassandra.contact-points``                       Comma-separated list of hosts in a Cassandra cluster. The Cassandra
                                                   driver uses these contact points to discover cluster topology.
                                                   At least one Cassandra host is required.

``cassandra.native-protocol-port``                 The Cassandra server port running the native client protocol,
                                                   defaults to ``9042``.

``cassandra.consistency-level``                    Consistency levels in Cassandra refer to the level of consistency
                                                   to be used for both read and write operations.  More information
                                                   about consistency levels can be found in the
                                                   `Cassandra consistency`_ documentation. This property defaults to
                                                   a consistency level of ``ONE``. Possible values include ``ALL``,
                                                   ``EACH_QUORUM``, ``QUORUM``, ``LOCAL_QUORUM``, ``ONE``, ``TWO``,
                                                   ``THREE``, ``LOCAL_ONE``, ``ANY``, ``SERIAL``, ``LOCAL_SERIAL``.

``cassandra.allow-drop-table``                     Set to ``true`` to allow dropping Cassandra tables from Trino
                                                   via :doc:`/sql/drop-table`, defaults to ``false``.

``cassandra.username``                             Username used for authentication to the Cassandra cluster.
                                                   This is a global setting used for all connections, regardless
                                                   of the user connected to Trino.

``cassandra.password``                             Password used for authentication to the Cassandra cluster.
                                                   This is a global setting used for all connections, regardless
                                                   of the user connected to Trino.

``cassandra.protocol-version``                     It is possible to override the protocol version for older Cassandra clusters.
                                                   By default, the values from the highest protocol version the driver can use.
                                                   Possible values include ``V2``, ``V3`` and ``V4``.
================================================== ======================================================================

.. note::

        If authorization is enabled, ``cassandra.username`` must have enough permissions to perform ``SELECT`` queries on
        the ``system.size_estimates`` table.

.. _Cassandra consistency: https://docs.datastax.com/en/cassandra-oss/2.2/cassandra/dml/dmlConfigConsistency.html

The following advanced configuration properties are available:

============================================================= ======================================================================
Property Name                                                 Description
============================================================= ======================================================================
``cassandra.fetch-size``                                      Number of rows fetched at a time in a Cassandra query.

``cassandra.partition-size-for-batch-select``                 Number of partitions batched together into a single select for a
                                                              single partion key column table.

``cassandra.split-size``                                      Number of keys per split when querying Cassandra.

``cassandra.splits-per-node``                                 Number of splits per node. By default, the values from the
                                                              ``system.size_estimates`` table are used. Only override when
                                                              connecting to Cassandra versions < 2.1.5, which lacks
                                                              the ``system.size_estimates`` table.

``cassandra.batch-size``                                      Maximum number of statements to execute in one batch.

``cassandra.client.read-timeout``                             Maximum time the Cassandra driver waits for an
                                                              answer to a query from one Cassandra node. Note that the underlying
                                                              Cassandra driver may retry a query against more than one node in
                                                              the event of a read timeout. Increasing this may help with queries
                                                              that use an index.

``cassandra.client.connect-timeout``                          Maximum time the Cassandra driver waits to establish
                                                              a connection to a Cassandra node. Increasing this may help with
                                                              heavily loaded Cassandra clusters.

``cassandra.client.so-linger``                                Number of seconds to linger on close if unsent data is queued.
                                                              If set to zero, the socket will be closed immediately.
                                                              When this option is non-zero, a socket lingers that many
                                                              seconds for an acknowledgement that all data was written to a
                                                              peer. This option can be used to avoid consuming sockets on a
                                                              Cassandra server by immediately closing connections when they
                                                              are no longer needed.

``cassandra.retry-policy``                                    Policy used to retry failed requests to Cassandra. This property
                                                              defaults to ``DEFAULT``. Using ``BACKOFF`` may help when
                                                              queries fail with *"not enough replicas"*. The other possible
                                                              values are ``DOWNGRADING_CONSISTENCY`` and ``FALLTHROUGH``.

``cassandra.load-policy.use-dc-aware``                        Set to ``true`` to use ``DCAwareRoundRobinPolicy``,
                                                              defaults to ``false``.

``cassandra.load-policy.dc-aware.local-dc``                   The name of the local datacenter for ``DCAwareRoundRobinPolicy``.

``cassandra.load-policy.dc-aware.used-hosts-per-remote-dc``   Uses the provided number of host per remote datacenter
                                                              as failover for the local hosts for ``DCAwareRoundRobinPolicy``.

``cassandra.load-policy.dc-aware.allow-remote-dc-for-local``  Set to ``true`` to allow to use hosts of
                                                              remote datacenter for local consistency level.

``cassandra.load-policy.use-token-aware``                     Set to ``true`` to use ``TokenAwarePolicy`` (defaults to ``false``).

``cassandra.load-policy.shuffle-replicas``                    Set to ``true`` to use ``TokenAwarePolicy`` with shuffling of replicas,
                                                              defaults to ``false``.

``cassandra.load-policy.allowed-addresses``                   Comma-separated list of hosts to allow.

``cassandra.no-host-available-retry-timeout``                 Retry timeout for ``NoHostAvailableException``, defaults to ``1m``.

``cassandra.speculative-execution.limit``                     The number of speculative executions. This is disabled by default.

``cassandra.speculative-execution.delay``                     The delay between each speculative execution, defaults to ``500ms``.

``cassandra.tls.enabled``                                     Whether TLS security is enabled, defaults to ``false``.

``cassandra.tls.keystore-path``                               Path to the PEM or JKS key store.

``cassandra.tls.truststore-path``                             Path to the PEM or JKS trust store.

``cassandra.tls.keystore-password``                           Password for the key store.

``cassandra.tls.truststore-password``                         Password for the trust store.
============================================================= ======================================================================

Querying Cassandra tables
-------------------------

The ``users`` table is an example Cassandra table from the Cassandra
`Getting Started`_ guide. It can be created along with the ``mykeyspace``
keyspace using Cassandra's cqlsh (CQL interactive terminal):

.. _Getting Started: https://cassandra.apache.org/doc/latest/cassandra/getting_started/index.html

.. code-block:: text

    cqlsh> CREATE KEYSPACE mykeyspace
       ... WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
    cqlsh> USE mykeyspace;
    cqlsh:mykeyspace> CREATE TABLE users (
                  ...   user_id int PRIMARY KEY,
                  ...   fname text,
                  ...   lname text
                  ... );

This table can be described in Trino::

    DESCRIBE cassandra.mykeyspace.users;

.. code-block:: text

     Column  |  Type   | Extra | Comment
    ---------+---------+-------+---------
     user_id | bigint  |       |
     fname   | varchar |       |
     lname   | varchar |       |
    (3 rows)

This table can then be queried in Trino::

    SELECT * FROM cassandra.mykeyspace.users;

Data types
----------

The data types mappings are as follows:

================  ======
Cassandra         Trino
================  ======
ASCII             VARCHAR
BIGINT            BIGINT
BLOB              VARBINARY
BOOLEAN           BOOLEAN
DATE              DATE
DECIMAL           DOUBLE
DOUBLE            DOUBLE
FLOAT             REAL
INET              VARCHAR(45)
INT               INTEGER
LIST<?>           VARCHAR
MAP<?, ?>         VARCHAR
SET<?>            VARCHAR
SMALLINT          SMALLINT
TEXT              VARCHAR
TIMESTAMP         TIMESTAMP(3) WITH TIME ZONE
TIMEUUID          UUID
TINYINT           TINYINT
TUPLE             ROW with anonymous fields
UUID              UUID
UDT               ROW with field names
VARCHAR           VARCHAR
VARINT            VARCHAR
================  ======

Any collection (LIST/MAP/SET) can be designated as FROZEN, and the value is
mapped to VARCHAR. Additionally, blobs have the limitation that they cannot be empty.

Types not mentioned in the table above are not supported.

Partition keys can only be of the following types:

* ASCII
* TEXT
* VARCHAR
* BIGINT
* BOOLEAN
* DOUBLE
* INET
* INT
* FLOAT
* DECIMAL
* TIMESTAMP
* UUID
* TIMEUUID

Limitations
-----------

* Queries without filters containing the partition key result in fetching all partitions.
  This causes a full scan of the entire data set, and is therefore much slower compared to a similar
  query with a partition key as a filter.
* ``IN`` list filters are only allowed on index (that is, partition key or clustering key) columns.
* Range (``<`` or ``>`` and ``BETWEEN``) filters can be applied only to the partition keys.

.. _cassandra-sql-support:

SQL support
-----------

The connector provides read and write access to data and metadata in
the Cassandra database. In addition to the :ref:`globally available
<sql-globally-available>` and :ref:`read operation <sql-read-operations>`
statements, the connector supports the following features:

* :doc:`/sql/insert`
* :doc:`/sql/delete` see :ref:`sql-delete-limitation`
* :doc:`/sql/truncate`
* :doc:`/sql/create-table`
* :doc:`/sql/create-table-as`
* :doc:`/sql/drop-table`

.. _sql-delete-limitation:

SQL delete limitation
^^^^^^^^^^^^^^^^^^^^^

``DELETE`` is only supported if the ``WHERE`` clause matches entire partitions.
