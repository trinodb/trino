==================
General properties
==================

``join-distribution-type``
^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-string`
* **Allowed values:** ``AUTOMATIC``, ``PARTITIONED``, ``BROADCAST``
* **Default value:** ``AUTOMATIC``

The type of distributed join to use.  When set to ``PARTITIONED``, Trino
uses hash distributed joins.  When set to ``BROADCAST``, it broadcasts the
right table to all nodes in the cluster that have data from the left table.
Partitioned joins require redistributing both tables using a hash of the join key.
This can be slower, sometimes substantially, than broadcast joins, but allows much
larger joins. In particular broadcast joins are faster, if the right table is
much smaller than the left.  However, broadcast joins require that the tables on the right
side of the join after filtering fit in memory on each node, whereas distributed joins
only need to fit in distributed memory across all nodes. When set to ``AUTOMATIC``,
Trino makes a cost based decision as to which distribution type is optimal.
It considers switching the left and right inputs to the join.  In ``AUTOMATIC``
mode, Trino defaults to hash distributed joins if no cost could be computed, such as if
the tables do not have statistics. This can be specified on a per-query basis using
the ``join_distribution_type`` session property.

``redistribute-writes``
^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-boolean`
* **Default value:** ``true``

This property enables redistribution of data before writing. This can
eliminate the performance impact of data skew when writing by hashing it
across nodes in the cluster. It can be disabled, when it is known that the
output data set is not skewed, in order to avoid the overhead of hashing and
redistributing all the data across the network. This can be specified
on a per-query basis using the ``redistribute_writes`` session property.

``protocol.v1.alternate-header-name``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**Type:** ``string``

The 351 release of Trino changes the HTTP client protocol headers to start with
``X-Trino-``. Clients for versions 350 and lower expect the HTTP headers to 
start with ``X-Presto-``, while newer clients expect ``X-Trino-``. You can support these
older clients by setting this property to ``Presto``.

The preferred approach to migrating from versions earlier than 351 is to update
all clients together with the release, or immediately afterwards, and then
remove usage of this property.

Ensure to use this only as a temporary measure to assist in your migration
efforts.

