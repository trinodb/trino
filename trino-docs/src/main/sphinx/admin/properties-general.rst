==================
General Properties
==================

``join-distribution-type``
^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``string``
* **Allowed values:** ``AUTOMATIC``, ``PARTITIONED``, ``BROADCAST``
* **Default value:** ``AUTOMATIC``

The type of distributed join to use.  When set to ``PARTITIONED``, Presto
uses hash distributed joins.  When set to ``BROADCAST``, it broadcasts the
right table to all nodes in the cluster that have data from the left table.
Partitioned joins require redistributing both tables using a hash of the join key.
This can be slower, sometimes substantially, than broadcast joins, but allows much
larger joins. In particular broadcast joins are faster, if the right table is
much smaller than the left.  However, broadcast joins require that the tables on the right
side of the join after filtering fit in memory on each node, whereas distributed joins
only need to fit in distributed memory across all nodes. When set to ``AUTOMATIC``,
Presto makes a cost based decision as to which distribution type is optimal.
It considers switching the left and right inputs to the join.  In ``AUTOMATIC``
mode, Presto defaults to hash distributed joins if no cost could be computed, such as if
the tables do not have statistics. This can be specified on a per-query basis using
the ``join_distribution_type`` session property.

``redistribute-writes``
^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``true``

This property enables redistribution of data before writing. This can
eliminate the performance impact of data skew when writing by hashing it
across nodes in the cluster. It can be disabled, when it is known that the
output data set is not skewed, in order to avoid the overhead of hashing and
redistributing all the data across the network. This can be specified
on a per-query basis using the ``redistribute_writes`` session property.
