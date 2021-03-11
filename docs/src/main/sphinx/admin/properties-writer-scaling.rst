=========================
Writer scaling properties
=========================

By default, the number of writer tasks is static. Enabling writer scaling allows
Trino to dynamically scale out the number of writer tasks rather than
allocating a fixed number of tasks. Additional tasks are added when the average
amount of physical data per writer is above a minimum threshold, but only if the
query is bottlenecked on writing.

Writer scaling is useful with connectors like Hive that produce one or more
files per writer -- reducing the number of writers results in a larger average
file size. However, writer scaling can have a small impact on query wall time
due to the decreased writer parallelism while the writer count ramps up to match
the needs of the query.

``scale-writers``
^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``false``

Enable writer scaling. This can be specified on a per-query basis
using the ``scale_writers`` session property.

``writer-min-size``
^^^^^^^^^^^^^^^^^^^
* **Type:** ``data size``
* **Default value:** ``32MB``

The minimum amount of data that must be written by a writer task before
another writer is eligible to be added. Each writer task may have multiple
writers, controlled by ``task.writer-count``, thus this value is effectively
divided by the number of writers per task. This can be specified on a
per-query basis using the ``writer_min_size`` session property.

``use-preferred-write-partitioning``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
* **Type:** ``boolean``
* **Default value:** ``true``

Enable preferred write partitioning. When set to ``true`` each partition is
written by a separate writer. As a result, for some connectors such as the
Hive connector, only a single new file is written per partition, instead of
multiple files. Partition writer assignments are distributed across worker
nodes for parallel processing.

``preferred-write-partitioning-min-number-of-partitions``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
* **Type:** ``integer``
* **Default value:** ``50``

The minimum number of written partitions that is required to use connector
``preferred write partitioning``. If the number of partitions cannot be
estimated from the statistics, then preferred write partitioning is not used.
If the threshold value is less than or equal to ``1`` then ``preferred write
partitioning`` is always used.
