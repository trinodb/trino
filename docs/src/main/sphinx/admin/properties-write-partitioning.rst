=============================
Write partitioning properties
=============================

.. _preferred-write-partitioning:

``use-preferred-write-partitioning``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
* **Type:** :ref:`prop-type-boolean`
* **Default value:** ``true``

Enable preferred write partitioning. When set to ``true`` and more than the
minimum number of partitions, set in ``preferred-write-partitioning-min-number-of-partitions``,
are written, each partition is written by a separate writer. As a result, for some connectors such as the
Hive connector, only a single new file is written per partition, instead of
multiple files. Partition writer assignments are distributed across worker
nodes for parallel processing. ``use-preferred-write-partitioning`` can be
specified on a per-query basis using the ``use_preferred_write_partitioning``
session property.

``preferred-write-partitioning-min-number-of-partitions``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
* **Type:** :ref:`prop-type-integer`
* **Default value:** ``50``

Use the connector's preferred write partitioning when the optimizer's estimate
of the number of partitions that will be written by the query is greater than
the configured value. If the number of partitions cannot be estimated from the
statistics, then preferred write partitioning is not used.
If the threshold value is ``1`` then preferred write partitioning is always used.
``preferred-write-partitioning-min-number-of-partitions`` can be specified on a
per-query basis using the ``preferred_write_partitioning_min_number_of_partitions``
session property.
