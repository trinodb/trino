=============================
Write partitioning properties
=============================

``use-preferred-write-partitioning``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
* **Type:** ``boolean``
* **Default value:** ``true``

Enable preferred write partitioning. When set to ``true`` and more than the
minimum number of partitions, set in ``preferred-write-partitioning-min-number-of-partitions``,
are written, each partition is written by a separate writer. As a result, for some connectors such as the
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
If the threshold value is ``1`` then ``preferred write partitioning`` is always
used.
