=============================
Write partitioning properties
=============================

.. _preferred-write-partitioning:

``use-preferred-write-partitioning``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
* **Type:** :ref:`prop-type-boolean`
* **Default value:** ``true``

Enable preferred write partitioning. When set to ``true``, each partition is
written by a separate writer. For some connectors such as the Hive connector,
only a single new file is written per partition, instead of multiple files.
Partition writer assignments are distributed across worker nodes for parallel
processing. ``use-preferred-write-partitioning`` can be specified on a per-query
basis using the ``use_preferred_write_partitioning`` session property.
