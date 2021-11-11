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

* **Type:** :ref:`prop-type-boolean`
* **Default value:** ``false``

Enable writer scaling. This can be specified on a per-query basis
using the ``scale_writers`` session property.

``writer-min-size``
^^^^^^^^^^^^^^^^^^^
* **Type:** :ref:`prop-type-data-size`
* **Default value:** ``32MB``

The minimum amount of data that must be written by a writer task before
another writer is eligible to be added. Each writer task may have multiple
writers, controlled by ``task.writer-count``, thus this value is effectively
divided by the number of writers per task. This can be specified on a
per-query basis using the ``writer_min_size`` session property.
