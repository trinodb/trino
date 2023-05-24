=========================
Writer scaling properties
=========================

Writer scaling allows Trino to dynamically scale out the number of writer tasks
rather than allocating a fixed number of tasks. Additional tasks are added when
the average amount of physical data per writer is above a minimum threshold, but
only if the query is bottlenecked on writing.

Writer scaling is useful with connectors like Hive that produce one or more
files per writer -- reducing the number of writers results in a larger average
file size. However, writer scaling can have a small impact on query wall time
due to the decreased writer parallelism while the writer count ramps up to match
the needs of the query.

``scale-writers``
^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-boolean`
* **Default value:** ``true``

Enable writer scaling by dynamically increasing the number of writer tasks on
the cluster. This can be specified on a per-query basis using the ``scale_writers``
session property.

.. _prop-task-scale-writers:

``task.scale-writers.enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-boolean`
* **Default value:** ``true``

Enable scaling the number of concurrent writers within a task. The maximum writer
count per task for scaling is ``task.scale-writers.max-writer-count``. Additional
writers are added only when the average amount of physical data written per writer
is above the minimum threshold of ``writer-min-size`` and query is bottlenecked on
writing. This can be specified on a per-query basis using the ``task_scale_writers_enabled``
session property.

.. _prop-task-scale-writers-max-writer-count:

``task.scale-writers.max-writer-count``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-integer`
* **Default value:** The number of physical CPUs of the node with a maximum of 32

Maximum number of concurrent writers per task upto which the task can be scaled when
``task.scale-writers.enabled`` is set. Increasing this value may improve the
performance of writes when the query is bottlenecked on writing. Setting this too high
may cause the cluster to become overloaded due to excessive resource utilization.

``writer-min-size``
^^^^^^^^^^^^^^^^^^^
* **Type:** :ref:`prop-type-data-size`
* **Default value:** ``32MB``

The minimum amount of data that must be written by a writer task before
another writer is eligible to be added. Each writer task may have multiple
writers, controlled by ``task.writer-count``, thus this value is effectively
divided by the number of writers per task. This can be specified on a
per-query basis using the ``writer_min_size`` session property.
