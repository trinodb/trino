==============================
Resource management properties
==============================

.. _prop-resource-query-max-cpu-time:

``query.max-cpu-time``
^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-duration`
* **Default value:** ``1_000_000_000d``

This is the max amount of CPU time that a query can use across the entire
cluster. Queries that exceed this limit are killed.

.. _prop-resource-query-max-memory-per-node:

``query.max-memory-per-node``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-data-size`
* **Default value:** (JVM max memory * 0.3)

This is the max amount of user memory a query can use on a worker.
User memory is allocated during execution for things that are directly
attributable to, or controllable by, a user query. For example, memory used
by the hash tables built during execution, memory used during sorting, etc.
When the user memory allocation of a query on any worker hits this limit,
it is killed.

.. warning::

   The sum of :ref:`prop-resource-query-max-memory-per-node` and
   :ref:`prop-resource-memory-heap-headroom-per-node` must be less than the
   maximum heap size in the JVM on the node. See :ref:`jvm-config`.

.. note::

    Does not apply for queries with task level retries enabled (``retry-policy=TASK``)

.. _prop-resource-query-max-memory:

``query.max-memory``
^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-data-size`
* **Default value:** ``20GB``

This is the max amount of user memory a query can use across the entire cluster.
User memory is allocated during execution for things that are directly
attributable to, or controllable by, a user query. For example, memory used
by the hash tables built during execution, memory used during sorting, etc.
When the user memory allocation of a query across all workers hits this limit
it is killed.

.. warning::

   :ref:`prop-resource-query-max-total-memory` must be greater than
   :ref:`prop-resource-query-max-memory`.

.. note::

    Does not apply for queries with task level retries enabled (``retry-policy=TASK``)

.. _prop-resource-query-max-total-memory:

``query.max-total-memory``
^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-data-size`
* **Default value:** (``query.max-memory`` * 2)

This is the max amount of memory a query can use across the entire cluster,
including revocable memory. When the memory allocated by a query across all
workers hits this limit it is killed. The value of ``query.max-total-memory``
must be greater than ``query.max-memory``.

.. warning::

   :ref:`prop-resource-query-max-total-memory` must be greater than
   :ref:`prop-resource-query-max-memory`.

.. note::

    Does not apply for queries with task level retries enabled (``retry-policy=TASK``)

.. _prop-resource-memory-heap-headroom-per-node:

``memory.heap-headroom-per-node``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-data-size`
* **Default value:** (JVM max memory * 0.3)

This is the amount of memory set aside as headroom/buffer in the JVM heap
for allocations that are not tracked by Trino.

.. warning::

   The sum of :ref:`prop-resource-query-max-memory-per-node` and
   :ref:`prop-resource-memory-heap-headroom-per-node` must be less than the
   maximum heap size in the JVM on the node. See :ref:`jvm-config`.

.. _prop-resource-exchange-deduplication-buffer-size:

``exchange.deduplication-buffer-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-data-size`
* **Default value:** ``32MB``

Size of the buffer used for spooled data during
:doc:`/admin/fault-tolerant-execution`.
