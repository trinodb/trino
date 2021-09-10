============================
Memory management properties
============================

``query.max-memory-per-node``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-data-size`
* **Default value:** (JVM max memory * 0.1)

This is the max amount of user memory a query can use on a worker.
User memory is allocated during execution for things that are directly
attributable to, or controllable by, a user query. For example, memory used
by the hash tables built during execution, memory used during sorting, etc.
When the user memory allocation of a query on any worker hits this limit,
it is killed.

``query.max-total-memory-per-node``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-data-size`
* **Default value:** (JVM max memory * 0.3)

This is the max amount of user and system memory a query can use on a worker.
System memory is allocated during execution for things that are not directly
attributable to, or controllable by, a user query. For example, memory allocated
by the readers, writers, network buffers, etc. When the sum of the user and
system memory allocated by a query on any worker hits this limit, it is killed.
The value of ``query.max-total-memory-per-node`` must be greater than
``query.max-memory-per-node``.

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

``query.max-total-memory``
^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-data-size`
* **Default value:** (``query.max-memory`` * 2)

This is the max amount of user and system memory a query can use across the entire cluster.
System memory is allocated during execution for things that are not directly
attributable to, or controllable by, a user query. For example, memory allocated
by the readers, writers, network buffers, etc. When the sum of the user and
system memory allocated by a query across all workers hits this limit it is
killed. The value of ``query.max-total-memory`` must be greater than
``query.max-memory``.

``memory.heap-headroom-per-node``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-data-size`
* **Default value:** (JVM max memory * 0.3)

This is the amount of memory set aside as headroom/buffer in the JVM heap
for allocations that are not tracked by Trino.
