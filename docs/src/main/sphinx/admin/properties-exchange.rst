===================
Exchange properties
===================

Exchanges transfer data between Trino nodes for different stages of
a query. Adjusting these properties may help to resolve inter-node
communication issues or improve network utilization.

``exchange.client-threads``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-integer`
* **Minimum value:** ``1``
* **Default value:** ``25``

Number of threads used by exchange clients to fetch data from other Trino
nodes. A higher value can improve performance for large clusters or clusters
with very high concurrency, but excessively high values may cause a drop
in performance due to context switches and additional memory usage.

``exchange.concurrent-request-multiplier``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-integer`
* **Minimum value:** ``1``
* **Default value:** ``3``

Multiplier determining the number of concurrent requests relative to
available buffer memory. The maximum number of requests is determined
using a heuristic of the number of clients that can fit into available
buffer space, based on average buffer usage per request times this
multiplier. For example, with an ``exchange.max-buffer-size`` of ``32 MB``
and ``20 MB`` already used and average size per request being ``2MB``,
the maximum number of clients is
``multiplier * ((32MB - 20MB) / 2MB) = multiplier * 6``. Tuning this
value adjusts the heuristic, which may increase concurrency and improve
network utilization.

``exchange.data-integrity-verification``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-string`
* **Allowed values:** ``NONE``, ``ABORT``, ``RETRY``
* **Default value:** ``ABORT``

Configure the resulting behavior of data integrity issues. By default,
``ABORT`` causes queries to be aborted when data integrity issues are
detected as part of the built-in verification. Setting the property to
``NONE`` disables the verification. ``RETRY`` causes the data exchange to be
repeated when integrity issues are detected.

``exchange.max-buffer-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-data-size`
* **Default value:** ``32MB``

Size of buffer in the exchange client that holds data fetched from other
nodes before it is processed. A larger buffer can increase network
throughput for larger clusters, and thus decrease query processing time,
but reduces the amount of memory available for other usages.

``exchange.max-response-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-data-size`
* **Minimum value:** ``1MB``
* **Default value:** ``16MB``

Maximum size of a response returned from an exchange request. The response
is placed in the exchange client buffer, which is shared across all
concurrent requests for the exchange.

Increasing the value may improve network throughput, if there is high
latency. Decreasing the value may improve query performance for large
clusters as it reduces skew, due to the exchange client buffer holding
responses for more tasks, rather than hold more data from fewer tasks.

``sink.max-buffer-size``
^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-data-size`
* **Default value:** ``32MB``

Output buffer size for task data that is waiting to be pulled by upstream
tasks. If the task output is hash partitioned, then the buffer is
shared across all of the partitioned consumers. Increasing this value may
improve network throughput for data transferred between stages, if the
network has high latency, or if there are many nodes in the cluster.

``sink.max-broadcast-buffer-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type** ``data size``
* **Default value:** ``200MB``

Broadcast output buffer size for task data that is waiting to be pulled by
upstream tasks. The broadcast buffer is used to store and transfer build side
data for replicated joins. If the buffer is too small, it prevents scaling of
join probe side tasks, when new nodes are added to the cluster.
