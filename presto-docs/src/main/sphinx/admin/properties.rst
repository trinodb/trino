====================
Properties Reference
====================

This section describes the most important config properties, that
may be used to tune Presto or alter its behavior when required.

.. contents::
    :local:
    :backlinks: none
    :depth: 1

General Properties
------------------

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

.. _tuning-memory:

Memory Management Properties
----------------------------

``query.max-memory-per-node``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``data size``
    * **Default value:** ``JVM max memory * 0.1``

    This is the max amount of user memory a query can use on a worker.
    User memory is allocated during execution for things that are directly
    attributable to, or controllable by, a user query. For example, memory used
    by the hash tables built during execution, memory used during sorting, etc.
    When the user memory allocation of a query on any worker hits this limit,
    it is killed.

``query.max-total-memory-per-node``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``data size``
    * **Default value:** ``JVM max memory * 0.3``

    This is the max amount of user and system memory a query can use on a worker.
    System memory is allocated during execution for things that are not directly
    attributable to, or controllable by, a user query. For example, memory allocated
    by the readers, writers, network buffers, etc. When the sum of the user and
    system memory allocated by a query on any worker hits this limit, it is killed.
    The value of ``query.max-total-memory-per-node`` must be greater than
    ``query.max-memory-per-node``.

``query.max-memory``
^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``data size``
    * **Default value:** ``20GB``

    This is the max amount of user memory a query can use across the entire cluster.
    User memory is allocated during execution for things that are directly
    attributable to, or controllable by, a user query. For example, memory used
    by the hash tables built during execution, memory used during sorting, etc.
    When the user memory allocation of a query across all workers hits this limit
    it is killed.

``query.max-total-memory``
^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``data size``
    * **Default value:** ``query.max-memory * 2``

    This is the max amount of user and system memory a query can use across the entire cluster.
    System memory is allocated during execution for things that are not directly
    attributable to, or controllable by, a user query. For example, memory allocated
    by the readers, writers, network buffers, etc. When the sum of the user and
    system memory allocated by a query across all workers hits this limit it is
    killed. The value of ``query.max-total-memory`` must be greater than
    ``query.max-memory``.

``memory.heap-headroom-per-node``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``data size``
    * **Default value:** ``JVM max memory * 0.3``

    This is the amount of memory set aside as headroom/buffer in the JVM heap
    for allocations that are not tracked by Presto.


Query Management Properties
---------------------------

``query.max-execution-time``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``duration``
    * **Default value:** ``100 days``
    * **Session property:** ``query_max_execution_time``

    The maxiumum allowed time for a query to be actively executing on the
    cluster, before it is terminated. Compared to the run time below, execution
    time does not include analysis, query planning or wait times in a queue.

``query.max-run-time``
^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``duration``
    * **Default value:** ``100 days``
    * **Session property:** ``query_max_run_time``

    The maxiumum allowed time for a query to be processed on the cluster, before
    it is terminated. The time includes time for analysis and planning, but also
    time spend in a queue waiting, so essentially this is the time allowed for a
    query to exist since creation.

``query.max-history``
^^^^^^^^^^^^^^^^^^^^^
    * **Type:** ``integer``
    * **Default value:** ``100``

    The maximum number of queries to keep in the query history to provide
    statistics and other information. If this amount is reached, queries are
    removed based on age.

``query.min-expire-age``
^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``duration``
    * **Default value:** ``15 min``

    The minimal age of a query in the history before it is expired. An expired
    query is removed from the query history buffer and no longer available in
    the :doc:`/admin/web-interface`.

.. _tuning-spilling:

Spilling Properties
-------------------

``spill-enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``boolean``
    * **Default value:** ``false``

    Try spilling memory to disk to avoid exceeding memory limits for the query.

    Spilling works by offloading memory to disk. This process can allow a query with a large memory
    footprint to pass at the cost of slower execution times. Spilling is supported for
    aggregations, joins (inner and outer), sorting, and window functions. This property does not
    reduce memory usage required for other join types.

    This config property can be overridden by the ``spill_enabled`` session property.


``spill-order-by``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``boolean``
    * **Default value:** ``true``

    Try spilling memory to disk to avoid exceeding memory limits for the query when running sorting operators.
    This property must be used in conjunction with the ``spill-enabled`` property.

    This config property can be overridden by the ``spill_order_by`` session property.

``spill-window-operator``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``boolean``
    * **Default value:** ``true``

    Try spilling memory to disk to avoid exceeding memory limits for the query when running window operators;
    This property must be used in conjunction with the ``spill-enabled`` property.

    This config property can be overridden by the ``spill_window_operator`` session property.

``spiller-spill-path``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``string``
    * **No default value.** Must be set when spilling is enabled

    Directory where spilled content is written. It can be a comma separated
    list to spill simultaneously to multiple directories, which helps to utilize
    multiple drives installed in the system.

    It is not recommended to spill to system drives. Most importantly, do not spill
    to the drive on which the JVM logs are written, as disk overutilization might
    cause JVM to pause for lengthy periods, causing queries to fail.

``spiller-max-used-space-threshold``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``double``
    * **Default value:** ``0.9``

    If disk space usage ratio of a given spill path is above this threshold,
    this spill path is not eligible for spilling.

``spiller-threads``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``integer``
    * **Default value:** ``4``

    Number of spiller threads. Increase this value if the default is not able
    to saturate the underlying spilling device (for example, when using RAID).

``max-spill-per-node``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``data size``
    * **Default value:** ``100 GB``

    Max spill space to be used by all queries on a single node.

``query-max-spill-per-node``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``data size``
    * **Default value:** ``100 GB``

    Max spill space to be used by a single query on a single node.

``aggregation-operator-unspill-memory-limit``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``data size``
    * **Default value:** ``4 MB``

    Limit for memory used for unspilling a single aggregation operator instance.

``spill-compression-enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``boolean``
    * **Default value:** ``false``

    Enables data compression for pages spilled to disk.

``spill-encryption-enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``boolean``
    * **Default value:** ``false``

    Enables using a randomly generated secret key (per spill file) to encrypt and decrypt
    data spilled to disk.


Exchange Properties
-------------------

Exchanges transfer data between Presto nodes for different stages of
a query. Adjusting these properties may help to resolve inter-node
communication issues or improve network utilization.

``exchange.client-threads``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``integer``
    * **Minimum value:** ``1``
    * **Default value:** ``25``

    Number of threads used by exchange clients to fetch data from other Presto
    nodes. A higher value can improve performance for large clusters or clusters
    with very high concurrency, but excessively high values may cause a drop
    in performance due to context switches and additional memory usage.

``exchange.concurrent-request-multiplier``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``integer``
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

``exchange.max-buffer-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``data size``
    * **Default value:** ``32MB``

    Size of buffer in the exchange client that holds data fetched from other
    nodes before it is processed. A larger buffer can increase network
    throughput for larger clusters, and thus decrease query processing time,
    but reduces the amount of memory available for other usages.

``exchange.max-response-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``data size``
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

    * **Type:** ``data size``
    * **Default value:** ``32MB``

    Output buffer size for task data that is waiting to be pulled by upstream
    tasks. If the task output is hash partitioned, then the buffer is
    shared across all of the partitioned consumers. Increasing this value may
    improve network throughput for data transferred between stages, if the
    network has high latency, or if there are many nodes in the cluster.

.. _task-properties:

Task Properties
---------------

``task.concurrency``
^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``integer``
    * **Restrictions:** must be a power of two
    * **Default value:** ``16``

    Default local concurrency for parallel operators, such as joins and aggregations.
    This value should be adjusted up or down based on the query concurrency and worker
    resource utilization. Lower values are better for clusters that run many queries
    concurrently, because the cluster is already utilized by all the running
    queries, so adding more concurrency results in slow downs due to context
    switching and other overhead. Higher values are better for clusters that only run
    one or a few queries at a time. This can also be specified on a per-query basis
    using the ``task_concurrency`` session property.

``task.http-response-threads``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``integer``
    * **Minimum value:** ``1``
    * **Default value:** ``100``

    Maximum number of threads that may be created to handle HTTP responses. Threads are
    created on demand and are cleaned up when idle, thus there is no overhead to a large
    value, if the number of requests to be handled is small. More threads may be helpful
    on clusters with a high number of concurrent queries, or on clusters with hundreds
    or thousands of workers.

``task.http-timeout-threads``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``integer``
    * **Minimum value:** ``1``
    * **Default value:** ``3``

    Number of threads used to handle timeouts when generating HTTP responses. This value
    should be increased if all the threads are frequently in use. This can be monitored
    via the ``io.prestosql.server:name=AsyncHttpExecutionMBean:TimeoutExecutor``
    JMX object. If ``ActiveCount`` is always the same as ``PoolSize``, increase the
    number of threads.

``task.info-update-interval``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``duration``
    * **Minimum value:** ``1ms``
    * **Maximum value:** ``10s``
    * **Default value:** ``3s``

    Controls staleness of task information, which is used in scheduling. Larger values
    can reduce coordinator CPU load, but may result in suboptimal split scheduling.

``task.max-partial-aggregation-memory``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``data size``
    * **Default value:** ``16MB``

    Maximum size of partial aggregation results for distributed aggregations. Increasing this
    value can result in less network transfer and lower CPU utilization, by allowing more
    groups to be kept locally before being flushed, at the cost of additional memory usage.

``task.max-worker-threads``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``integer``
    * **Default value:** ``Node CPUs * 2``

    Sets the number of threads used by workers to process splits. Increasing this number
    can improve throughput, if worker CPU utilization is low and all the threads are in use,
    but it causes increased heap space usage. Setting the value too high may cause a drop
    in performance due to a context switching. The number of active threads is available
    via the ``RunningSplits`` property of the
    ``io.prestosql.execution.executor:name=TaskExecutor.RunningSplits`` JMX object.

``task.min-drivers``
^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``integer``
    * **Default value:** ``task.max-worker-threads * 2``

    The target number of running leaf splits on a worker. This is a minimum value because
    each leaf task is guaranteed at least ``3`` running splits. Non-leaf tasks are also
    guaranteed to run in order to prevent deadlocks. A lower value may improve responsiveness
    for new tasks, but can result in underutilized resources. A higher value can increase
    resource utilization, but uses additional memory.

``task.writer-count``
^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``integer``
    * **Restrictions:** must be a power of two
    * **Default value:** ``1``

    The number of concurrent writer threads per worker per query. Increasing this value may
    increase write speed, especially when a query is not I/O bound and can take advantage
    of additional CPU for parallel writes. Some connectors can be bottlenecked on CPU when
    writing due to compression or other factors. Setting this too high may cause the cluster
    to become overloaded due to excessive resource utilization. This can also be specified on
    a per-query basis using the ``task_writer_count`` session property.

Writer Scaling Properties
-------------------------

By default, the number of writer tasks is static. Enabling writer scaling allows
Presto to dynamically scale out the number of writer tasks rather than
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
    divided by the the number of writers per task. This can be specified on a
    per-query basis using the ``writer_min_size`` session property.

.. _node-scheduler-properties:

Node Scheduler Properties
-------------------------

``node-scheduler.max-splits-per-node``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``integer``
    * **Default value:** ``100``

    The target value for the total number of splits that can be running for
    each worker node.

    Using a higher value is recommended, if queries are submitted in large batches
    (e.g., running a large group of reports periodically), or for connectors that
    produce many splits that complete quickly. Increasing this value may improve
    query latency, by ensuring that the workers have enough splits to keep them
    fully utilized.

    Setting this too high wastes memory and may result in lower performance
    due to splits not being balanced across workers. Ideally, it should be set
    such that there is always at least one split waiting to be processed, but
    not higher.

``node-scheduler.max-pending-splits-per-task``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``integer``
    * **Default value:** ``10``

    The number of outstanding splits that can be queued for each worker node
    for a single stage of a query, even when the node is already at the limit for
    total number of splits. Allowing a minimum number of splits per stage is
    required to prevent starvation and deadlocks.

    This value must be smaller than ``node-scheduler.max-splits-per-node``,
    is usually increased for the same reasons, and has similar drawbacks
    if set too high.

``node-scheduler.min-candidates``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``integer``
    * **Minimum value:** ``1``
    * **Default value:** ``10``

    The minimum number of candidate nodes that are evaluated by the
    node scheduler when choosing the target node for a split. Setting
    this value too low may prevent splits from being properly balanced
    across all worker nodes. Setting it too high may increase query
    latency and increase CPU usage on the coordinator.

``node-scheduler.policy``
^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``string``
    * **Allowed values:** ``uniform``, ``topology``
    * **Default value:** ``uniform``

    Sets the node scheduler policy to use when scheduling splits. ``uniform``  attempts
    to schedule splits on the host where the data is located, while maintaining a uniform
    distribution across all hosts. ``topology`` tries to schedule splits according to
    the topology distance between nodes and splits. It is recommended to use ``uniform``
    for clusters where distributed storage runs on the same nodes as Presto workers.

``node-scheduler.network-topology.segments``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``string``
    * **Default value:** ``machine``

    A comma-separated string describing the meaning of each segment of a network location.
    For example, setting ``region,rack,machine`` means a network location contains three segments.

``node-scheduler.network-topology.type``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``string``
    * **Allowed values:** ``flat``, ``file``
    * **Default value:** ``flat``

    Sets the network topology type. To use this option, ``node-scheduler.policy`` must be set to
    ``topology``. ``flat`` has only one segment, with one value for each machine.
    ``file`` loads the topology from a file as described below.

``node-scheduler.network-topology.file``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``string``

    Load the network topology from a file. To use this option, ``node-scheduler.network-topology.type``
    must be set to ``file``. Each line contains a mapping between a host name and a
    network location, separated by whitespace. Network location must begin with a leading
    ``/`` and segments are separated by a ``/``.

.. code-block:: none

    192.168.0.1 /region1/rack1/machine1
    192.168.0.2 /region1/rack1/machine2
    hdfs01.example.com /region2/rack2/machine3

``node-scheduler.network-topology.refresh-period``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``duration``
    * **Minimum value:** ``1ms``
    * **Default value:** ``5m``

    Controls how often the network topology file is reloaded.  To use this option,
    ``node-scheduler.network-topology.type`` must be set to ``file``.

Optimizer Properties
--------------------

``optimizer.dictionary-aggregation``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``boolean``
    * **Default value:** ``false``

    Enables optimization for aggregations on dictionaries. This can also be specified
    on a per-query basis using the ``dictionary_aggregation`` session property.

``optimizer.optimize-hash-generation``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``boolean``
    * **Default value:** ``true``

    Compute hash codes for distribution, joins, and aggregations early during execution,
    allowing result to be shared between operations later in the query. This can reduce
    CPU usage by avoiding computing the same hash multiple times, but at the cost of
    additional network transfer for the hashes. In most cases it decreases overall
    query processing time. This can also be specified on a per-query basis using the
    ``optimize_hash_generation`` session property.

    It is often helpful to disable this property, when using :doc:`/sql/explain` in order
    to make the query plan easier to read.

``optimizer.optimize-metadata-queries``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``boolean``
    * **Default value:** ``false``

    Enable optimization of some aggregations by using values that are stored as metadata.
    This allows Presto to execute some simple queries in constant time. Currently, this
    optimization applies to ``max``, ``min`` and ``approx_distinct`` of partition
    keys and other aggregation insensitive to the cardinality of the input,including
    ``DISTINCT`` aggregates. Using this may speed up some queries significantly.

    The main drawback is that it can produce incorrect results, if the connector returns
    partition keys for partitions that have no rows. In particular, the Hive connector
    can return empty partitions, if they were created by other systems. Presto cannot
    create them.

``optimizer.push-aggregation-through-join``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``boolean``
    * **Default value:** ``true``

    When an aggregation is above an outer join and all columns from the outer side of the join
    are in the grouping clause, the aggregation is pushed below the outer join. This optimization
    is particularly useful for correlated scalar subqueries, which get rewritten to an aggregation
    over an outer join. For example::

        SELECT * FROM item i
            WHERE i.i_current_price > (
                SELECT AVG(j.i_current_price) FROM item j
                    WHERE i.i_category = j.i_category);

    Enabling this optimization can substantially speed up queries by reducing
    the amount of data that needs to be processed by the join.  However, it may slow down some
    queries that have very selective joins. This can also be specified on a per-query basis using
    the ``push_aggregation_through_join`` session property.

``optimizer.push-table-write-through-union``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``boolean``
    * **Default value:** ``true``

    Parallelize writes when using ``UNION ALL`` in queries that write data. This improves the
    speed of writing output tables in ``UNION ALL`` queries, because these writes do not require
    additional synchronization when collecting results. Enabling this optimization can improve
    ``UNION ALL`` speed, when write speed is not yet saturated. However, it may slow down queries
    in an already heavily loaded system. This can also be specified on a per-query basis
    using the ``push_table_write_through_union`` session property.


``optimizer.join-reordering-strategy``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``string``
    * **Allowed values:** ``AUTOMATIC``, ``ELIMINATE_CROSS_JOINS``, ``NONE``
    * **Default value:** ``AUTOMATIC``

    The join reordering strategy to use.  ``NONE`` maintains the order the tables are listed in the
    query.  ``ELIMINATE_CROSS_JOINS`` reorders joins to eliminate cross joins, where possible, and
    otherwise maintains the original query order. When reordering joins, it also strives to maintain the
    original table order as much as possible. ``AUTOMATIC`` enumerates possible orders, and uses
    statistics-based cost estimation to determine the least cost order. If stats are not available, or if
    for any reason a cost could not be computed, the ``ELIMINATE_CROSS_JOINS`` strategy is used. This can
    be specified on a per-query basis using the ``join_reordering_strategy`` session property.

``optimizer.max-reordered-joins``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``integer``
    * **Default value:** ``9``

    When optimizer.join-reordering-strategy is set to cost-based, this property determines the maximum
    number of joins that can be reordered at once.

    .. warning:: The number of possible join orders scales factorially with the number of relations,
                 so increasing this value can cause serious performance issues.

Regular Expression Function Properties
--------------------------------------

The following properties allow tuning the :doc:`/functions/regexp`.

``regex-library``
^^^^^^^^^^^^^^^^^

    * **Type:** ``string``
    * **Allowed values:** ``JONI``, ``RE2J``
    * **Default value:** ``JONI``

    Which library to use for regular expression functions.
    ``JONI`` is generally faster for common usage, but can require exponential
    time for certain expression patterns. ``RE2J`` uses a different algorithm,
    which guarantees linear time, but is often slower.

``re2j.dfa-states-limit``
^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``integer``
    * **Minimum value:** ``2``
    * **Default value:** ``2147483647``

    The maximum number of states to use when RE2J builds the fast,
    but potentially memory intensive, deterministic finite automaton (DFA)
    for regular expression matching. If the limit is reached, RE2J falls
    back to the algorithm that uses the slower, but less memory intensive
    non-deterministic finite automaton (NFA). Decreasing this value decreases the
    maximum memory footprint of a regular expression search at the cost of speed.

``re2j.dfa-retries``
^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``integer``
    * **Minimum value:** ``0``
    * **Default value:** ``5``

    The number of times that RE2J retries the DFA algorithm, when
    it reaches a states limit before using the slower, but less memory
    intensive NFA algorithm, for all future inputs for that search. If hitting the
    limit for a given input row is likely to be an outlier, you want to be able
    to process subsequent rows using the faster DFA algorithm. If you are likely
    to hit the limit on matches for subsequent rows as well, you want to use the
    correct algorithm from the beginning so as not to waste time and resources.
    The more rows you are processing, the larger this value should be.


Logging Properties
------------------

``log.path``
^^^^^^^^^^^^

    * **Type:** ``string``
    * **Default value:** ``var/log/server.log``

    The path to the log file used by Presto. The path is relative to the data
    directory, configured by the launcher script as detailed in
    :ref:`running_presto`.

``log.max-history``
^^^^^^^^^^^^^^^^^^^

    * **Type:** ``integer``
    * **Default value:** ``30``

    The maximum number of general application log files to use, before log
    rotation replaces old content.

``log.max-size``
^^^^^^^^^^^^^^^^
    * **Type:** ``data size``
    * **Default value:** ``100MB``

    The maximum file size for the general application log file.

``http-server.log.enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``boolean``
    * **Default value:** ``true``

    Flag to enable or disable logging for the HTTP server.

``http-server.log.compression.enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``boolean``
    * **Default value:** ``true``

    Flag to enable or disable compression of the log files of the HTTP server.

``http-server.log.path``
^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``string``
    * **Default value:** ``var/log/http-request.log``

    The path to the log file used by the HTTP server. The path is relative to
    the data directory, configured by the launcher script as detailed in
    :ref:`running_presto`.

``http-server.log.max-history``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``integer``
    * **Default value:** ``15``

    The maxiumum number of log files for the HTTP server to use, before
    log rotation replaces old content.

``http-server.log.max-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``data size``
    * **Default value:** ``unlimited``

    The maximum file size for the log file of the HTTP server.

.. _web-ui-properties:

Web UI Properties
-----------------

The following properties can be used to configure the :doc:`./web-interface`.

``web-ui.enabled``
^^^^^^^^^^^^^^^^^^

    * **Type:** ``boolean``
    * **Default value:** ``true``

    This property controls whether or not the Web UI is available.

``web-ui.shared-secret``
^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``string``
    * **Default value:** randomly generated unless set

    The shared secret is used to generate authentication cookies for users of
    the Web UI. If not set to a static value, any coordinator restart generates
    a new random value, which in turn invalidates the session of any currently
    logged in Web UI user.

web-ui.session-timeout
^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``duration``
    * **Default value:** ``1 day``

    The duration how long a user can be logged into the Web UI, before the
    session times out, which forces an automatic log-out.
