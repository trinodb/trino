========================
Fault-tolerant execution
========================

By default, if a Trino node lacks the resources to execute a task or
otherwise fails during query execution, the query fails and must be run again
manually. The longer the runtime of a query, the more likely it is to be
susceptible to such failures.

Fault-tolerant execution is a mechanism in Trino that enables a cluster to
mitigate query failures by retrying queries or their component tasks in
the event of failure. With fault-tolerant execution enabled, intermediate
exchange data is spooled and can be re-used by another worker in the event of a
worker outage or other fault during query execution.

.. note::

    Fault tolerance does not apply to broken queries or other user error. For
    example, Trino does not spend resources retrying a query that fails because
    its SQL cannot be parsed.

Configuration
-------------

Fault-tolerant execution is disabled by default. To enable the feature, set the
``retry-policy`` configuration property to either ``QUERY`` or ``TASK``
depending on the desired :ref:`retry policy <fte-retry-policy>`.

.. code-block:: properties

    retry-policy=QUERY

The following configuration properties control the behavior of fault-tolerant
execution on a Trino cluster:

.. list-table:: Fault-tolerant execution configuration properties
   :widths: 30, 40, 30
   :header-rows: 1

   * - Property name
     - Description
     - Default value
   * - ``retry-policy``
     - Configures what is retried in the event of failure, either
       ``QUERY`` to retry the whole query, or ``TASK`` to retry tasks
       individually if they fail. See :ref:`retry policy <fte-retry-policy>` for
       more information.
     - ``NONE``
   * - ``exchange.deduplication-buffer-size``
     - Size of the coordinator's in-memory buffer used by fault-tolerant
       execution to store output of query :ref:`stages <trino-concept-stage>`.
       If this buffer is filled during query execution, the query fails unless
       an :ref:`exchange manager <fte-exchange-manager>` is configured.
     - ``32MB``

.. _fte-retry-policy:

Retry policy
------------

The ``retry-policy`` configuration property designates whether Trino retries
entire queries or a query's individual tasks in the event of failure.

QUERY
^^^^^

A ``QUERY`` retry policy instructs Trino to automatically retry a query in the
event of an error occuring on a worker node. A ``QUERY`` retry policy is
recommended when the majority of the Trino cluster's workload consists of many
small queries, or if an :ref:`exchange manager <fte-exchange-manager>` is not
configured.

By default Trino does not implement fault tolerance for queries whose result set
exceeds 32MB in size, such as :doc:`/sql/select` statements that return a very
large data set to the user. This limit can be increased by modifying the
``exchange.deduplication-buffer-size`` configuration property to be greater than
the default value of ``32MB``, but this results in higher memory usage on the
coordinator.

To enable fault-tolerant execution on queries with a larger result set, it is
strongly recommended to configure an :ref:`exchange manager
<fte-exchange-manager>` that utilizes external storage for spooled data and
therefore allows for storage of spilled data beyond the in-memory buffer size.

TASK
^^^^

A ``TASK`` retry policy instructs Trino to retry individual query
:ref:`tasks <trino-concept-task>` in the event of failure. This policy is
recommended when executing large batch queries, as the cluster can more
efficiently retry smaller tasks within the query rather than retry the whole
query.

``TASK`` retry policy requires a configured :ref:`exchange manager
<fte-exchange-manager>` to store spooled exchange data used for each task. It is
also strongly recommended to set the ``query.low-memory-killer.policy``
configuration property to ``total-reservation-on-blocked-nodes``, or queries may
need to be manually killed if the cluster runs out of memory.

Advanced configuration
----------------------

You can further configure fault-tolerant execution with the following
configuration properties. The default values for these properties should work
for most deployments, but you can change these values for testing or
troubleshooting purposes.

Retry limits
^^^^^^^^^^^^

The following configuration properties control the thresholds at which
queries/tasks are no longer retried in the event of repeated failures:

.. list-table:: Fault tolerance retry limit configuration properties
   :widths: 30, 40, 30, 30
   :header-rows: 1

   * - Property name
     - Description
     - Default value
     - Retry policy
   * - ``query-retry-attempts``
     - Maximum number of times Trino may attempt to retry a query before
       declaring the query as failed.
     - ``4``
     - Only ``QUERY``
   * - ``task-retry-attempts-overall``
     - Maximum number retries across all tasks within a given query
       before declaring the query as failed.
     - ``null`` (no limit)
     - Only ``TASK``
   * - ``task-retry-attempts-per-task``
     - Maximum number of times Trino may attempt to retry a single task before
       declaring the query as failed.
     - ``4``
     - Only ``TASK``
   * - ``retry-initial-delay``
     - Minimum time that a failed query or task must wait before it is retried. May be
       overridden with the ``retry_initial_delay`` :ref:`session property
       <session-properties-definition>`.
     - ``10s``
     - ``QUERY`` and ``TASK``
   * - ``retry-max-delay``
     - Maximum time that a failed query or task must wait before it is retried.
       Wait time is increased on each subsequent  failure. May be
       overridden with the ``retry_max_delay`` :ref:`session property
       <session-properties-definition>`.
     - ``1m``
     - ``QUERY`` and ``TASK``
   * - ``retry-delay-scale-factor``
     - Factor by which retry delay is increased on each query or task failure. May be
       overridden with the ``retry_delay_scale_factor`` :ref:`session property
       <session-properties-definition>`.
     - ``2.0``
     - ``QUERY`` and ``TASK``

Task sizing
^^^^^^^^^^^

With a ``TASK`` retry policy, it is important to manage the amount of data
processed in each task. If tasks are too small, the management of task
coordination can take more processing time and resources than executing the task
itself. If tasks are too large, then a single task may require more resources
than are available on any one node and therefore prevent the query from
completing.

Trino supports limited automatic task sizing. If issues are occurring
during fault-tolerant task execution, you can configure the following
configuration properties to manually control task sizing. These configuration
properties only apply to a ``TASK`` retry policy.

.. list-table:: Task sizing configuration properties
   :widths: 30, 40, 30
   :header-rows: 1

   * - Property name
     - Description
     - Default value
   * - ``fault-tolerant-execution-target-task-input-size``
     - Target size in bytes of all task inputs for a single fault-tolerant task.
       Applies to tasks that read input from spooled data written by other
       tasks.

       May be overridden for the current session with the
       ``fault_tolerant_execution_target_task_input_size``
       :ref:`session property <session-properties-definition>`.
     - ``1GB``
   * - ``fault-tolerant-execution-target-task-split-count``
     - Target number of standard :ref:`splits <trino-concept-splits>` processed
       by a single task that reads data from source tables. Value is interpreted
       with split weight taken into account. If the weight of splits produced by
       a catalog denotes that they are lighter or heavier than "standard" split,
       then the number of splits processed by single task is adjusted
       accordingly.

       May be overridden for the current session with the
       ``fault_tolerant_execution_target_task_split_count``
       :ref:`session property <session-properties-definition>`.
     - ``16``
   * - ``fault-tolerant-execution-min-task-split-count``
     - Minimum number of :ref:`splits <trino-concept-splits>` processed by
       a single task. This value is not split weight-adjusted and serves as
       protection against situations where catalogs report an incorrect split
       weight.

       May be overridden for the current session with the
       ``fault_tolerant_execution_min_task_split_count``
       :ref:`session property <session-properties-definition>`.
     - ``16``
   * - ``fault-tolerant-execution-max-task-split-count``
     - Maximum number of :ref:`splits <trino-concept-splits>` processed by a
       single task. This value is not split weight-adjusted and serves as
       protection against situations where catalogs report an incorrect split
       weight.

       May be overridden for the current session with the
       ``fault_tolerant_execution_max_task_split_count``
       :ref:`session property <session-properties-definition>`.
     - ``256``

Node allocation
^^^^^^^^^^^^^^^

With a ``TASK`` retry policy, nodes are allocated to tasks based on available
memory and estimated memory usage. If task failure occurs due to exceeding
available memory on a node, the task is restarted with a request to allocate the
full node for its execution.

The initial task memory-requirements estimation is static and configured with
the ``fault-tolerant-task-memory`` configuration property. This property only
applies to a ``TASK`` retry policy.

.. list-table:: Node allocation configuration properties
   :widths: 30, 40, 30
   :header-rows: 1

   * - Property name
     - Description
     - Default value
   * - ``fault-tolerant-execution-task-memory``
     - Initial task memory estimation used for bin-packing when allocating nodes
       for tasks. May be overridden for the current session with the
       ``fault_tolerant_execution_task_memory``
       :ref:`session property <session-properties-definition>`.
     - ``4GB``

Other tuning
^^^^^^^^^^^^

The following additional configuration property can be used to manage
fault-tolerant execution:

.. list-table:: Other fault-tolerant execution configuration properties
   :widths: 30, 40, 30, 30
   :header-rows: 1

   * - Property name
     - Description
     - Default value
     - Retry policy
   * - ``fault-tolerant-execution-task-descriptor-storage-max-memory``
     - Maximum amount of memory to be used to store task descriptors for fault
       tolerant queries on coordinator. Extra memory is needed to be able to
       reschedule tasks in case of a failure.
     - (JVM heap size * 0.15)
     - Only ``TASK``
   * - ``max-tasks-waiting-for-node-per-stage``
     - Allow for up to configured number of tasks to wait for node allocation
       per stage, before pausing scheduling for other tasks from this stage.
     - 5
     - Only ``TASK``

.. _fte-exchange-manager:

Exchange manager
----------------

Exchange spooling is responsible for storing and managing spooled data for
fault-tolerant execution. You can configure a filesystem-based exchange manager
that stores spooled data in a specified location, either an S3-compatible
storage system or a local filesystem.

To configure an exchange manager, create a new
``etc/exchange-manager.properties`` configuration file on the coordinator and
all worker nodes. In this file, set the ``exchange-manager.name`` configuration
propertry to ``filesystem``, and additional configuration properties as needed
for your storage solution.

.. list-table:: Exchange manager configuration properties
   :widths: 30, 40, 30
   :header-rows: 1

   * - Property name
     - Description
     - Default value
   * - ``exchange.base-directory``
     - The base directory URI location that the exchange manager uses to store
       spooling data. Only supports S3 and local filesystems.
     -
   * - ``exchange.encryption-enabled``
     - Enable encrypting of spooling data.
     - ``true``
   * - ``exchange.sink-buffer-pool-min-size``
     - The minimum buffer pool size for an exchange sink. The larger the buffer
       pool size, the larger the write parallelism and memory usage.
     - ``10``
   * - ``exchange.sink-buffers-per-partition``
     - The number of buffers per partition in the buffer pool. The larger the
       buffer pool size, the larger the write parallelism and memory usage.
     - ``2``
   * - ``exchange.sink-max-file-size``
     - Max size of files written by exchange sinks.
     - ``1GB``
   * - ``exchange.source-concurrent-reader``
     - The number of concurrent readers to read from spooling storage. The
       larger the number of concurrent readers, the larger the read parallelism
       and memory usage.
     - ``4``
   * - ``exchange.s3.aws-access-key``
     - AWS access key to use. Required for a connection to AWS S3, can be
       ignored for other S3 storage systems.
     -
   * - ``exchange.s3.aws-secret-key``
     - AWS secret key to use. Required for a connection to AWS S3, can be
       ignored for other S3 storage systems.
     -
   * - ``exchange.s3.region``
     - Region of the S3 bucket.
     -
   * - ``exchange.s3.endpoint``
     - S3 storage endpoint server if using an S3-compatible storage system that
       is not AWS. If using AWS S3, can be ignored.
     -
   * - ``exchange.s3.max-error-retries``
     - Maximum number of times the exchange manager's S3 client should retry
       a request.
     - ``3``
   * - ``exchange.s3.upload.part-size``
     - Part size for S3 multi-part upload.
     - ``5MB``

The following example ``exchange-manager.properties`` configuration specifies an
AWS S3 bucket as the spooling storage destination. Note that the destination
does not have to be in AWS, but can be any S3-compatible storage system.

.. code-block:: properties

    exchange-manager.name=filesystem
    exchange.base-directory=s3n://trino-exchange-manager
    exchange.encryption-enabled=true
    exchange.s3.region=us-west-1
    exchange.s3.aws-access-key=example-access-key
    exchange.s3.aws-secret-key=example-secret-key

The following example ``exchange-manager.properties`` configuration specifies a
local directory, ``/tmp/trino-exchange-manager``, as the spooling storage
destination.

.. note::

    It is only recommended to use a local filesystem for exchange in standalone,
    non-production clusters. A local directory can only be used for exchange in
    a distributed cluster if the exchange directory is shared and accessible
    from all worker nodes.

.. code-block:: properties

    exchange-manager.name=filesystem
    exchange.base-directory=/tmp/trino-exchange-manager
