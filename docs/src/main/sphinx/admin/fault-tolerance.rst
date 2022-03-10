========================
Fault tolerant execution
========================

(maybe: some disclaimer that fault-tolerant execution in Trino is a preview feature
 and details like config parameters are subject to change)

By default Trino executes queries in the streaming manner. This mode provides best performance
but comes at a cost. If one of the nodes of the cluster fails during query execution,
or there are to little resources on for one of the tasks,
whole query will fail and will need to manually restarted (possibly with different session configuration).

The longer query runtime is the bigger chance that query will fail due to "environmental" causes.
Therefore default execution mode works best for interactive queries, which complete in
matter of seconds or single-digit minutes. In such case cost of restaring is not huge.

You can choose failure recovery mode using ``retry-policy`` config property. Available options are:
 * NONE - default execution mode with no automatic retries
 * QUERY - automatic query based retries
 * TASK - automatic task based retries

Query based retries
-------------------

With query based retires query will be executed in the same manner as with default mode but query will be
automatically (internally) restarted in case of non-fatal errors happening on worker nodes.
Query will automatically be retried on all but USER errors (query will not be retried if for example query SQL cannot be parsed).

Limitations:

Query based retries impose limitation on the amount of data that can be sent from worker nodes to final stage execution on the coordinator.
This limitation is mostly visible for "SELECT ..." queries which produce large results, but can also trigger for DML queries which need to
pass metadata regarding created data to coordinator (e.g. if INSERT query using Hive connector creates lot's of files the query may also trigger limit).
If data passed from workers to coordinator crosses buffer size the query will fail. The buffer size can be configured via ``exchange.deduplication-buffer-size``
config property and is set to 32 MB by default.

The limitation can be by configuring exchange storage. Then when memory limit is reached buffered data will be spilled into external location.
For details on how to configure exchange storage refert to: TODO LINK TO THE SECTION

Task based retries
------------------

With task based retries enabled Trino uses different execution flow which checkpoints data passed between tasks on workers into
external exchange storage. Having checkpoints allow for restarting individual tasks in case of failure. Restarted task may use
different node and be assigned more resources which allows for better handling of queries executed on skewed data.

To use task based retries you have to configure exchange storage. For details on that refer to : TODO LINK TO THE SECTION

Limitations:

Task based retries are currently only compatible with ``total-reservation-on-blocked-nodes`` low memory killer.
If ``query.low-memory-killer.policy`` config property is set to value other than ``total-reservation-on-blocked-nodes``
queries execution may dead-lock if cluster runs out of memory, unless one of the queries is killed manually.

Fine tuning
-----------

Note:

It should not be necessary to fine tune the execution most of the time but we still provide some gauges to do so in case need arises.
As fuctionality matures we may no longer see need to all configuration properties and deprecate some of those (and eventually remove).

Retry limits
^^^^^^^^^^^^

You can configure thresholds at with query/tasks will no longer be required in case of repated failures.

The following configuration properties control the behavior of fault-tolerant
execution on a Trino cluster:

.. list-table:: Fault-tolerance retry limits configuration properties
  :widths: 30, 40, 30
  :header-rows: 1

  * - Property name
    - Description
    - Default value
  * - ``query-retry-attempts``
    - Number of times Trino attempts to retry the query before
      declaring the operation as a failure. Property is applicable for query based retries.
    - ``4``
  * - ``task-retry-attempts-overall``
    - Number of times Trino attempts to retry a task before
      declaring the operation as a failure. Retries are counted over all tasks executed as part of the query.
      Property is applicable for task based retries.
    - ``no limit``
  * - ``task-retry-attempts-per-task``
    - Number of times Trino attempts to retry a task before
      declaring the operation as a failure. Retries are counted separately for each task.
      Property is applicable for task based retries.
    - ``2``
  * - ``retry-initial-delay``
    - Minimum time that a failed query must wait before it is retried. Property is applicable for query based retries.
      Value may be overridden using ``retry_initial_delay`` session property.
    - ``10s``
  * - ``retry-max-delay``
    - Minimum time that a failed query must wait before it is retried.
      Wait time is increased on each subsequent query failure.
      Property is applicable for query based retries.
      Value may be overridden using ``retry_initial_delay`` session property.
    - ``1m``

Task sizing properties
^^^^^^^^^^^^^^^^^^^^^^

With task level retries it is important to for each task to process a "reasonable" amount of data.
If the tasks are to small lots of processing time is spent on tax of handling tasks themselves, not processing data. Also
end up having more distinct data partitions stored on the external exchange which may lead to scalability problems.
If tasks are to big then even a single task may require more resources than there are available on any single node on the cluster;
then query cannot be completed at all. Also in case of failure it is more costly to retry big task than small task.

At the moment we have limited support for self adaptation of task sizing; hence we provide gauges which allow for manual tweaking, should default
configuration not work well.

Note that task sizing is best effort. Depending on the query plan and data shape it may not always be possible to
split task into smaller ones or join two tasks into bigger one, but scheduler logic will work towards being as near to the requested values as possible.

.. list-table:: Task sizing configuration properties
  :widths: 30, 40, 30
  :header-rows: 1
  * - Property name
    - Description
    - Default value
  * - ``fault-tolerant-execution-target-task-input-size``
    - Target size in bytes of all task inputs for a single fault tolerant task. Property governs
      sizing for tasks which are reading input written to exchange by other tasks (where written data size is known).
      Depending on the plan constraints it may be possible to split input so it is processed by two or more tasks instead of one.
      It should be always possible to merge input so there is just a single task processing insteads of a few.
      Value may be overridden using ``fault_tolerant_execution_target_task_input_size`` session property.
    - ``1GB``
  * - ``fault-tolerant-execution-target-task-split-count``
    - Target number of splits processed by a single task which is reading data from source tables.
      The value is interpreted with split weight taken into account. The value unit is number of "standard" splits.
      If weight of splits produced by connector denotes that they are lighter or heavier than "standard" split then
      number of splits processed by single task is adjusted accordingly.
      Value may be overridden using ``fault_tolerant_execution_target_task_split_count`` session property.
    - ``16``
  * - ``fault-tolerant-execution-min-task-split-count``
    - Minimum number of splits processed by single task. This value is not split weight adjusted and serves as guard against in situation
      where split weight reported by connectors does not match reality.
      Value may be overridden using ``fault_tolerant_execution_min_task_split_count`` session property.
    - ``16``
  * - ``fault-tolerant-execution-max-task-split-count``
    - Maximum number of splits processed by single task. This value is not split weight adjusted and serves as guard against in situation
      where split weight reported by connectors does not match reality.
      Value may be overridden using ``fault_tolerant_execution_max_task_split_count`` session property.
    - ``256``

Node allocation
^^^^^^^^^^^^^^^

With task based retries nodes are allocated for tasks as query execution progresses.
Tasks are bin-packed to nodes based on estimated memory usage. In case or task failure
because it exceeded available memory task is restarted with request to allocate full node for its execution.
Initial task memory requirements estimation is currently static and configured via ``fault-tolerant-task-memory`` config property.

.. list-table:: Node allocation configuration properties
  :widths: 30, 40, 30
  :header-rows: 1
  * - Property name
    - Description
    - Default value
  * - ``fault-tolerant-execution-task-memory``
    - Initial task memory estimation used for bin-packing when allocating nodes for tasks.
      Value can be overridden using ``fault_tolerant_execution_task_memory``
    - ``4GB``


Other properties
^^^^^^^^^^^^^^^^

.. list-table:: Other configuration properties
  :widths: 30, 40, 30
  :header-rows: 1
  * - Property name
    - Description
    - Default value
  * - ``fault-tolerant-execution-task-descriptor-storage-max-memory``
    - Maximum amount of memory to be used to store task descriptors for fault tolerant queries on coordinator.
      Extra memory is needed to be able to reschedule tasks in case of a failure. This property is relevant for task-level retries
    - 15% of Java heap






