# Query management properties

## `query.client.timeout`

- **Type:** {ref}`prop-type-duration`
- **Default value:** `5m`

Configures how long the cluster runs without contact from the client
application, such as the CLI, before it abandons and cancels its work.

## `query.execution-policy`

- **Type:** {ref}`prop-type-string`
- **Default value:** `phased`
- **Session property:** `execution_policy`

Configures the algorithm to organize the processing of all the
stages of a query. You can use the following execution policies:

- `phased` schedules stages in a sequence to avoid blockages because of
  inter-stage dependencies. This policy maximizes cluster resource utilization
  and provides the lowest query wall time.
- `all-at-once` schedules all the stages of a query at one time. As a
  result, cluster resource utilization is initially high, but inter-stage
  dependencies typically prevent full processing and cause longer queue times
  which increases the query wall time overall.

## `query-manager.required-workers`

- **Type:** {ref}`prop-type-integer`
- **Default value:** `1`
- **Session property:** `required_workers_count`

Sets the minimum number of active worker nodes that must be available before
the coordinator starts executing queries. Until this threshold is met, or
the maximum wait time configured by
`query-manager.required-workers-max-wait` expires, incoming queries enter a
`WAITING_FOR_RESOURCES` state.

This property is particularly useful for Kubernetes deployments with
autoscaling (for example, [KEDA](https://keda.sh)) where workers may scale to
zero during idle periods. Setting this value higher than `1` ensures queries
are not scheduled on an undersized cluster while workers are starting up.

:::{note}
Coordinator-only queries, such as queries against system tables and
`EXPLAIN` queries, are also gated by this requirement. See
[issue #4130](https://github.com/trinodb/trino/issues/4130) and
[issue #4131](https://github.com/trinodb/trino/issues/4131) for details
on this known limitation.
:::

## `query-manager.required-workers-max-wait`

- **Type:** {ref}`prop-type-duration`
- **Default value:** `5m`
- **Session property:** `required_workers_max_wait_time`

Sets the maximum time the coordinator waits for the number of active worker
nodes to reach the threshold defined by `query-manager.required-workers`.
If the timeout expires before enough workers are available, the query fails
with an ``Insufficient active worker nodes`` error.

For autoscaling environments, set this value high enough to cover the time
required for worker pods or instances to start and register with the
coordinator.

## `query.determine-partition-count-for-write-enabled`

- **Type:** {ref}`prop-type-boolean`
- **Default value:** `false`
- **Session property:** `determine_partition_count_for_write_enabled`

Enables determining the number of partitions based on amount of data read and processed by the
query for write queries.

## `query.max-hash-partition-count`

- **Type:** {ref}`prop-type-integer`
- **Default value:** `100`
- **Session property:** `max_hash_partition_count`

The maximum number of partitions to use for processing distributed operations, such as
joins, aggregations, partitioned window functions and others.

## `query.min-hash-partition-count`

- **Type:** {ref}`prop-type-integer`
- **Default value:** `4`
- **Session property:** `min_hash_partition_count`

The minimum number of partitions to use for processing distributed operations, such as
joins, aggregations, partitioned window functions and others.

## `query.min-hash-partition-count-for-write`

- **Type:** {ref}`prop-type-integer`
- **Default value:** `50`
- **Session property:** `min_hash_partition_count_for_write`

The minimum number of partitions to use for processing distributed operations in write queries,
such as joins, aggregations, partitioned window functions and others.

## `query.max-writer-task-count`

- **Type:** {ref}`prop-type-integer`
- **Default value:** `100`
- **Session property:** `max_writer_task_count`

The maximum number of tasks that will take part in writing data during
`INSERT`, `CREATE TABLE AS SELECT` and `EXECUTE` queries.
The limit is only applicable when `redistribute-writes` or `scale-writers` is enabled.

## `query.low-memory-killer.policy`

- **Type:** {ref}`prop-type-string`
- **Default value:** `total-reservation-on-blocked-nodes`

Configures the behavior to handle killing running queries in the event of low
memory availability. Supports the following values:

- `none` - Do not kill any queries in the event of low memory.
- `total-reservation` - Kill the query currently using the most total memory.
- `total-reservation-on-blocked-nodes` - Kill the query currently using the
  most memory specifically on nodes that are now out of memory.

:::{note}
Only applies for queries with task level retries disabled (`retry-policy` set to `NONE` or `QUERY`)
:::

## `task.low-memory-killer.policy`

- **Type:** {ref}`prop-type-string`
- **Default value:** `total-reservation-on-blocked-nodes`

Configures the behavior to handle killing running tasks in the event of low
memory availability. Supports the following values:

- `none` - Do not kill any tasks in the event of low memory.
- `total-reservation-on-blocked-nodes` - Kill the tasks that are part of the queries
  which have task retries enabled and are currently using the most memory specifically
  on nodes that are now out of memory.
- `least-waste` - Kill the tasks that are part of the queries
  which have task retries enabled and use significant amount of memory on nodes
  which are now out of memory. This policy avoids killing tasks which are already
  executing for a long time, so significant amount of work is not wasted.

:::{note}
Only applies for queries with task level retries enabled (`retry-policy=TASK`)
:::

## `query.max-execution-time`

- **Type:** {ref}`prop-type-duration`
- **Default value:** `100d`
- **Session property:** `query_max_execution_time`

The maximum allowed time for a query to be actively executing on the
cluster, before it is terminated. Compared to the run time below, execution
time does not include analysis, query planning or wait times in a queue.

## `query.max-length`

- **Type:** {ref}`prop-type-integer`
- **Default value:** `1,000,000`
- **Maximum value:** `1,000,000,000`

The maximum number of characters allowed for the SQL query text. Longer queries
are not processed, and terminated with error `QUERY_TEXT_TOO_LARGE`.

## `query.max-planning-time`

- **Type:** {ref}`prop-type-duration`
- **Default value:** `10m`
- **Session property:** `query_max_planning_time`

The maximum allowed time for a query to be actively planning the execution.
After this period the coordinator will make its best effort to stop the
query. Note that some operations in planning phase are not easily cancellable
and may not terminate immediately.

## `query.max-run-time`

- **Type:** {ref}`prop-type-duration`
- **Default value:** `100d`
- **Session property:** `query_max_run_time`

The maximum allowed time for a query to be processed on the cluster, before
it is terminated. The time includes time for analysis and planning, but also
time spent in a queue waiting, so essentially this is the time allowed for a
query to exist since creation.

## `query.max-scan-physical-bytes`

- **Type:** {ref}`prop-type-data-size`
- **Session property:** `query_max_scan_physical_bytes`

The maximum number of bytes that can be scanned by a query during its execution.
When this limit is reached, query processing is terminated to prevent excessive
resource usage.

## `query.max-write-physical-size`

- **Type:** {ref}`prop-type-data-size`
- **Session property:** `query_max_write_physical_size`

The maximum physical size of data that can be written by a query during its execution.
When this limit is reached, query processing is terminated to prevent excessive
resource usage.

## `query.max-stage-count`

- **Type:** {ref}`prop-type-integer`
- **Default value:** `150`
- **Minimum value:** `1`

The maximum number of stages allowed to be generated per query. If a query
generates more stages than this it will get killed with error
`QUERY_HAS_TOO_MANY_STAGES`.

:::{warning}
Setting this to a high value can cause queries with a large number of
stages to introduce instability in the cluster causing unrelated queries
to get killed with `REMOTE_TASK_ERROR` and the message
`Max requests queued per destination exceeded for HttpDestination ...`
:::

## `query.stage-count-warning-threshold`

- **Type:** {ref}`prop-type-integer`
- **Default value:** `50`
- **Minimum value:** `1`

The number of stages a query can generate before the coordinator emits a
`TOO_MANY_STAGES` warning. Unlike `query.max-stage-count`, exceeding this
threshold does not cause the query to fail. The warning includes a
suggestion to use `distinct_aggregations_strategy = 'single_step'` or
create temporary tables for re-used CTEs.

## `query.max-history`

- **Type:** {ref}`prop-type-integer`
- **Default value:** `100`

The maximum number of queries to keep in the query history to provide statistics
and other information, and make the data available in the
[](/admin/web-interface). If this amount is reached, queries are removed based
on age.

To store query events and therefore information about more queries in an
external system you must use [an event listener](admin-event-listeners).

## `query.min-expire-age`

- **Type:** {ref}`prop-type-duration`
- **Default value:** `15m`

The minimal age of a query in the history before it is expired. An expired query
is removed from the query history buffer and no longer available in the
[](/admin/web-interface).

To store query events and therefore information about more queries in an
external system you must use [an event listener](admin-event-listeners).

## `query.schedule-split-batch-size`

- **Type:** {ref}`prop-type-integer`
- **Default value:** `1000`
- **Minimum value:** `1`

The maximum number of splits fetched from a connector in a single
`getNextBatch()` call during query scheduling. This applies to both
pipelined and fault-tolerant execution modes.

## `query.min-schedule-split-batch-size`

- **Type:** {ref}`prop-type-integer`
- **Default value:** `100`
- **Minimum value:** `1`

The minimum number of splits that must be buffered from a connector before
the scheduler processes them. When set to a value greater than `1`, split
sources are wrapped in a buffering layer that accumulates splits
asynchronously until this threshold is reached or the connector has no more
splits to provide. Setting this to `1` disables the buffering layer.

## `query.remote-task.enable-adaptive-request-size`

- **Type:** {ref}`prop-type-boolean`
- **Default value:** `true`
- **Session property:** `remote_task_adaptive_update_request_size_enabled`

Enables dynamically splitting up server requests sent by tasks, which can
prevent out-of-memory errors for large schemas. The default settings are
optimized for typical usage and should only be modified by advanced users
working with extremely large tables.

## `query.remote-task.guaranteed-splits-per-task`

- **Type:** {ref}`prop-type-integer`
- **Default value:** `3`
- **Session property:** `remote_task_guaranteed_splits_per_request`

The minimum number of splits that should be assigned to each remote task to
ensure that each task has a minimum amount of work to perform. Requires
`query.remote-task.enable-adaptive-request-size` to be enabled.

## `query.remote-task.max-error-duration`

- **Type:** {ref}`prop-type-duration`
- **Default value:** `1m`

Timeout value for remote tasks that fail to communicate with the coordinator. If
the coordinator is unable to receive updates from a remote task before this
value is reached, the coordinator treats the task as failed.

## `query.remote-task.max-request-size`

- **Type:** {ref}`prop-type-data-size`
- **Default value:** `8MB`
- **Session property:** `remote_task_max_request_size`

The maximum size of a single request made by a remote task. Requires
`query.remote-task.enable-adaptive-request-size` to be enabled.

## `query.remote-task.request-size-headroom`

- **Type:** {ref}`prop-type-data-size`
- **Default value:** `2MB`
- **Session property:** `remote_task_request_size_headroom`

Determines the amount of headroom that should be allocated beyond the size of
the request data. Requires `query.remote-task.enable-adaptive-request-size` to
be enabled.

## `query.remote-task.max-callback-threads`

- **Type:** {ref}`prop-type-integer`
- **Default value:** `1000`
- **Minimum value:** `1`

The maximum number of threads used concurrently to handle responses from
worker nodes. This thread pool processes task status updates, task info
fetches, dynamic filter updates, and split assignment confirmations across
all tasks for all queries. Reducing this value on clusters running queries
with many concurrent tasks may delay detection of task completion and slow
the delivery of new splits to workers.

## `query.max-split-manager-callback-threads`

- **Type:** {ref}`prop-type-integer`
- **Default value:** `100`
- **Minimum value:** `1`

The maximum number of threads used concurrently to process split generation
callbacks from connectors. All connectors share this thread pool. Reducing
this value may slow query startup on coordinators that schedule many stages
concurrently, particularly when split generation involves high-latency
operations such as listing remote partitions.

## `query.info-url-template`

- **Type:** {ref}`prop-type-string`
- **Default value:** `(URL of the query info page on the coordinator)`

Configure redirection of clients to an alternative location for query
information. The URL must contain a query id placeholder `${QUERY_ID}`.

For example `https://example.com/query/${QUERY_ID}`.

The `${QUERY_ID}` gets replaced with the actual query's id.

## `query.manager-executor-pool-size`

- **Type:** {ref}`prop-type-integer`
- **Default value:** `5`
- **Minimum value:** `1`

The size of the thread pool used for periodic coordinator housekeeping
tasks, including enforcing memory, CPU, scan, and write limits on running
queries, expiring completed queries from the history buffer, and failing
queries that have lost contact with their client. A separate pool of the
same size is also created for query dispatch operations such as
minimum-worker-count wait callbacks.

## `query.executor-pool-size`

- **Type:** {ref}`prop-type-integer`
- **Default value:** `1000`
- **Minimum value:** `1`

The size of the thread pool used for query scheduling work on the
coordinator. This includes stage scheduling, split assignment, and
state machine callbacks for both pipelined and fault-tolerant execution
modes. Each fault-tolerant query occupies one thread in this pool for
the duration of its scheduling lifecycle. Reducing this value significantly
on coordinators with high query concurrency may cause scheduling delays.

## `query.max-state-machine-callback-threads`

- **Type:** {ref}`prop-type-integer`
- **Default value:** `5`
- **Minimum value:** `1`

The maximum number of threads allowed to run query and stage state machine
listener callbacks concurrently for each query. This limit is applied
per query, not globally. State machine callbacks include query completion
event delivery and propagation of terminal states to child stages and tasks.

## `query.dispatcher-query-pool-size`

- **Type:** {ref}`prop-type-integer`
- **Default value:** `(max(50, available_processors * 10))`
- **Minimum value:** `1`

The maximum number of queries that can be simultaneously in the dispatch
phase on the coordinator. The dispatch phase includes SQL parsing, session
creation, access control checks, and resource group selection. Resource
group admission callbacks also run in this pool.

## `query.reported-rule-stats-limit`

- **Type:** {ref}`prop-type-integer`
- **Default value:** `10`
- **Minimum value:** `1`

The maximum number of optimizer rule statistics entries included in each
query completion event. Rules are ranked by total CPU time consumed during
planning, and only the top entries up to this limit are reported. The
resulting statistics are available through the query info API and delivered
to {doc}`event listeners </develop/event-listener>`.

## `max-tasks-waiting-for-execution-per-query`

- **Type:** {ref}`prop-type-integer`
- **Default value:** `10`
- **Minimum value:** `1`
- **Session property:** `max_tasks_waiting_for_execution_per_query`

The maximum number of tasks that can be waiting in the scheduling queue
before the coordinator pauses split enumeration for the query. When the
number of pending tasks reaches this threshold, no new task descriptors are
loaded for non-eager stages until the backlog drains. This prevents a single
query from building up an excessive number of pending tasks.

:::{note}
Only applies when {doc}`fault-tolerant execution </admin/fault-tolerant-execution>`
is enabled (`retry-policy=TASK`).
:::

## `source-pages-validation-enabled`

- **Type:** {ref}`prop-type-boolean`
- **Default value:** `true`
- **Session property:** `output_pages_validation_enabled`

Enables runtime validation of data pages produced by source operators,
including table scans and exchanges. When enabled, each page is checked
to verify that the number of channels and the block types match the
expected output types. Validation failures result in a
`GENERIC_INTERNAL_ERROR`. This check applies to all queries regardless
of the retry policy.

## `retry-policy`

- **Type:** {ref}`prop-type-string`
- **Default value:** `NONE`

The {ref}`retry policy <fte-retry-policy>` to use for
{doc}`/admin/fault-tolerant-execution`. Supports the following values:

- `NONE` - Disable fault-tolerant execution.
- `TASK` - Retry individual tasks within a query in the event of failure.
  Requires configuration of an {ref}`exchange manager <fte-exchange-manager>`.
- `QUERY` - Retry the whole query in the event of failure.
