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

Configures the algorithm to organize the processing of all of the
stages of a query. You can use the following execution policies:

- `phased` schedules stages in a sequence to avoid blockages because of
  inter-stage dependencies. This policy maximizes cluster resource utilization
  and provides the lowest query wall time.
- `all-at-once` schedules all of the stages of a query at one time. As a
  result, cluster resource utilization is initially high, but inter-stage
  dependencies typically prevent full processing and cause longer queue times
  which increases the query wall time overall.

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
- **Session property:** `min_hash_partition_count_for_writre`

The minimum number of partitions to use for processing distributed operations in write queries,
such as joins, aggregations, partitioned window functions and others.

## `query.max-writer-tasks-count`

- **Type:** {ref}`prop-type-integer`
- **Default value:** `100`
- **Session property:** `max_writer_tasks_count`

The maximum number of tasks that will take part in writing data during
`INSERT`, `CREATE TABLE AS SELECT` and `EXECUTE` queries.
The limit is only applicable when `redistribute-writes` or `scale-writers` is be enabled.

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
- `total-reservation-on-blocked-nodes` - Kill the tasks which are part of the queries
  which has task retries enabled and are currently using the most memory specifically
  on nodes that are now out of memory.
- `least-waste` - Kill the tasks which are part of the queries
  which has task retries enabled and use significant amount of memory on nodes
  which are now out of memory. This policy avoids killing tasks which are already
  executing for a long time, so significant amount of work is not wasted.

:::{note}
Only applies for queries with task level retries enabled (`retry-policy=TASK`)
:::

## `query.low-memory-killer.delay`

- **Type:** {ref}`prop-type-duration`
- **Default value:** `5m`

The amount of time a query is allowed to recover between running out of memory
and being killed, if `query.low-memory-killer.policy` or
`task.low-memory-killer.policy` is set to value differnt than `none`.

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
time spend in a queue waiting, so essentially this is the time allowed for a
query to exist since creation.

## `query.max-scan-physical-bytes`

- **Type:** {ref}`prop-type-data-size`
- **Session property:** `query_max_scan_physical_bytes`

The maximum number of bytes that can be scanned by a query during its execution.
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
Setting this to a high value can cause queries with large number of
stages to introduce instability in the cluster causing unrelated queries
to get killed with `REMOTE_TASK_ERROR` and the message
`Max requests queued per destination exceeded for HttpDestination ...`
:::

## `query.max-history`

- **Type:** {ref}`prop-type-integer`
- **Default value:** `100`

The maximum number of queries to keep in the query history to provide
statistics and other information. If this amount is reached, queries are
removed based on age.

## `query.min-expire-age`

- **Type:** {ref}`prop-type-duration`
- **Default value:** `15m`

The minimal age of a query in the history before it is expired. An expired
query is removed from the query history buffer and no longer available in
the {doc}`/admin/web-interface`.

## `query.remote-task.enable-adaptive-request-size`

- **Type:** {ref}`prop-type-boolean`
- **Default value:** `true`
- **Session property:** `query_remote_task_enable_adaptive_request_size`

Enables dynamically splitting up server requests sent by tasks, which can
prevent out-of-memory errors for large schemas. The default settings are
optimized for typical usage and should only be modified by advanced users
working with extremely large tables.

## `query.remote-task.guaranteed-splits-per-task`

- **Type:** {ref}`prop-type-integer`
- **Default value:** `3`
- **Session property:** `query_remote_task_guaranteed_splits_per_task`

The minimum number of splits that should be assigned to each remote task to
ensure that each task has a minimum amount of work to perform. Requires
`query.remote-task.enable-adaptive-request-size` to be enabled.

## `query.remote-task.max-error-duration`

- **Type:** {ref}`prop-type-duration`
- **Default value:** `5m`

Timeout value for remote tasks that fail to communicate with the coordinator. If
the coordinator is unable to receive updates from a remote task before this
value is reached, the coordinator treats the task as failed.

## `query.remote-task.max-request-size`

- **Type:** {ref}`prop-type-data-size`
- **Default value:** `8MB`
- **Session property:** `query_remote_task_max_request_size`

The maximum size of a single request made by a remote task. Requires
`query.remote-task.enable-adaptive-request-size` to be enabled.

## `query.remote-task.request-size-headroom`

- **Type:** {ref}`prop-type-data-size`
- **Default value:** `2MB`
- **Session property:** `query_remote_task_request_size_headroom`

Determines the amount of headroom that should be allocated beyond the size of
the request data. Requires `query.remote-task.enable-adaptive-request-size` to
be enabled.

## `retry-policy`

- **Type:** {ref}`prop-type-string`
- **Default value:** `NONE`

The {ref}`retry policy <fte-retry-policy>` to use for
{doc}`/admin/fault-tolerant-execution`. Supports the following values:

- `NONE` - Disable fault-tolerant execution.
- `TASK` - Retry individual tasks within a query in the event of failure.
  Requires configuration of an {ref}`exchange manager <fte-exchange-manager>`.
- `QUERY` - Retry the whole query in the event of failure.
