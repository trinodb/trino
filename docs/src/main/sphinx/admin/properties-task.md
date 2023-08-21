# Task properties

## `task.concurrency`

- **Type:** {ref}`prop-type-integer`
- **Restrictions:** Must be a power of two
- **Default value:** The number of physical CPUs of the node, with a minimum value of 2 and a maximum of 32
- **Session property:** `task_concurrency`

Default local concurrency for parallel operators, such as joins and aggregations.
This value should be adjusted up or down based on the query concurrency and worker
resource utilization. Lower values are better for clusters that run many queries
concurrently, because the cluster is already utilized by all the running
queries, so adding more concurrency results in slow downs due to context
switching and other overhead. Higher values are better for clusters that only run
one or a few queries at a time.

## `task.http-response-threads`

- **Type:** {ref}`prop-type-integer`
- **Minimum value:** `1`
- **Default value:** `100`

Maximum number of threads that may be created to handle HTTP responses. Threads are
created on demand and are cleaned up when idle, thus there is no overhead to a large
value, if the number of requests to be handled is small. More threads may be helpful
on clusters with a high number of concurrent queries, or on clusters with hundreds
or thousands of workers.

## `task.http-timeout-threads`

- **Type:** {ref}`prop-type-integer`
- **Minimum value:** `1`
- **Default value:** `3`

Number of threads used to handle timeouts when generating HTTP responses. This value
should be increased if all the threads are frequently in use. This can be monitored
via the `trino.server:name=AsyncHttpExecutionMBean:TimeoutExecutor`
JMX object. If `ActiveCount` is always the same as `PoolSize`, increase the
number of threads.

## `task.info-update-interval`

- **Type:** {ref}`prop-type-duration`
- **Minimum value:** `1ms`
- **Maximum value:** `10s`
- **Default value:** `3s`

Controls staleness of task information, which is used in scheduling. Larger values
can reduce coordinator CPU load, but may result in suboptimal split scheduling.

## `task.max-drivers-per-task`

- **Type:** {ref}`prop-type-integer`
- **Minimum value:** `1`
- **Default Value:** `2147483647`

Controls the maximum number of drivers a task runs concurrently. Setting this value
reduces the likelihood that a task uses too many drivers and can improve concurrent query
performance. This can lead to resource waste if it runs too few concurrent queries.

## `task.max-partial-aggregation-memory`

- **Type:** {ref}`prop-type-data-size`
- **Default value:** `16MB`

Maximum size of partial aggregation results for distributed aggregations. Increasing this
value can result in less network transfer and lower CPU utilization, by allowing more
groups to be kept locally before being flushed, at the cost of additional memory usage.

## `task.max-worker-threads`

- **Type:** {ref}`prop-type-integer`
- **Default value:** (Node CPUs * 2)

Sets the number of threads used by workers to process splits. Increasing this number
can improve throughput, if worker CPU utilization is low and all the threads are in use,
but it causes increased heap space usage. Setting the value too high may cause a drop
in performance due to a context switching. The number of active threads is available
via the `RunningSplits` property of the
`trino.execution.executor:name=TaskExecutor.RunningSplits` JMX object.

## `task.min-drivers`

- **Type:** {ref}`prop-type-integer`
- **Default value:** (`task.max-worker-threads` * 2)

The target number of running leaf splits on a worker. This is a minimum value because
each leaf task is guaranteed at least `3` running splits. Non-leaf tasks are also
guaranteed to run in order to prevent deadlocks. A lower value may improve responsiveness
for new tasks, but can result in underutilized resources. A higher value can increase
resource utilization, but uses additional memory.

## `task.min-drivers-per-task`

- **Type:** {ref}`prop-type-integer`
- **Minimum value:** `1`
- **Default Value:** `3`

The minimum number of drivers guaranteed to run concurrently for a single task given
the task has remaining splits to process.

## `task.scale-writers.enabled`

- **Description:** see details at {ref}`prop-task-scale-writers`

## `task.scale-writers.max-writer-count`

- **Description:** see details at {ref}`prop-task-scale-writers-max-writer-count`

## `task.writer-count`

- **Type:** {ref}`prop-type-integer`
- **Default value:** `1`
- **Session property:** `task_writer_count`

The number of concurrent writer threads per worker per query when
{ref}`preferred partitioning <preferred-write-partitioning>` and
{ref}`task writer scaling <prop-task-scale-writers>` are not used. Increasing this value may
increase write speed, especially when a query is not I/O bound and can take advantage of
additional CPU for parallel writes.

Some connectors can be bottlenecked on the CPU when writing due to compression or other factors.
Setting this too high may cause the cluster to become overloaded due to excessive resource
utilization. Especially when the engine is inserting into a partitioned table without using
{ref}`preferred partitioning <preferred-write-partitioning>`. In such case, each writer thread
could write to all partitions. This can lead to out of memory error since writing to a partition
allocates a certain amount of memory for buffering.

## `task.partitioned-writer-count`

- **Type:** {ref}`prop-type-integer`
- **Restrictions:** Must be a power of two
- **Default value:** The number of physical CPUs of the node, with a minimum value of 2 and a maximum of 32
- **Session property:** `task_partitioned_writer_count`

The number of concurrent writer threads per worker per query when
{ref}`preferred partitioning <preferred-write-partitioning>` is used. Increasing this value may
increase write speed, especially when a query is not I/O bound and can take advantage of additional
CPU for parallel writes. Some connectors can be bottlenecked on CPU when writing due to compression
or other factors. Setting this too high may cause the cluster to become overloaded due to excessive
resource utilization.

## `task.interrupt-stuck-split-tasks-enabled`

- **Type:** {ref}`prop-type-boolean`
- **Default value:** `true`

Enables Trino detecting and failing tasks containing splits that have been stuck. Can be
specified by `task.interrupt-stuck-split-tasks-timeout` and
`task.interrupt-stuck-split-tasks-detection-interval`. Only applies to threads that
are blocked by the third-party Joni regular expression library.

## `task.interrupt-stuck-split-tasks-warning-threshold`

- **Type:** {ref}`prop-type-duration`
- **Minimum value:** `1m`
- **Default value:** `10m`

Print out call stacks at `/v1/maxActiveSplits` endpoint and generate JMX metrics
for splits running longer than the threshold.

## `task.interrupt-stuck-split-tasks-timeout`

- **Type:** {ref}`prop-type-duration`
- **Minimum value:** `3m`
- **Default value:** `10m`

The length of time Trino waits for a blocked split processing thread before failing the
task. Only applies to threads that are blocked by the third-party Joni regular
expression library.

## `task.interrupt-stuck-split-tasks-detection-interval`

- **Type:** {ref}`prop-type-duration`
- **Minimum value:** `1m`
- **Default value:** `2m`

The interval of Trino checks for splits that have processing time exceeding
`task.interrupt-stuck-split-tasks-timeout`. Only applies to threads that are blocked
by the third-party Joni regular expression library.
