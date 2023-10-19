# Writer scaling properties

Writer scaling allows Trino to dynamically scale out the number of writer tasks
rather than allocating a fixed number of tasks. Additional tasks are added when
the average amount of physical data per writer is above a minimum threshold, but
only if the query is bottlenecked on writing.

Writer scaling is useful with connectors like Hive that produce one or more
files per writer -- reducing the number of writers results in a larger average
file size. However, writer scaling can have a small impact on query wall time
due to the decreased writer parallelism while the writer count ramps up to match
the needs of the query.

## `scale-writers`

- **Type:** {ref}`prop-type-boolean`
- **Default value:** `true`
- **Session property:** `scale_writers`

Enable writer scaling by dynamically increasing the number of writer tasks on
the cluster.

(prop-task-scale-writers)=

## `task.scale-writers.enabled`

- **Type:** {ref}`prop-type-boolean`
- **Default value:** `true`
- **Session property:** `task_scale_writers_enabled`

Enable scaling the number of concurrent writers within a task. The maximum
writer count per task for scaling is [](prop-task-max-writer-count). Additional
writers are added only when the average amount of uncompressed data processed
per writer is above the minimum threshold of `writer-scaling-min-data-processed`
and query is bottlenecked on writing.

(writer-scaling-min-data-processed)=
## `writer-scaling-min-data-processed`

- **Type:** {ref}`prop-type-data-size`
- **Default value:** `100MB`
- **Session property:** `writer_scaling_min_data_processed`

The minimum amount of uncompressed data that must be processed by a writer
before another writer can be added.
