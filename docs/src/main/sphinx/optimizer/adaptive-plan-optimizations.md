# Adaptive plan optimizations

Trino offers several adaptive plan optimizations that adjust query
execution plans dynamically based on runtime statistics. These
optimizations are only available when
[](/admin/fault-tolerant-execution) is enabled.

To deactivate all adaptive plan optimizations, set the
`fault-tolerant-execution-adaptive-query-planning-enabled`
configuration property to `false`. The equivalent session property is
`fault_tolerant_execution_adaptive_query_planning_enabled`.

## Adaptive reordering of partitioned joins

By default, Trino enables adaptive reordering of partitioned joins. This
optimization allows Trino to dynamically reorder the join inputs, based
on the actual size of the build and probe sides during query execution.
This is particularly useful when table statistics are not available
beforehand, as it can improve query performance by making more efficient
join order decisions based on runtime information.

To deactivate this optimization, set the
`fault-tolerant-execution-adaptive-join-reordering-enabled`
configuration property to `false`. The equivalent session property is
`fault_tolerant_execution_adaptive_join_reordering_enabled`.
