# Optimizer properties

## `optimizer.dictionary-aggregation`

- **Type:** {ref}`prop-type-boolean`
- **Default value:** `false`
- **Session property:** `dictionary_aggregation`

Enables optimization for aggregations on dictionaries.

## `optimizer.optimize-hash-generation`

- **Type:** {ref}`prop-type-boolean`
- **Default value:** `false`
- **Session property:** `optimize_hash_generation`

Compute hash codes for distribution, joins, and aggregations early during execution,
allowing result to be shared between operations later in the query. This can reduce
CPU usage by avoiding computing the same hash multiple times, but at the cost of
additional network transfer for the hashes. In most cases it decreases overall
query processing time.

It is often helpful to disable this property, when using {doc}`/sql/explain` in order
to make the query plan easier to read.

## `optimizer.optimize-metadata-queries`

- **Type:** {ref}`prop-type-boolean`
- **Default value:** `false`
- **Session property:** `optimize_metadata_queries`

Enable optimization of some aggregations by using values that are stored as metadata.
This allows Trino to execute some simple queries in constant time. Currently, this
optimization applies to `max`, `min` and `approx_distinct` of partition
keys and other aggregation insensitive to the cardinality of the input,including
`DISTINCT` aggregates. Using this may speed up some queries significantly.

The main drawback is that it can produce incorrect results, if the connector returns
partition keys for partitions that have no rows. In particular, the Hive connector
can return empty partitions, if they were created by other systems. Trino cannot
create them.

## `optimizer.distinct-aggregations-strategy`

- **Type:** {ref}`prop-type-string`
- **Allowed values:** `AUTOMATIC`, `MARK_DISTINCT`, `SINGLE_STEP`, `PRE_AGGREGATE`, `SPLIT_TO_SUBQUERIES`
- **Default value:** `AUTOMATIC`
- **Session property:** `distinct_aggregations_strategy`

The strategy to use for multiple distinct aggregations.
- `SINGLE_STEP` Computes distinct aggregations in single-step without any pre-aggregations.
This strategy will perform poorly if the number of distinct grouping keys is small.
- `MARK_DISTINCT` uses `MarkDistinct` for multiple distinct aggregations or for mix of distinct and non-distinct aggregations.
- `PRE_AGGREGATE` Computes distinct aggregations using a combination of aggregation and pre-aggregation steps.
- `SPLIT_TO_SUBQUERIES` Splits the aggregation input to independent sub-queries, where each subquery computes single distinct aggregation thus improving parallelism
- `AUTOMATIC` chooses the strategy automatically.

Single-step strategy is preferred. However, for cases with limited concurrency due to
a small number of distinct grouping keys, it will choose an alternative strategy
based on input data statistics.

## `optimizer.push-aggregation-through-outer-join`

- **Type:** {ref}`prop-type-boolean`
- **Default value:** `true`
- **Session property:** `push_aggregation_through_outer_join`

When an aggregation is above an outer join and all columns from the outer side of the join
are in the grouping clause, the aggregation is pushed below the outer join. This optimization
is particularly useful for correlated scalar subqueries, which get rewritten to an aggregation
over an outer join. For example:

```
SELECT * FROM item i
    WHERE i.i_current_price > (
        SELECT AVG(j.i_current_price) FROM item j
            WHERE i.i_category = j.i_category);
```

Enabling this optimization can substantially speed up queries by reducing the
amount of data that needs to be processed by the join. However, it may slow down
some queries that have very selective joins.

## `optimizer.push-table-write-through-union`

- **Type:** {ref}`prop-type-boolean`
- **Default value:** `true`
- **Session property:** `push_table_write_through_union`

Parallelize writes when using `UNION ALL` in queries that write data. This improves the
speed of writing output tables in `UNION ALL` queries, because these writes do not require
additional synchronization when collecting results. Enabling this optimization can improve
`UNION ALL` speed, when write speed is not yet saturated. However, it may slow down queries
in an already heavily loaded system.

## `optimizer.push-filter-into-values-max-row-count`

- **Type:** {ref}`prop-type-integer`
- **Default value:** `100`
- **Minimum value:** `0`
- **Session property:** `push_filter_into_values_max_row_count`
 
The number of rows in [](/sql/values) below which the planner evaluates a filter
on top of `VALUES` to optimize the query plan.

## `optimizer.join-reordering-strategy`

- **Type:** {ref}`prop-type-string`
- **Allowed values:** `AUTOMATIC`, `ELIMINATE_CROSS_JOINS`, `NONE`
- **Default value:** `AUTOMATIC`
- **Session property:** `join_reordering_strategy`

The join reordering strategy to use.  `NONE` maintains the order the tables are listed in the
query.  `ELIMINATE_CROSS_JOINS` reorders joins to eliminate cross joins, where possible, and
otherwise maintains the original query order. When reordering joins, it also strives to maintain the
original table order as much as possible. `AUTOMATIC` enumerates possible orders, and uses
statistics-based cost estimation to determine the least cost order. If stats are not available, or if
for any reason a cost could not be computed, the `ELIMINATE_CROSS_JOINS` strategy is used.

## `optimizer.max-reordered-joins`

- **Type:** {ref}`prop-type-integer`
- **Default value:** `8`
- **Session property:** `max_reordered_joins`

When optimizer.join-reordering-strategy is set to cost-based, this property determines
the maximum number of joins that can be reordered at once.

:::{warning}
The number of possible join orders scales factorially with the number of
relations, so increasing this value can cause serious performance issues.
:::

## `optimizer.optimize-duplicate-insensitive-joins`

- **Type:** {ref}`prop-type-boolean`
- **Default value:** `true`
- **Session property:** `optimize_duplicate_insensitive_joins`

Reduces number of rows produced by joins when optimizer detects that duplicated
join output rows can be skipped.

## `optimizer.use-exact-partitioning`

- **Type:** {ref}`prop-type-boolean`
- **Default value:** `false`
- **Session property:** `use_exact_partitioning` 

Re-partition data unless the partitioning of the upstream
{ref}`stage <trino-concept-stage>` exactly matches what the downstream stage
expects.

## `optimizer.use-table-scan-node-partitioning`

- **Type:** {ref}`prop-type-boolean`
- **Default value:** `true`
- **Session property:** `use_table_scan_node_partitioning`

Use connector provided table node partitioning when reading tables.
For example, table node partitioning corresponds to Hive table buckets.
When set to `true` and minimal partition to task ratio is matched or exceeded,
each table partition is read by a separate worker. The minimal ratio is defined in
`optimizer.table-scan-node-partitioning-min-bucket-to-task-ratio`.

Partition reader assignments are distributed across workers for
parallel processing. Use of table scan node partitioning can improve
query performance by reducing query complexity. For example,
cluster wide data reshuffling might not be needed when processing an aggregation query.
However, query parallelism might be reduced when partition count is
low compared to number of workers.

## `optimizer.table-scan-node-partitioning-min-bucket-to-task-ratio`

- **Type:** {ref}`prop-type-double`
- **Default value:** `0.5`
- **Session property:** `table_scan_node_partitioning_min_bucket_to_task_ratio`

Specifies minimal bucket to task ratio that has to be matched or exceeded in order
to use table scan node partitioning. When the table bucket count is small
compared to the number of workers, then the table scan is distributed across
all workers for improved parallelism.

## `optimizer.colocated-joins-enabled`

- **Type:** {ref}`prop-type-boolean`
- **Default value:** `true`
- **Session property:** `colocated_join`

Use co-located joins when both sides of a join have the same table partitioning on the join keys
and the conditions for `optimizer.use-table-scan-node-partitioning` are met.
For example, a join on bucketed Hive tables with matching bucketing schemes can
avoid exchanging data between workers using a co-located join to improve query performance.

## `optimizer.filter-conjunction-independence-factor`

- **Type:** {ref}`prop-type-double`
- **Default value:** `0.75`
- **Min allowed value:** `0`
- **Max allowed value:** `1`
- **Session property:** `filter_conjunction_independence_factor`

Scales the strength of independence assumption for estimating the selectivity of
the conjunction of multiple predicates. Lower values for this property will produce
more conservative estimates by assuming a greater degree of correlation between the
columns of the predicates in a conjunction. A value of `0` results in the
optimizer assuming that the columns of the predicates are fully correlated and only
the most selective predicate drives the selectivity of a conjunction of predicates.

## `optimizer.join-multi-clause-independence-factor`

- **Type:** {ref}`prop-type-double`
- **Default value:** `0.25`
- **Min allowed value:** `0`
- **Max allowed value:** `1`
- **Session property:** `join_multi_clause_independence_factor` 

Scales the strength of independence assumption for estimating the output of a
multi-clause join. Lower values for this property will produce more
conservative estimates by assuming a greater degree of correlation between the
columns of the clauses in a join. A value of `0` results in the optimizer
assuming that the columns of the join clauses are fully correlated and only
the most selective clause drives the selectivity of the join.

## `optimizer.non-estimatable-predicate-approximation.enabled`

- **Type:** {ref}`prop-type-boolean`
- **Default value:** `true`
- **Session property:** `non_estimatable_predicate_approximation_enabled`

Enables approximation of the output row count of filters whose costs cannot be
accurately estimated even with complete statistics. This allows the optimizer to
produce more efficient plans in the presence of filters which were previously
not estimated.

## `optimizer.join-partitioned-build-min-row-count`

- **Type:** {ref}`prop-type-integer`
- **Default value:** `1000000`
- **Min allowed value:** `0`
- **Session property:** `join_partitioned_build_min_row_count`

The minimum number of join build side rows required to use partitioned join lookup.
If the build side of a join is estimated to be smaller than the configured threshold,
single threaded join lookup is used to improve join performance.
A value of `0` disables this optimization.

## `optimizer.min-input-size-per-task`

- **Type:** {ref}`prop-type-data-size`
- **Default value:** `5GB`
- **Min allowed value:** `0MB`
- **Session property:** `min_input_size_per_task`

The minimum input size required per task. This will help optimizer to determine hash
partition count for joins and aggregations. Limiting hash partition count for small queries
increases concurrency on large clusters where multiple small queries are running concurrently.
The estimated value will always be between `min_hash_partition_count` and
`max_hash_partition_count` session property.
A value of `0MB` disables this optimization.

## `optimizer.min-input-rows-per-task`

- **Type:** {ref}`prop-type-integer`
- **Default value:** `10000000`
- **Min allowed value:** `0`
- **Session property:** `min_input_rows_per_task`

The minimum number of input rows required per task. This will help optimizer to determine hash
partition count for joins and aggregations. Limiting hash partition count for small queries
increases concurrency on large clusters where multiple small queries are running concurrently.
The estimated value will always be between `min_hash_partition_count` and
`max_hash_partition_count` session property.
A value of `0` disables this optimization.

## `optimizer.use-cost-based-partitioning`

- **Type:** {ref}`prop-type-boolean`
- **Default value:** `true`
- **Session property:** `use_cost_based_partitioning`

When enabled the cost based optimizer is used to determine if repartitioning the output of an
already partitioned stage is necessary.
