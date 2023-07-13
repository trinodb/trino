/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.execution.DynamicFilterConfig;
import io.trino.execution.QueryManagerConfig;
import io.trino.execution.TaskManagerConfig;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.memory.MemoryManagerConfig;
import io.trino.memory.NodeMemoryConfig;
import io.trino.operator.RetryPolicy;
import io.trino.spi.TrinoException;
import io.trino.spi.session.PropertyMetadata;
import io.trino.sql.planner.OptimizerConfig;
import io.trino.sql.planner.OptimizerConfig.JoinDistributionType;
import io.trino.sql.planner.OptimizerConfig.JoinReorderingStrategy;
import io.trino.sql.planner.OptimizerConfig.MarkDistinctStrategy;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.execution.QueryManagerConfig.FAULT_TOLERANT_EXECUTION_MAX_PARTITION_COUNT_LIMIT;
import static io.trino.execution.QueryManagerConfig.MAX_TASK_RETRY_ATTEMPTS;
import static io.trino.plugin.base.session.PropertyMetadataUtil.dataSizeProperty;
import static io.trino.plugin.base.session.PropertyMetadataUtil.durationProperty;
import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.doubleProperty;
import static io.trino.spi.session.PropertyMetadata.enumProperty;
import static io.trino.spi.session.PropertyMetadata.integerProperty;
import static io.trino.spi.session.PropertyMetadata.longProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static java.lang.Math.min;
import static java.lang.String.format;

public final class SystemSessionProperties
        implements SystemSessionPropertiesProvider
{
    public static final String OPTIMIZE_HASH_GENERATION = "optimize_hash_generation";
    public static final String JOIN_DISTRIBUTION_TYPE = "join_distribution_type";
    public static final String JOIN_MAX_BROADCAST_TABLE_SIZE = "join_max_broadcast_table_size";
    public static final String JOIN_MULTI_CLAUSE_INDEPENDENCE_FACTOR = "join_multi_clause_independence_factor";
    public static final String DETERMINE_PARTITION_COUNT_FOR_WRITE_ENABLED = "determine_partition_count_for_write_enabled";
    public static final String MAX_HASH_PARTITION_COUNT = "max_hash_partition_count";
    public static final String MIN_HASH_PARTITION_COUNT = "min_hash_partition_count";
    public static final String MIN_HASH_PARTITION_COUNT_FOR_WRITE = "min_hash_partition_count_for_write";
    public static final String PREFER_STREAMING_OPERATORS = "prefer_streaming_operators";
    public static final String TASK_WRITER_COUNT = "task_writer_count";
    public static final String TASK_PARTITIONED_WRITER_COUNT = "task_partitioned_writer_count";
    public static final String TASK_CONCURRENCY = "task_concurrency";
    public static final String TASK_SHARE_INDEX_LOADING = "task_share_index_loading";
    public static final String QUERY_MAX_MEMORY = "query_max_memory";
    public static final String QUERY_MAX_TOTAL_MEMORY = "query_max_total_memory";
    public static final String QUERY_MAX_EXECUTION_TIME = "query_max_execution_time";
    public static final String QUERY_MAX_PLANNING_TIME = "query_max_planning_time";
    public static final String QUERY_MAX_RUN_TIME = "query_max_run_time";
    public static final String RESOURCE_OVERCOMMIT = "resource_overcommit";
    public static final String QUERY_MAX_CPU_TIME = "query_max_cpu_time";
    public static final String QUERY_MAX_SCAN_PHYSICAL_BYTES = "query_max_scan_physical_bytes";
    public static final String QUERY_MAX_STAGE_COUNT = "query_max_stage_count";
    public static final String REDISTRIBUTE_WRITES = "redistribute_writes";
    public static final String USE_PREFERRED_WRITE_PARTITIONING = "use_preferred_write_partitioning";
    public static final String SCALE_WRITERS = "scale_writers";
    public static final String TASK_SCALE_WRITERS_ENABLED = "task_scale_writers_enabled";
    public static final String MAX_WRITER_TASKS_COUNT = "max_writer_tasks_count";
    public static final String TASK_SCALE_WRITERS_MAX_WRITER_COUNT = "task_scale_writers_max_writer_count";
    public static final String WRITER_MIN_SIZE = "writer_min_size";
    public static final String PUSH_TABLE_WRITE_THROUGH_UNION = "push_table_write_through_union";
    public static final String EXECUTION_POLICY = "execution_policy";
    public static final String DICTIONARY_AGGREGATION = "dictionary_aggregation";
    public static final String USE_TABLE_SCAN_NODE_PARTITIONING = "use_table_scan_node_partitioning";
    public static final String TABLE_SCAN_NODE_PARTITIONING_MIN_BUCKET_TO_TASK_RATIO = "table_scan_node_partitioning_min_bucket_to_task_ratio";
    public static final String SPATIAL_JOIN = "spatial_join";
    public static final String SPATIAL_PARTITIONING_TABLE_NAME = "spatial_partitioning_table_name";
    public static final String COLOCATED_JOIN = "colocated_join";
    public static final String JOIN_REORDERING_STRATEGY = "join_reordering_strategy";
    public static final String MAX_REORDERED_JOINS = "max_reordered_joins";
    public static final String INITIAL_SPLITS_PER_NODE = "initial_splits_per_node";
    public static final String SPLIT_CONCURRENCY_ADJUSTMENT_INTERVAL = "split_concurrency_adjustment_interval";
    public static final String OPTIMIZE_METADATA_QUERIES = "optimize_metadata_queries";
    public static final String QUERY_PRIORITY = "query_priority";
    public static final String SPILL_ENABLED = "spill_enabled";
    public static final String AGGREGATION_OPERATOR_UNSPILL_MEMORY_LIMIT = "aggregation_operator_unspill_memory_limit";
    public static final String OPTIMIZE_DISTINCT_AGGREGATIONS = "optimize_mixed_distinct_aggregations";
    public static final String ITERATIVE_OPTIMIZER_TIMEOUT = "iterative_optimizer_timeout";
    public static final String ENABLE_FORCED_EXCHANGE_BELOW_GROUP_ID = "enable_forced_exchange_below_group_id";
    public static final String EXCHANGE_COMPRESSION = "exchange_compression";
    public static final String ENABLE_INTERMEDIATE_AGGREGATIONS = "enable_intermediate_aggregations";
    public static final String PUSH_AGGREGATION_THROUGH_OUTER_JOIN = "push_aggregation_through_outer_join";
    public static final String PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN = "push_partial_aggregation_through_join";
    public static final String PRE_AGGREGATE_CASE_AGGREGATIONS_ENABLED = "pre_aggregate_case_aggregations_enabled";
    public static final String PARSE_DECIMAL_LITERALS_AS_DOUBLE = "parse_decimal_literals_as_double";
    public static final String FORCE_SINGLE_NODE_OUTPUT = "force_single_node_output";
    public static final String FILTER_AND_PROJECT_MIN_OUTPUT_PAGE_SIZE = "filter_and_project_min_output_page_size";
    public static final String FILTER_AND_PROJECT_MIN_OUTPUT_PAGE_ROW_COUNT = "filter_and_project_min_output_page_row_count";
    public static final String DISTRIBUTED_SORT = "distributed_sort";
    public static final String USE_PARTIAL_TOPN = "use_partial_topn";
    public static final String USE_PARTIAL_DISTINCT_LIMIT = "use_partial_distinct_limit";
    public static final String MAX_RECURSION_DEPTH = "max_recursion_depth";
    public static final String USE_MARK_DISTINCT = "use_mark_distinct";
    public static final String MARK_DISTINCT_STRATEGY = "mark_distinct_strategy";
    public static final String PREFER_PARTIAL_AGGREGATION = "prefer_partial_aggregation";
    public static final String OPTIMIZE_TOP_N_RANKING = "optimize_top_n_ranking";
    public static final String MAX_GROUPING_SETS = "max_grouping_sets";
    public static final String STATISTICS_CPU_TIMER_ENABLED = "statistics_cpu_timer_enabled";
    public static final String ENABLE_STATS_CALCULATOR = "enable_stats_calculator";
    public static final String STATISTICS_PRECALCULATION_FOR_PUSHDOWN_ENABLED = "statistics_precalculation_for_pushdown_enabled";
    public static final String COLLECT_PLAN_STATISTICS_FOR_ALL_QUERIES = "collect_plan_statistics_for_all_queries";
    public static final String IGNORE_STATS_CALCULATOR_FAILURES = "ignore_stats_calculator_failures";
    public static final String MAX_DRIVERS_PER_TASK = "max_drivers_per_task";
    public static final String DEFAULT_FILTER_FACTOR_ENABLED = "default_filter_factor_enabled";
    public static final String FILTER_CONJUNCTION_INDEPENDENCE_FACTOR = "filter_conjunction_independence_factor";
    public static final String NON_ESTIMATABLE_PREDICATE_APPROXIMATION_ENABLED = "non_estimatable_predicate_approximation_enabled";
    public static final String SKIP_REDUNDANT_SORT = "skip_redundant_sort";
    public static final String ALLOW_PUSHDOWN_INTO_CONNECTORS = "allow_pushdown_into_connectors";
    public static final String COMPLEX_EXPRESSION_PUSHDOWN = "complex_expression_pushdown";
    public static final String PREDICATE_PUSHDOWN_USE_TABLE_PROPERTIES = "predicate_pushdown_use_table_properties";
    public static final String LATE_MATERIALIZATION = "late_materialization";
    public static final String ENABLE_DYNAMIC_FILTERING = "enable_dynamic_filtering";
    public static final String ENABLE_COORDINATOR_DYNAMIC_FILTERS_DISTRIBUTION = "enable_coordinator_dynamic_filters_distribution";
    public static final String ENABLE_LARGE_DYNAMIC_FILTERS = "enable_large_dynamic_filters";
    public static final String QUERY_MAX_MEMORY_PER_NODE = "query_max_memory_per_node";
    public static final String IGNORE_DOWNSTREAM_PREFERENCES = "ignore_downstream_preferences";
    public static final String FILTERING_SEMI_JOIN_TO_INNER = "rewrite_filtering_semi_join_to_inner_join";
    public static final String OPTIMIZE_DUPLICATE_INSENSITIVE_JOINS = "optimize_duplicate_insensitive_joins";
    public static final String REQUIRED_WORKERS_COUNT = "required_workers_count";
    public static final String REQUIRED_WORKERS_MAX_WAIT_TIME = "required_workers_max_wait_time";
    public static final String COST_ESTIMATION_WORKER_COUNT = "cost_estimation_worker_count";
    public static final String OMIT_DATETIME_TYPE_PRECISION = "omit_datetime_type_precision";
    public static final String USE_LEGACY_WINDOW_FILTER_PUSHDOWN = "use_legacy_window_filter_pushdown";
    public static final String MAX_UNACKNOWLEDGED_SPLITS_PER_TASK = "max_unacknowledged_splits_per_task";
    public static final String MERGE_PROJECT_WITH_VALUES = "merge_project_with_values";
    public static final String TIME_ZONE_ID = "time_zone_id";
    public static final String LEGACY_CATALOG_ROLES = "legacy_catalog_roles";
    public static final String INCREMENTAL_HASH_ARRAY_LOAD_FACTOR_ENABLED = "incremental_hash_array_load_factor_enabled";
    public static final String MAX_PARTIAL_TOP_N_MEMORY = "max_partial_top_n_memory";
    public static final String RETRY_POLICY = "retry_policy";
    public static final String QUERY_RETRY_ATTEMPTS = "query_retry_attempts";
    public static final String TASK_RETRY_ATTEMPTS_PER_TASK = "task_retry_attempts_per_task";
    public static final String MAX_TASKS_WAITING_FOR_EXECUTION_PER_QUERY = "max_tasks_waiting_for_execution_per_query";
    public static final String MAX_TASKS_WAITING_FOR_NODE_PER_STAGE = "max_tasks_waiting_for_node_per_stage";
    public static final String RETRY_INITIAL_DELAY = "retry_initial_delay";
    public static final String RETRY_MAX_DELAY = "retry_max_delay";
    public static final String RETRY_DELAY_SCALE_FACTOR = "retry_delay_scale_factor";
    public static final String LEGACY_MATERIALIZED_VIEW_GRACE_PERIOD = "legacy_materialized_view_grace_period";
    public static final String HIDE_INACCESSIBLE_COLUMNS = "hide_inaccessible_columns";
    public static final String FAULT_TOLERANT_EXECUTION_ARBITRARY_DISTRIBUTION_COMPUTE_TASK_TARGET_SIZE_GROWTH_PERIOD = "fault_tolerant_execution_arbitrary_distribution_compute_task_target_size_growth_period";
    public static final String FAULT_TOLERANT_EXECUTION_ARBITRARY_DISTRIBUTION_COMPUTE_TASK_TARGET_SIZE_GROWTH_FACTOR = "fault_tolerant_execution_arbitrary_distribution_compute_task_target_size_growth_factor";
    public static final String FAULT_TOLERANT_EXECUTION_ARBITRARY_DISTRIBUTION_COMPUTE_TASK_TARGET_SIZE_MIN = "fault_tolerant_execution_arbitrary_distribution_compute_task_target_size_min";
    public static final String FAULT_TOLERANT_EXECUTION_ARBITRARY_DISTRIBUTION_COMPUTE_TASK_TARGET_SIZE_MAX = "fault_tolerant_execution_arbitrary_distribution_compute_task_target_size_max";
    public static final String FAULT_TOLERANT_EXECUTION_ARBITRARY_DISTRIBUTION_WRITE_TASK_TARGET_SIZE_GROWTH_PERIOD = "fault_tolerant_execution_arbitrary_distribution_write_task_target_size_growth_period";
    public static final String FAULT_TOLERANT_EXECUTION_ARBITRARY_DISTRIBUTION_WRITE_TASK_TARGET_SIZE_GROWTH_FACTOR = "fault_tolerant_execution_arbitrary_distribution_write_task_target_size_growth_factor";
    public static final String FAULT_TOLERANT_EXECUTION_ARBITRARY_DISTRIBUTION_WRITE_TASK_TARGET_SIZE_MIN = "fault_tolerant_execution_arbitrary_distribution_write_task_target_size_min";
    public static final String FAULT_TOLERANT_EXECUTION_ARBITRARY_DISTRIBUTION_WRITE_TASK_TARGET_SIZE_MAX = "fault_tolerant_execution_arbitrary_distribution_write_task_target_size_max";
    public static final String FAULT_TOLERANT_EXECUTION_HASH_DISTRIBUTION_COMPUTE_TASK_TARGET_SIZE = "fault_tolerant_execution_hash_distribution_compute_task_target_size";
    public static final String FAULT_TOLERANT_EXECUTION_HASH_DISTRIBUTION_WRITE_TASK_TARGET_SIZE = "fault_tolerant_execution_hash_distribution_write_task_target_size";
    public static final String FAULT_TOLERANT_EXECUTION_HASH_DISTRIBUTION_WRITE_TASK_TARGET_MAX_COUNT = "fault_tolerant_execution_hash_distribution_write_task_target_max_count";
    public static final String FAULT_TOLERANT_EXECUTION_STANDARD_SPLIT_SIZE = "fault_tolerant_execution_standard_split_size";
    public static final String FAULT_TOLERANT_EXECUTION_MAX_TASK_SPLIT_COUNT = "fault_tolerant_execution_max_task_split_count";
    public static final String FAULT_TOLERANT_EXECUTION_COORDINATOR_TASK_MEMORY = "fault_tolerant_execution_coordinator_task_memory";
    public static final String FAULT_TOLERANT_EXECUTION_TASK_MEMORY = "fault_tolerant_execution_task_memory";
    public static final String FAULT_TOLERANT_EXECUTION_TASK_MEMORY_GROWTH_FACTOR = "fault_tolerant_execution_task_memory_growth_factor";
    public static final String FAULT_TOLERANT_EXECUTION_TASK_MEMORY_ESTIMATION_QUANTILE = "fault_tolerant_execution_task_memory_estimation_quantile";
    public static final String FAULT_TOLERANT_EXECUTION_MAX_PARTITION_COUNT = "fault_tolerant_execution_max_partition_count";
    public static final String FAULT_TOLERANT_EXECUTION_MIN_PARTITION_COUNT = "fault_tolerant_execution_min_partition_count";
    public static final String FAULT_TOLERANT_EXECUTION_MIN_PARTITION_COUNT_FOR_WRITE = "fault_tolerant_execution_min_partition_count_for_write";
    public static final String ADAPTIVE_PARTIAL_AGGREGATION_ENABLED = "adaptive_partial_aggregation_enabled";
    public static final String ADAPTIVE_PARTIAL_AGGREGATION_UNIQUE_ROWS_RATIO_THRESHOLD = "adaptive_partial_aggregation_unique_rows_ratio_threshold";
    public static final String REMOTE_TASK_ADAPTIVE_UPDATE_REQUEST_SIZE_ENABLED = "remote_task_adaptive_update_request_size_enabled";
    public static final String REMOTE_TASK_MAX_REQUEST_SIZE = "remote_task_max_request_size";
    public static final String REMOTE_TASK_REQUEST_SIZE_HEADROOM = "remote_task_request_size_headroom";
    public static final String REMOTE_TASK_GUARANTEED_SPLITS_PER_REQUEST = "remote_task_guaranteed_splits_per_request";
    public static final String JOIN_PARTITIONED_BUILD_MIN_ROW_COUNT = "join_partitioned_build_min_row_count";
    public static final String MIN_INPUT_SIZE_PER_TASK = "min_input_size_per_task";
    public static final String MIN_INPUT_ROWS_PER_TASK = "min_input_rows_per_task";
    public static final String USE_EXACT_PARTITIONING = "use_exact_partitioning";
    public static final String USE_COST_BASED_PARTITIONING = "use_cost_based_partitioning";
    public static final String FORCE_SPILLING_JOIN = "force_spilling_join";
    public static final String FAULT_TOLERANT_EXECUTION_FORCE_PREFERRED_WRITE_PARTITIONING_ENABLED = "fault_tolerant_execution_force_preferred_write_partitioning_enabled";
    public static final String PAGE_PARTITIONING_BUFFER_POOL_SIZE = "page_partitioning_buffer_pool_size";

    private final List<PropertyMetadata<?>> sessionProperties;

    public SystemSessionProperties()
    {
        this(
                new QueryManagerConfig(),
                new TaskManagerConfig(),
                new MemoryManagerConfig(),
                new FeaturesConfig(),
                new OptimizerConfig(),
                new NodeMemoryConfig(),
                new DynamicFilterConfig(),
                new NodeSchedulerConfig());
    }

    @Inject
    public SystemSessionProperties(
            QueryManagerConfig queryManagerConfig,
            TaskManagerConfig taskManagerConfig,
            MemoryManagerConfig memoryManagerConfig,
            FeaturesConfig featuresConfig,
            OptimizerConfig optimizerConfig,
            NodeMemoryConfig nodeMemoryConfig,
            DynamicFilterConfig dynamicFilterConfig,
            NodeSchedulerConfig nodeSchedulerConfig)
    {
        sessionProperties = ImmutableList.of(
                stringProperty(
                        EXECUTION_POLICY,
                        "Policy used for scheduling query tasks",
                        queryManagerConfig.getQueryExecutionPolicy(),
                        false),
                booleanProperty(
                        OPTIMIZE_HASH_GENERATION,
                        "Compute hash codes for distribution, joins, and aggregations early in query plan",
                        optimizerConfig.isOptimizeHashGeneration(),
                        false),
                enumProperty(
                        JOIN_DISTRIBUTION_TYPE,
                        "Join distribution type",
                        JoinDistributionType.class,
                        optimizerConfig.getJoinDistributionType(),
                        false),
                dataSizeProperty(
                        JOIN_MAX_BROADCAST_TABLE_SIZE,
                        "Maximum estimated size of a table that can be broadcast when using automatic join type selection",
                        optimizerConfig.getJoinMaxBroadcastTableSize(),
                        false),
                new PropertyMetadata<>(
                        JOIN_MULTI_CLAUSE_INDEPENDENCE_FACTOR,
                        "Scales the strength of independence assumption for selectivity estimates of multi-clause joins",
                        DOUBLE,
                        Double.class,
                        optimizerConfig.getJoinMultiClauseIndependenceFactor(),
                        false,
                        value -> validateDoubleRange(value, JOIN_MULTI_CLAUSE_INDEPENDENCE_FACTOR, 0.0, 1.0),
                        value -> value),
                booleanProperty(
                        DETERMINE_PARTITION_COUNT_FOR_WRITE_ENABLED,
                        "Determine the number of partitions based on amount of data read and processed by the query for write queries",
                        queryManagerConfig.isDeterminePartitionCountForWriteEnabled(),
                        false),
                integerProperty(
                        MAX_HASH_PARTITION_COUNT,
                        "Maximum number of partitions for distributed joins and aggregations",
                        queryManagerConfig.getMaxHashPartitionCount(),
                        value -> validateIntegerValue(value, MAX_HASH_PARTITION_COUNT, 1, false),
                        false),
                integerProperty(
                        MIN_HASH_PARTITION_COUNT,
                        "Minimum number of partitions for distributed joins and aggregations",
                        queryManagerConfig.getMinHashPartitionCount(),
                        value -> validateIntegerValue(value, MIN_HASH_PARTITION_COUNT, 1, false),
                        false),
                integerProperty(
                        MIN_HASH_PARTITION_COUNT_FOR_WRITE,
                        "Minimum number of partitions for distributed joins and aggregations in write queries",
                        queryManagerConfig.getMinHashPartitionCountForWrite(),
                        value -> validateIntegerValue(value, MIN_HASH_PARTITION_COUNT_FOR_WRITE, 1, false),
                        false),
                booleanProperty(
                        PREFER_STREAMING_OPERATORS,
                        "Prefer source table layouts that produce streaming operators",
                        false,
                        false),
                integerProperty(
                        TASK_WRITER_COUNT,
                        "Number of local parallel table writers per task when prefer partitioning and task writer scaling are not used",
                        taskManagerConfig.getWriterCount(),
                        false),
                integerProperty(
                        TASK_PARTITIONED_WRITER_COUNT,
                        "Number of local parallel table writers per task when prefer partitioning is used",
                        taskManagerConfig.getPartitionedWriterCount(),
                        value -> validateValueIsPowerOfTwo(value, TASK_PARTITIONED_WRITER_COUNT),
                        false),
                booleanProperty(
                        REDISTRIBUTE_WRITES,
                        "Force parallel distributed writes",
                        featuresConfig.isRedistributeWrites(),
                        false),
                booleanProperty(
                        USE_PREFERRED_WRITE_PARTITIONING,
                        "Use preferred write partitioning",
                        optimizerConfig.isUsePreferredWritePartitioning(),
                        false),
                booleanProperty(
                        SCALE_WRITERS,
                        "Scale out writers based on throughput (use minimum necessary)",
                        featuresConfig.isScaleWriters(),
                        false),
                integerProperty(
                        MAX_WRITER_TASKS_COUNT,
                        "Maximum number of tasks that will participate in writing data",
                        queryManagerConfig.getMaxWriterTasksCount(),
                        value -> validateIntegerValue(value, MAX_WRITER_TASKS_COUNT, 1, false),
                        false),
                booleanProperty(
                        TASK_SCALE_WRITERS_ENABLED,
                        "Scale the number of concurrent table writers per task based on throughput",
                        taskManagerConfig.isScaleWritersEnabled(),
                        false),
                integerProperty(
                        TASK_SCALE_WRITERS_MAX_WRITER_COUNT,
                        "Maximum number of writers per task up to which scaling will happen if task.scale-writers.enabled is set",
                        taskManagerConfig.getScaleWritersMaxWriterCount(),
                        true),
                dataSizeProperty(
                        WRITER_MIN_SIZE,
                        "Target minimum size of writer output when scaling writers",
                        featuresConfig.getWriterMinSize(),
                        false),
                booleanProperty(
                        PUSH_TABLE_WRITE_THROUGH_UNION,
                        "Parallelize writes when using UNION ALL in queries that write data",
                        optimizerConfig.isPushTableWriteThroughUnion(),
                        false),
                integerProperty(
                        TASK_CONCURRENCY,
                        "Default number of local parallel jobs per worker",
                        taskManagerConfig.getTaskConcurrency(),
                        value -> validateValueIsPowerOfTwo(value, TASK_CONCURRENCY),
                        false),
                booleanProperty(
                        TASK_SHARE_INDEX_LOADING,
                        "Share index join lookups and caching within a task",
                        taskManagerConfig.isShareIndexLoading(),
                        false),
                durationProperty(
                        QUERY_MAX_RUN_TIME,
                        "Maximum run time of a query (includes the queueing time)",
                        queryManagerConfig.getQueryMaxRunTime(),
                        false),
                durationProperty(
                        QUERY_MAX_EXECUTION_TIME,
                        "Maximum execution time of a query",
                        queryManagerConfig.getQueryMaxExecutionTime(),
                        false),
                durationProperty(
                        QUERY_MAX_PLANNING_TIME,
                        "Maximum planning time of a query",
                        queryManagerConfig.getQueryMaxPlanningTime(),
                        false),
                durationProperty(
                        QUERY_MAX_CPU_TIME,
                        "Maximum CPU time of a query",
                        queryManagerConfig.getQueryMaxCpuTime(),
                        false),
                dataSizeProperty(
                        QUERY_MAX_MEMORY,
                        "Maximum amount of distributed memory a query can use",
                        memoryManagerConfig.getMaxQueryMemory(),
                        true),
                dataSizeProperty(
                        QUERY_MAX_TOTAL_MEMORY,
                        "Maximum amount of distributed total memory a query can use",
                        memoryManagerConfig.getMaxQueryTotalMemory(),
                        true),
                dataSizeProperty(
                        QUERY_MAX_SCAN_PHYSICAL_BYTES,
                        "Maximum scan physical bytes of a query",
                        queryManagerConfig.getQueryMaxScanPhysicalBytes().orElse(null),
                        false),
                booleanProperty(
                        RESOURCE_OVERCOMMIT,
                        "Use resources which are not guaranteed to be available to the query",
                        false,
                        false),
                integerProperty(
                        QUERY_MAX_STAGE_COUNT,
                        "Temporary: Maximum number of stages a query can have",
                        queryManagerConfig.getMaxStageCount(),
                        true),
                booleanProperty(
                        DICTIONARY_AGGREGATION,
                        "Enable optimization for aggregations on dictionaries",
                        optimizerConfig.isDictionaryAggregation(),
                        false),
                integerProperty(
                        INITIAL_SPLITS_PER_NODE,
                        "The number of splits each node will run per task, initially",
                        taskManagerConfig.getInitialSplitsPerNode(),
                        false),
                durationProperty(
                        SPLIT_CONCURRENCY_ADJUSTMENT_INTERVAL,
                        "Experimental: Interval between changes to the number of concurrent splits per node",
                        taskManagerConfig.getSplitConcurrencyAdjustmentInterval(),
                        false),
                booleanProperty(
                        OPTIMIZE_METADATA_QUERIES,
                        "Enable optimization for metadata queries",
                        optimizerConfig.isOptimizeMetadataQueries(),
                        false),
                integerProperty(
                        QUERY_PRIORITY,
                        "The priority of queries. Larger numbers are higher priority",
                        1,
                        false),
                booleanProperty(
                        USE_TABLE_SCAN_NODE_PARTITIONING,
                        "Adapt plan to node pre-partitioned tables",
                        optimizerConfig.isUseTableScanNodePartitioning(),
                        false),
                doubleProperty(
                        TABLE_SCAN_NODE_PARTITIONING_MIN_BUCKET_TO_TASK_RATIO,
                        "Min table scan bucket to task ratio for which plan will be adopted to node pre-partitioned tables",
                        optimizerConfig.getTableScanNodePartitioningMinBucketToTaskRatio(),
                        false),
                enumProperty(
                        JOIN_REORDERING_STRATEGY,
                        "Join reordering strategy",
                        JoinReorderingStrategy.class,
                        optimizerConfig.getJoinReorderingStrategy(),
                        false),
                new PropertyMetadata<>(
                        MAX_REORDERED_JOINS,
                        "The maximum number of joins to reorder as one group in cost-based join reordering",
                        INTEGER,
                        Integer.class,
                        optimizerConfig.getMaxReorderedJoins(),
                        false,
                        value -> {
                            int intValue = (int) value;
                            if (intValue < 2) {
                                throw new TrinoException(INVALID_SESSION_PROPERTY, format("%s must be greater than or equal to 2: %s", MAX_REORDERED_JOINS, intValue));
                            }
                            return intValue;
                        },
                        value -> value),
                booleanProperty(
                        COLOCATED_JOIN,
                        "Use a colocated join when possible",
                        optimizerConfig.isColocatedJoinsEnabled(),
                        false),
                booleanProperty(
                        SPATIAL_JOIN,
                        "Use spatial index for spatial join when possible",
                        optimizerConfig.isSpatialJoinsEnabled(),
                        false),
                stringProperty(
                        SPATIAL_PARTITIONING_TABLE_NAME,
                        "Name of the table containing spatial partitioning scheme",
                        null,
                        false),
                booleanProperty(
                        SPILL_ENABLED,
                        "Enable spilling",
                        featuresConfig.isSpillEnabled(),
                        false),
                dataSizeProperty(
                        AGGREGATION_OPERATOR_UNSPILL_MEMORY_LIMIT,
                        "How much memory should be allocated per aggregation operator in unspilling process",
                        featuresConfig.getAggregationOperatorUnspillMemoryLimit(),
                        false),
                booleanProperty(
                        OPTIMIZE_DISTINCT_AGGREGATIONS,
                        "Optimize mixed non-distinct and distinct aggregations",
                        optimizerConfig.isOptimizeMixedDistinctAggregations(),
                        false),
                durationProperty(
                        ITERATIVE_OPTIMIZER_TIMEOUT,
                        "Timeout for plan optimization in iterative optimizer",
                        optimizerConfig.getIterativeOptimizerTimeout(),
                        false),
                booleanProperty(
                        ENABLE_FORCED_EXCHANGE_BELOW_GROUP_ID,
                        "Enable a stats-based rule adding exchanges below GroupId",
                        optimizerConfig.isEnableForcedExchangeBelowGroupId(),
                        true),
                booleanProperty(
                        EXCHANGE_COMPRESSION,
                        "Enable compression in exchanges",
                        featuresConfig.isExchangeCompressionEnabled(),
                        false),
                booleanProperty(
                        ENABLE_INTERMEDIATE_AGGREGATIONS,
                        "Enable the use of intermediate aggregations",
                        optimizerConfig.isEnableIntermediateAggregations(),
                        false),
                booleanProperty(
                        PUSH_AGGREGATION_THROUGH_OUTER_JOIN,
                        "Allow pushing aggregations below joins",
                        optimizerConfig.isPushAggregationThroughOuterJoin(),
                        false),
                booleanProperty(
                        PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN,
                        "Push partial aggregations below joins",
                        optimizerConfig.isPushPartialAggregationThroughJoin(),
                        false),
                booleanProperty(
                        PRE_AGGREGATE_CASE_AGGREGATIONS_ENABLED,
                        "Pre-aggregate rows before GROUP BY with multiple CASE aggregations on same column",
                        optimizerConfig.isPreAggregateCaseAggregationsEnabled(),
                        false),
                booleanProperty(
                        PARSE_DECIMAL_LITERALS_AS_DOUBLE,
                        "Parse decimal literals as DOUBLE instead of DECIMAL",
                        featuresConfig.isParseDecimalLiteralsAsDouble(),
                        false),
                booleanProperty(
                        FORCE_SINGLE_NODE_OUTPUT,
                        "Force single node output",
                        optimizerConfig.isForceSingleNodeOutput(),
                        true),
                dataSizeProperty(
                        FILTER_AND_PROJECT_MIN_OUTPUT_PAGE_SIZE,
                        "Experimental: Minimum output page size for filter and project operators",
                        featuresConfig.getFilterAndProjectMinOutputPageSize(),
                        false),
                integerProperty(
                        FILTER_AND_PROJECT_MIN_OUTPUT_PAGE_ROW_COUNT,
                        "Experimental: Minimum output page row count for filter and project operators",
                        featuresConfig.getFilterAndProjectMinOutputPageRowCount(),
                        false),
                booleanProperty(
                        DISTRIBUTED_SORT,
                        "Parallelize sort across multiple nodes",
                        optimizerConfig.isDistributedSortEnabled(),
                        false),
                booleanProperty(
                        // Useful to make EXPLAIN or SHOW STATS provide stats for a query involving TopN
                        USE_PARTIAL_TOPN,
                        "Use partial TopN",
                        true,
                        true),
                booleanProperty(
                        // Useful to make EXPLAIN or SHOW STATS provide stats for a query involving TopN
                        USE_PARTIAL_DISTINCT_LIMIT,
                        "Use partial Distinct Limit",
                        true,
                        true),
                new PropertyMetadata<>(
                        MAX_RECURSION_DEPTH,
                        "Maximum recursion depth for recursive common table expression",
                        INTEGER,
                        Integer.class,
                        featuresConfig.getMaxRecursionDepth(),
                        false,
                        value -> validateIntegerValue(value, MAX_RECURSION_DEPTH, 1, false),
                        object -> object),
                booleanProperty(
                        USE_MARK_DISTINCT,
                        "Implement DISTINCT aggregations using MarkDistinct",
                        optimizerConfig.isUseMarkDistinct(),
                        false),
                enumProperty(
                        MARK_DISTINCT_STRATEGY,
                        "",
                        MarkDistinctStrategy.class,
                        optimizerConfig.getMarkDistinctStrategy(),
                        false),
                booleanProperty(
                        PREFER_PARTIAL_AGGREGATION,
                        "Prefer splitting aggregations into partial and final stages",
                        optimizerConfig.isPreferPartialAggregation(),
                        false),
                booleanProperty(
                        OPTIMIZE_TOP_N_RANKING,
                        "Use top N ranking optimization",
                        optimizerConfig.isOptimizeTopNRanking(),
                        false),
                integerProperty(
                        MAX_GROUPING_SETS,
                        "Maximum number of grouping sets in a GROUP BY",
                        featuresConfig.getMaxGroupingSets(),
                        true),
                booleanProperty(
                        STATISTICS_CPU_TIMER_ENABLED,
                        "Experimental: Enable cpu time tracking for automatic column statistics collection on write",
                        taskManagerConfig.isStatisticsCpuTimerEnabled(),
                        false),
                booleanProperty(
                        ENABLE_STATS_CALCULATOR,
                        "Enable statistics calculator",
                        optimizerConfig.isEnableStatsCalculator(),
                        false),
                booleanProperty(
                        STATISTICS_PRECALCULATION_FOR_PUSHDOWN_ENABLED,
                        "Enable statistics precalculation for pushdown",
                        optimizerConfig.isStatisticsPrecalculationForPushdownEnabled(),
                        false),
                booleanProperty(
                        COLLECT_PLAN_STATISTICS_FOR_ALL_QUERIES,
                        "Collect plan statistics for non-EXPLAIN queries",
                        optimizerConfig.isCollectPlanStatisticsForAllQueries(),
                        false),
                new PropertyMetadata<>(
                        MAX_DRIVERS_PER_TASK,
                        "Maximum number of drivers per task",
                        INTEGER,
                        Integer.class,
                        null,
                        false,
                        value -> min(taskManagerConfig.getMaxDriversPerTask(), validateNullablePositiveIntegerValue(value, MAX_DRIVERS_PER_TASK)),
                        object -> object),
                booleanProperty(
                        IGNORE_STATS_CALCULATOR_FAILURES,
                        "Ignore statistics calculator failures",
                        optimizerConfig.isIgnoreStatsCalculatorFailures(),
                        false),
                booleanProperty(
                        DEFAULT_FILTER_FACTOR_ENABLED,
                        "use a default filter factor for unknown filters in a filter node",
                        optimizerConfig.isDefaultFilterFactorEnabled(),
                        false),
                new PropertyMetadata<>(
                        FILTER_CONJUNCTION_INDEPENDENCE_FACTOR,
                        "Scales the strength of independence assumption for selectivity estimates of the conjunction of multiple filters",
                        DOUBLE,
                        Double.class,
                        optimizerConfig.getFilterConjunctionIndependenceFactor(),
                        false,
                        value -> validateDoubleRange(value, FILTER_CONJUNCTION_INDEPENDENCE_FACTOR, 0.0, 1.0),
                        value -> value),
                booleanProperty(
                        NON_ESTIMATABLE_PREDICATE_APPROXIMATION_ENABLED,
                        "Approximate the cost of filters which cannot be accurately estimated even with complete statistics",
                        optimizerConfig.isNonEstimatablePredicateApproximationEnabled(),
                        false),
                booleanProperty(
                        SKIP_REDUNDANT_SORT,
                        "Skip redundant sort operations",
                        optimizerConfig.isSkipRedundantSort(),
                        false),
                booleanProperty(
                        ALLOW_PUSHDOWN_INTO_CONNECTORS,
                        "Allow pushdown into connectors",
                        // This is a diagnostic property
                        true,
                        true),
                booleanProperty(
                        COMPLEX_EXPRESSION_PUSHDOWN,
                        "Allow complex expression pushdown into connectors",
                        optimizerConfig.isComplexExpressionPushdownEnabled(),
                        true),
                booleanProperty(
                        PREDICATE_PUSHDOWN_USE_TABLE_PROPERTIES,
                        "Use table properties in predicate pushdown",
                        optimizerConfig.isPredicatePushdownUseTableProperties(),
                        false),
                booleanProperty(
                        LATE_MATERIALIZATION,
                        "Experimental: Use late materialization (including WorkProcessor pipelines)",
                        featuresConfig.isLateMaterializationEnabled(),
                        false),
                booleanProperty(
                        ENABLE_DYNAMIC_FILTERING,
                        "Enable dynamic filtering",
                        dynamicFilterConfig.isEnableDynamicFiltering(),
                        false),
                booleanProperty(
                        ENABLE_COORDINATOR_DYNAMIC_FILTERS_DISTRIBUTION,
                        "Enable distribution of dynamic filters from coordinator to all workers",
                        dynamicFilterConfig.isEnableCoordinatorDynamicFiltersDistribution(),
                        false),
                booleanProperty(
                        ENABLE_LARGE_DYNAMIC_FILTERS,
                        "Enable collection of large dynamic filters",
                        dynamicFilterConfig.isEnableLargeDynamicFilters(),
                        false),
                dataSizeProperty(
                        QUERY_MAX_MEMORY_PER_NODE,
                        "Maximum amount of memory a query can use per node",
                        nodeMemoryConfig.getMaxQueryMemoryPerNode(),
                        true),
                booleanProperty(
                        IGNORE_DOWNSTREAM_PREFERENCES,
                        "Ignore Parent's PreferredProperties in AddExchange optimizer",
                        optimizerConfig.isIgnoreDownstreamPreferences(),
                        false),
                booleanProperty(
                        FILTERING_SEMI_JOIN_TO_INNER,
                        "Rewrite semi join in filtering context to inner join",
                        optimizerConfig.isRewriteFilteringSemiJoinToInnerJoin(),
                        false),
                booleanProperty(
                        OPTIMIZE_DUPLICATE_INSENSITIVE_JOINS,
                        "Optimize duplicate insensitive joins",
                        optimizerConfig.isOptimizeDuplicateInsensitiveJoins(),
                        false),
                integerProperty(
                        REQUIRED_WORKERS_COUNT,
                        "Minimum number of active workers that must be available before the query will start",
                        queryManagerConfig.getRequiredWorkers(),
                        false),
                durationProperty(
                        REQUIRED_WORKERS_MAX_WAIT_TIME,
                        "Maximum time to wait for minimum number of workers before the query is failed",
                        queryManagerConfig.getRequiredWorkersMaxWait(),
                        false),
                integerProperty(
                        COST_ESTIMATION_WORKER_COUNT,
                        "Set the estimate count of workers while planning",
                        null,
                        true),
                booleanProperty(
                        OMIT_DATETIME_TYPE_PRECISION,
                        "Omit precision when rendering datetime type names with default precision",
                        featuresConfig.isOmitDateTimeTypePrecision(),
                        false),
                booleanProperty(
                        USE_LEGACY_WINDOW_FILTER_PUSHDOWN,
                        "Use legacy window filter pushdown optimizer",
                        optimizerConfig.isUseLegacyWindowFilterPushdown(),
                        false),
                new PropertyMetadata<>(
                        MAX_UNACKNOWLEDGED_SPLITS_PER_TASK,
                        "Maximum number of leaf splits awaiting delivery to a given task",
                        INTEGER,
                        Integer.class,
                        nodeSchedulerConfig.getMaxUnacknowledgedSplitsPerTask(),
                        false,
                        value -> validateIntegerValue(value, MAX_UNACKNOWLEDGED_SPLITS_PER_TASK, 1, false),
                        object -> object),
                booleanProperty(
                        MERGE_PROJECT_WITH_VALUES,
                        "Inline project expressions into values",
                        optimizerConfig.isMergeProjectWithValues(),
                        false),
                stringProperty(
                        TIME_ZONE_ID,
                        "Time Zone Id for the current session",
                        null,
                        value -> {
                            if (value != null) {
                                getTimeZoneKey(value);
                            }
                        },
                        true),
                booleanProperty(
                        LEGACY_CATALOG_ROLES,
                        "Enable legacy role management syntax that assumed all roles are catalog scoped",
                        featuresConfig.isLegacyCatalogRoles(),
                        true),
                booleanProperty(
                        INCREMENTAL_HASH_ARRAY_LOAD_FACTOR_ENABLED,
                        "Use smaller load factor for small hash arrays in order to improve performance",
                        featuresConfig.isIncrementalHashArrayLoadFactorEnabled(),
                        false),
                dataSizeProperty(
                        MAX_PARTIAL_TOP_N_MEMORY,
                        "Max memory size for partial Top N aggregations. This can be turned off by setting it with '0'.",
                        taskManagerConfig.getMaxPartialTopNMemory(),
                        false),
                enumProperty(
                        RETRY_POLICY,
                        "Retry policy",
                        RetryPolicy.class,
                        queryManagerConfig.getRetryPolicy(),
                        true),
                integerProperty(
                        QUERY_RETRY_ATTEMPTS,
                        "Maximum number of query retry attempts",
                        queryManagerConfig.getQueryRetryAttempts(),
                        false),
                integerProperty(
                        TASK_RETRY_ATTEMPTS_PER_TASK,
                        "Maximum number of task retry attempts per single task",
                        queryManagerConfig.getTaskRetryAttemptsPerTask(),
                        value -> {
                            if (value < 0 || value > MAX_TASK_RETRY_ATTEMPTS) {
                                throw new TrinoException(
                                        INVALID_SESSION_PROPERTY,
                                        format("%s must be greater than or equal to 0 and not not greater than %s", TASK_RETRY_ATTEMPTS_PER_TASK, MAX_TASK_RETRY_ATTEMPTS));
                            }
                        },
                        false),
                integerProperty(
                        MAX_TASKS_WAITING_FOR_EXECUTION_PER_QUERY,
                        "Maximum number of tasks waiting to be scheduled per query. Split enumeration is paused by the scheduler when this threshold is crossed",
                        queryManagerConfig.getMaxTasksWaitingForExecutionPerQuery(),
                        false),
                integerProperty(
                        MAX_TASKS_WAITING_FOR_NODE_PER_STAGE,
                        "Maximum possible number of tasks waiting for node allocation per stage before scheduling of new tasks for stage is paused",
                        queryManagerConfig.getMaxTasksWaitingForNodePerStage(),
                        false),
                durationProperty(
                        RETRY_INITIAL_DELAY,
                        "Initial delay before initiating a retry attempt. Delay increases exponentially for each subsequent attempt up to 'retry_max_delay'",
                        queryManagerConfig.getRetryInitialDelay(),
                        false),
                durationProperty(
                        RETRY_MAX_DELAY,
                        "Maximum delay before initiating a retry attempt. Delay increases exponentially for each subsequent attempt starting from 'retry_initial_delay'",
                        queryManagerConfig.getRetryMaxDelay(),
                        false),
                doubleProperty(
                        RETRY_DELAY_SCALE_FACTOR,
                        "Maximum delay before initiating a retry attempt. Delay increases exponentially for each subsequent attempt starting from 'retry_initial_delay'",
                        queryManagerConfig.getRetryDelayScaleFactor(),
                        value -> {
                            if (value < 1.0) {
                                throw new TrinoException(
                                        INVALID_SESSION_PROPERTY,
                                        format("%s must be greater than or equal to 1.0", RETRY_DELAY_SCALE_FACTOR));
                            }
                        },
                        false),
                booleanProperty(
                        LEGACY_MATERIALIZED_VIEW_GRACE_PERIOD,
                        "Enable legacy handling of stale materialized views",
                        featuresConfig.isLegacyMaterializedViewGracePeriod(),
                        false),
                booleanProperty(
                        HIDE_INACCESSIBLE_COLUMNS,
                        "When enabled non-accessible columns are silently filtered from results from SELECT * statements",
                        featuresConfig.isHideInaccessibleColumns(),
                        value -> validateHideInaccessibleColumns(value, featuresConfig.isHideInaccessibleColumns()),
                        false),
                integerProperty(
                        FAULT_TOLERANT_EXECUTION_ARBITRARY_DISTRIBUTION_COMPUTE_TASK_TARGET_SIZE_GROWTH_PERIOD,
                        "The number of tasks we create for given non-writer stage of arbitrary distribution before we increase task size",
                        queryManagerConfig.getFaultTolerantExecutionArbitraryDistributionComputeTaskTargetSizeGrowthPeriod(),
                        true),
                doubleProperty(
                        FAULT_TOLERANT_EXECUTION_ARBITRARY_DISTRIBUTION_COMPUTE_TASK_TARGET_SIZE_GROWTH_FACTOR,
                        "Growth factor for adaptive sizing of non-writer tasks of arbitrary distribution for fault-tolerant execution",
                        queryManagerConfig.getFaultTolerantExecutionArbitraryDistributionComputeTaskTargetSizeGrowthFactor(),
                        true),
                dataSizeProperty(
                        FAULT_TOLERANT_EXECUTION_ARBITRARY_DISTRIBUTION_COMPUTE_TASK_TARGET_SIZE_MIN,
                        "Initial/min target input size for non-writer tasks of arbitrary distribution of fault-tolerant execution",
                        queryManagerConfig.getFaultTolerantExecutionArbitraryDistributionComputeTaskTargetSizeMin(),
                        true),
                dataSizeProperty(
                        FAULT_TOLERANT_EXECUTION_ARBITRARY_DISTRIBUTION_COMPUTE_TASK_TARGET_SIZE_MAX,
                        "Max target input size for non-writer task of arbitrary distribution of fault-tolerant execution",
                        queryManagerConfig.getFaultTolerantExecutionArbitraryDistributionComputeTaskTargetSizeMax(),
                        true),
                integerProperty(
                        FAULT_TOLERANT_EXECUTION_ARBITRARY_DISTRIBUTION_WRITE_TASK_TARGET_SIZE_GROWTH_PERIOD,
                        "The number of tasks we create for given writer stage of arbitrary distribution before we increase task size",
                        queryManagerConfig.getFaultTolerantExecutionArbitraryDistributionWriteTaskTargetSizeGrowthPeriod(),
                        true),
                doubleProperty(
                        FAULT_TOLERANT_EXECUTION_ARBITRARY_DISTRIBUTION_WRITE_TASK_TARGET_SIZE_GROWTH_FACTOR,
                        "Growth factor for adaptive sizing of writer tasks of arbitrary distribution for fault-tolerant execution",
                        queryManagerConfig.getFaultTolerantExecutionArbitraryDistributionWriteTaskTargetSizeGrowthFactor(),
                        true),
                dataSizeProperty(
                        FAULT_TOLERANT_EXECUTION_ARBITRARY_DISTRIBUTION_WRITE_TASK_TARGET_SIZE_MIN,
                        "Initial/min target input size for writer tasks of arbitrary distribution of fault-tolerant execution",
                        queryManagerConfig.getFaultTolerantExecutionArbitraryDistributionWriteTaskTargetSizeMin(),
                        true),
                dataSizeProperty(
                        FAULT_TOLERANT_EXECUTION_ARBITRARY_DISTRIBUTION_WRITE_TASK_TARGET_SIZE_MAX,
                        "Max target input size for writer tasks of arbitrary distribution of fault-tolerant execution",
                        queryManagerConfig.getFaultTolerantExecutionArbitraryDistributionWriteTaskTargetSizeMax(),
                        true),
                dataSizeProperty(
                        FAULT_TOLERANT_EXECUTION_HASH_DISTRIBUTION_COMPUTE_TASK_TARGET_SIZE,
                        "Target input size for non-writer tasks of hash distribution of fault-tolerant execution",
                        queryManagerConfig.getFaultTolerantExecutionHashDistributionComputeTaskTargetSize(),
                        true),
                dataSizeProperty(
                        FAULT_TOLERANT_EXECUTION_HASH_DISTRIBUTION_WRITE_TASK_TARGET_SIZE,
                        "Target input size of writer tasks of hash distribution of fault-tolerant execution",
                        queryManagerConfig.getFaultTolerantExecutionHashDistributionWriteTaskTargetSize(),
                        true),
                integerProperty(
                        FAULT_TOLERANT_EXECUTION_HASH_DISTRIBUTION_WRITE_TASK_TARGET_MAX_COUNT,
                        "Soft upper bound on number of writer tasks in a stage of hash distribution of fault-tolerant execution",
                        queryManagerConfig.getFaultTolerantExecutionHashDistributionWriteTaskTargetMaxCount(),
                        true),
                dataSizeProperty(
                        FAULT_TOLERANT_EXECUTION_STANDARD_SPLIT_SIZE,
                        "Standard split size for a single fault tolerant task (split weight aware)",
                        queryManagerConfig.getFaultTolerantExecutionStandardSplitSize(),
                        false),
                integerProperty(
                        FAULT_TOLERANT_EXECUTION_MAX_TASK_SPLIT_COUNT,
                        "Maximal number of splits for a single fault tolerant task (count based)",
                        queryManagerConfig.getFaultTolerantExecutionMaxTaskSplitCount(),
                        false),
                dataSizeProperty(
                        FAULT_TOLERANT_EXECUTION_COORDINATOR_TASK_MEMORY,
                        "Estimated amount of memory a single coordinator task will use when task level retries are used; value is used when allocating nodes for tasks execution",
                        memoryManagerConfig.getFaultTolerantExecutionCoordinatorTaskMemory(),
                        false),
                dataSizeProperty(
                        FAULT_TOLERANT_EXECUTION_TASK_MEMORY,
                        "Estimated amount of memory a single task will use when task level retries are used; value is used when allocating nodes for tasks execution",
                        memoryManagerConfig.getFaultTolerantExecutionTaskMemory(),
                        false),
                doubleProperty(
                        FAULT_TOLERANT_EXECUTION_TASK_MEMORY_GROWTH_FACTOR,
                        "Factor by which estimated task memory is increased if task execution runs out of memory; value is used allocating nodes for tasks execution",
                        memoryManagerConfig.getFaultTolerantExecutionTaskMemoryGrowthFactor(),
                        false),
                doubleProperty(
                        FAULT_TOLERANT_EXECUTION_TASK_MEMORY_ESTIMATION_QUANTILE,
                        "What quantile of memory usage of completed tasks to look at when estimating memory usage for upcoming tasks",
                        memoryManagerConfig.getFaultTolerantExecutionTaskMemoryEstimationQuantile(),
                        value -> validateDoubleRange(value, FAULT_TOLERANT_EXECUTION_TASK_MEMORY_ESTIMATION_QUANTILE, 0.0, 1.0),
                        false),
                integerProperty(
                        FAULT_TOLERANT_EXECUTION_MAX_PARTITION_COUNT,
                        "Maximum number of partitions for distributed joins and aggregations executed with fault tolerant execution enabled",
                        queryManagerConfig.getFaultTolerantExecutionMaxPartitionCount(),
                        value -> validateIntegerValue(value, FAULT_TOLERANT_EXECUTION_MAX_PARTITION_COUNT, 1, FAULT_TOLERANT_EXECUTION_MAX_PARTITION_COUNT_LIMIT, false),
                        false),
                integerProperty(
                        FAULT_TOLERANT_EXECUTION_MIN_PARTITION_COUNT,
                        "Minimum number of partitions for distributed joins and aggregations executed with fault tolerant execution enabled",
                        queryManagerConfig.getFaultTolerantExecutionMinPartitionCount(),
                        value -> validateIntegerValue(value, FAULT_TOLERANT_EXECUTION_MIN_PARTITION_COUNT, 1, FAULT_TOLERANT_EXECUTION_MAX_PARTITION_COUNT_LIMIT, false),
                        false),
                integerProperty(
                        FAULT_TOLERANT_EXECUTION_MIN_PARTITION_COUNT_FOR_WRITE,
                        "Minimum number of partitions for distributed joins and aggregations in write queries executed with fault tolerant execution enabled",
                        queryManagerConfig.getFaultTolerantExecutionMinPartitionCountForWrite(),
                        value -> validateIntegerValue(value, FAULT_TOLERANT_EXECUTION_MIN_PARTITION_COUNT_FOR_WRITE, 1, FAULT_TOLERANT_EXECUTION_MAX_PARTITION_COUNT_LIMIT, false),
                        false),
                booleanProperty(
                        ADAPTIVE_PARTIAL_AGGREGATION_ENABLED,
                        "When enabled, partial aggregation might be adaptively turned off when it does not provide any performance gain",
                        optimizerConfig.isAdaptivePartialAggregationEnabled(),
                        false),
                doubleProperty(
                        ADAPTIVE_PARTIAL_AGGREGATION_UNIQUE_ROWS_RATIO_THRESHOLD,
                        "Ratio between aggregation output and input rows above which partial aggregation might be adaptively turned off",
                        optimizerConfig.getAdaptivePartialAggregationUniqueRowsRatioThreshold(),
                        false),
                booleanProperty(
                        REMOTE_TASK_ADAPTIVE_UPDATE_REQUEST_SIZE_ENABLED,
                        "Experimental: Enable adaptive adjustment for size of remote task update request",
                        queryManagerConfig.isEnabledAdaptiveTaskRequestSize(),
                        false),
                dataSizeProperty(
                        REMOTE_TASK_MAX_REQUEST_SIZE,
                        "Experimental: Max size of remote task update request",
                        queryManagerConfig.getMaxRemoteTaskRequestSize(),
                        false),
                dataSizeProperty(
                        REMOTE_TASK_REQUEST_SIZE_HEADROOM,
                        "Experimental: Headroom for size of remote task update request",
                        queryManagerConfig.getRemoteTaskRequestSizeHeadroom(),
                        false),
                integerProperty(
                        REMOTE_TASK_GUARANTEED_SPLITS_PER_REQUEST,
                        "Guaranteed splits per remote task request",
                        queryManagerConfig.getRemoteTaskGuaranteedSplitPerTask(),
                        false),
                longProperty(
                        JOIN_PARTITIONED_BUILD_MIN_ROW_COUNT,
                        "Minimum number of join build side rows required to use partitioned join lookup",
                        optimizerConfig.getJoinPartitionedBuildMinRowCount(),
                        value -> validateNonNegativeLongValue(value, JOIN_PARTITIONED_BUILD_MIN_ROW_COUNT),
                        false),
                dataSizeProperty(
                        MIN_INPUT_SIZE_PER_TASK,
                        "Minimum input data size required per task. This will help optimizer determine hash partition count for joins and aggregations",
                        optimizerConfig.getMinInputSizePerTask(),
                        false),
                longProperty(
                        MIN_INPUT_ROWS_PER_TASK,
                        "Minimum input rows required per task. This will help optimizer determine hash partition count for joins and aggregations",
                        optimizerConfig.getMinInputRowsPerTask(),
                        value -> validateNonNegativeLongValue(value, MIN_INPUT_ROWS_PER_TASK),
                        false),
                booleanProperty(
                        USE_EXACT_PARTITIONING,
                        "When enabled this forces data repartitioning unless the partitioning of upstream stage matches exactly what downstream stage expects",
                        optimizerConfig.isUseExactPartitioning(),
                        false),
                booleanProperty(
                        USE_COST_BASED_PARTITIONING,
                        "When enabled the cost based optimizer is used to determine if repartitioning the output of an already partitioned stage is necessary",
                        optimizerConfig.isUseCostBasedPartitioning(),
                        false),
                booleanProperty(
                        FORCE_SPILLING_JOIN,
                        "Force the usage of spliing join operator in favor of the non-spilling one, even if spill is not enabled",
                        featuresConfig.isForceSpillingJoin(),
                        false),
                booleanProperty(
                        FAULT_TOLERANT_EXECUTION_FORCE_PREFERRED_WRITE_PARTITIONING_ENABLED,
                        "Force preferred write partitioning for fault tolerant execution",
                        queryManagerConfig.isFaultTolerantExecutionForcePreferredWritePartitioningEnabled(),
                        true),
                integerProperty(PAGE_PARTITIONING_BUFFER_POOL_SIZE,
                        "Maximum number of free buffers in the per task partitioned page buffer pool. Setting this to zero effectively disables the pool",
                        taskManagerConfig.getPagePartitioningBufferPoolSize(),
                        true));
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static String getExecutionPolicy(Session session)
    {
        return session.getSystemProperty(EXECUTION_POLICY, String.class);
    }

    public static boolean isOptimizeHashGenerationEnabled(Session session)
    {
        return session.getSystemProperty(OPTIMIZE_HASH_GENERATION, Boolean.class);
    }

    public static JoinDistributionType getJoinDistributionType(Session session)
    {
        return session.getSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.class);
    }

    public static DataSize getJoinMaxBroadcastTableSize(Session session)
    {
        return session.getSystemProperty(JOIN_MAX_BROADCAST_TABLE_SIZE, DataSize.class);
    }

    public static double getJoinMultiClauseIndependenceFactor(Session session)
    {
        return session.getSystemProperty(JOIN_MULTI_CLAUSE_INDEPENDENCE_FACTOR, Double.class);
    }

    public static boolean isDeterminePartitionCountForWriteEnabled(Session session)
    {
        return session.getSystemProperty(DETERMINE_PARTITION_COUNT_FOR_WRITE_ENABLED, Boolean.class);
    }

    public static int getMaxHashPartitionCount(Session session)
    {
        return session.getSystemProperty(MAX_HASH_PARTITION_COUNT, Integer.class);
    }

    public static int getMinHashPartitionCount(Session session)
    {
        return session.getSystemProperty(MIN_HASH_PARTITION_COUNT, Integer.class);
    }

    public static int getMinHashPartitionCountForWrite(Session session)
    {
        return session.getSystemProperty(MIN_HASH_PARTITION_COUNT_FOR_WRITE, Integer.class);
    }

    public static boolean preferStreamingOperators(Session session)
    {
        return session.getSystemProperty(PREFER_STREAMING_OPERATORS, Boolean.class);
    }

    public static int getTaskWriterCount(Session session)
    {
        return session.getSystemProperty(TASK_WRITER_COUNT, Integer.class);
    }

    public static int getTaskPartitionedWriterCount(Session session)
    {
        return session.getSystemProperty(TASK_PARTITIONED_WRITER_COUNT, Integer.class);
    }

    public static boolean isRedistributeWrites(Session session)
    {
        return session.getSystemProperty(REDISTRIBUTE_WRITES, Boolean.class);
    }

    public static boolean isUsePreferredWritePartitioning(Session session)
    {
        return session.getSystemProperty(USE_PREFERRED_WRITE_PARTITIONING, Boolean.class);
    }

    public static boolean isScaleWriters(Session session)
    {
        return session.getSystemProperty(SCALE_WRITERS, Boolean.class);
    }

    public static boolean isTaskScaleWritersEnabled(Session session)
    {
        return session.getSystemProperty(TASK_SCALE_WRITERS_ENABLED, Boolean.class);
    }

    public static int getTaskScaleWritersMaxWriterCount(Session session)
    {
        return session.getSystemProperty(TASK_SCALE_WRITERS_MAX_WRITER_COUNT, Integer.class);
    }

    public static int getMaxWriterTaskCount(Session session)
    {
        return session.getSystemProperty(MAX_WRITER_TASKS_COUNT, Integer.class);
    }

    public static DataSize getWriterMinSize(Session session)
    {
        return session.getSystemProperty(WRITER_MIN_SIZE, DataSize.class);
    }

    public static boolean isPushTableWriteThroughUnion(Session session)
    {
        return session.getSystemProperty(PUSH_TABLE_WRITE_THROUGH_UNION, Boolean.class);
    }

    public static int getTaskConcurrency(Session session)
    {
        return session.getSystemProperty(TASK_CONCURRENCY, Integer.class);
    }

    public static boolean isShareIndexLoading(Session session)
    {
        return session.getSystemProperty(TASK_SHARE_INDEX_LOADING, Boolean.class);
    }

    public static boolean isDictionaryAggregationEnabled(Session session)
    {
        return session.getSystemProperty(DICTIONARY_AGGREGATION, Boolean.class);
    }

    public static boolean isOptimizeMetadataQueries(Session session)
    {
        return session.getSystemProperty(OPTIMIZE_METADATA_QUERIES, Boolean.class);
    }

    public static DataSize getQueryMaxMemory(Session session)
    {
        return session.getSystemProperty(QUERY_MAX_MEMORY, DataSize.class);
    }

    public static DataSize getQueryMaxTotalMemory(Session session)
    {
        return session.getSystemProperty(QUERY_MAX_TOTAL_MEMORY, DataSize.class);
    }

    public static Duration getQueryMaxRunTime(Session session)
    {
        return session.getSystemProperty(QUERY_MAX_RUN_TIME, Duration.class);
    }

    public static Duration getQueryMaxExecutionTime(Session session)
    {
        return session.getSystemProperty(QUERY_MAX_EXECUTION_TIME, Duration.class);
    }

    public static Duration getQueryMaxPlanningTime(Session session)
    {
        return session.getSystemProperty(QUERY_MAX_PLANNING_TIME, Duration.class);
    }

    public static boolean resourceOvercommit(Session session)
    {
        return session.getSystemProperty(RESOURCE_OVERCOMMIT, Boolean.class);
    }

    public static int getQueryMaxStageCount(Session session)
    {
        return session.getSystemProperty(QUERY_MAX_STAGE_COUNT, Integer.class);
    }

    public static boolean isUseTableScanNodePartitioning(Session session)
    {
        return session.getSystemProperty(USE_TABLE_SCAN_NODE_PARTITIONING, Boolean.class);
    }

    public static double getTableScanNodePartitioningMinBucketToTaskRatio(Session session)
    {
        return session.getSystemProperty(TABLE_SCAN_NODE_PARTITIONING_MIN_BUCKET_TO_TASK_RATIO, Double.class);
    }

    public static JoinReorderingStrategy getJoinReorderingStrategy(Session session)
    {
        return session.getSystemProperty(JOIN_REORDERING_STRATEGY, JoinReorderingStrategy.class);
    }

    public static int getMaxReorderedJoins(Session session)
    {
        return session.getSystemProperty(MAX_REORDERED_JOINS, Integer.class);
    }

    public static boolean isColocatedJoinEnabled(Session session)
    {
        return session.getSystemProperty(COLOCATED_JOIN, Boolean.class);
    }

    public static boolean isSpatialJoinEnabled(Session session)
    {
        return session.getSystemProperty(SPATIAL_JOIN, Boolean.class);
    }

    public static Optional<String> getSpatialPartitioningTableName(Session session)
    {
        return Optional.ofNullable(session.getSystemProperty(SPATIAL_PARTITIONING_TABLE_NAME, String.class));
    }

    public static int getInitialSplitsPerNode(Session session)
    {
        return session.getSystemProperty(INITIAL_SPLITS_PER_NODE, Integer.class);
    }

    public static int getQueryPriority(Session session)
    {
        Integer priority = session.getSystemProperty(QUERY_PRIORITY, Integer.class);
        checkArgument(priority > 0, "Query priority must be positive");
        return priority;
    }

    public static Duration getSplitConcurrencyAdjustmentInterval(Session session)
    {
        return session.getSystemProperty(SPLIT_CONCURRENCY_ADJUSTMENT_INTERVAL, Duration.class);
    }

    public static Duration getQueryMaxCpuTime(Session session)
    {
        return session.getSystemProperty(QUERY_MAX_CPU_TIME, Duration.class);
    }

    public static Optional<DataSize> getQueryMaxScanPhysicalBytes(Session session)
    {
        return Optional.ofNullable(session.getSystemProperty(QUERY_MAX_SCAN_PHYSICAL_BYTES, DataSize.class));
    }

    public static boolean isSpillEnabled(Session session)
    {
        return session.getSystemProperty(SPILL_ENABLED, Boolean.class);
    }

    public static DataSize getAggregationOperatorUnspillMemoryLimit(Session session)
    {
        DataSize memoryLimitForMerge = session.getSystemProperty(AGGREGATION_OPERATOR_UNSPILL_MEMORY_LIMIT, DataSize.class);
        checkArgument(memoryLimitForMerge.toBytes() >= 0, "%s must be positive", AGGREGATION_OPERATOR_UNSPILL_MEMORY_LIMIT);
        return memoryLimitForMerge;
    }

    public static boolean isOptimizeDistinctAggregationEnabled(Session session)
    {
        return session.getSystemProperty(OPTIMIZE_DISTINCT_AGGREGATIONS, Boolean.class);
    }

    public static Duration getOptimizerTimeout(Session session)
    {
        return session.getSystemProperty(ITERATIVE_OPTIMIZER_TIMEOUT, Duration.class);
    }

    public static boolean isEnableForcedExchangeBelowGroupId(Session session)
    {
        return session.getSystemProperty(ENABLE_FORCED_EXCHANGE_BELOW_GROUP_ID, Boolean.class);
    }

    public static boolean isExchangeCompressionEnabled(Session session)
    {
        return session.getSystemProperty(EXCHANGE_COMPRESSION, Boolean.class);
    }

    public static boolean isEnableIntermediateAggregations(Session session)
    {
        return session.getSystemProperty(ENABLE_INTERMEDIATE_AGGREGATIONS, Boolean.class);
    }

    public static boolean isPushAggregationThroughOuterJoin(Session session)
    {
        return session.getSystemProperty(PUSH_AGGREGATION_THROUGH_OUTER_JOIN, Boolean.class);
    }

    public static boolean isPushPartialAggregationThroughJoin(Session session)
    {
        return session.getSystemProperty(PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN, Boolean.class);
    }

    public static boolean isPreAggregateCaseAggregationsEnabled(Session session)
    {
        return session.getSystemProperty(PRE_AGGREGATE_CASE_AGGREGATIONS_ENABLED, Boolean.class);
    }

    public static boolean isParseDecimalLiteralsAsDouble(Session session)
    {
        return session.getSystemProperty(PARSE_DECIMAL_LITERALS_AS_DOUBLE, Boolean.class);
    }

    public static boolean isForceSingleNodeOutput(Session session)
    {
        return session.getSystemProperty(FORCE_SINGLE_NODE_OUTPUT, Boolean.class);
    }

    public static DataSize getFilterAndProjectMinOutputPageSize(Session session)
    {
        return session.getSystemProperty(FILTER_AND_PROJECT_MIN_OUTPUT_PAGE_SIZE, DataSize.class);
    }

    public static int getFilterAndProjectMinOutputPageRowCount(Session session)
    {
        return session.getSystemProperty(FILTER_AND_PROJECT_MIN_OUTPUT_PAGE_ROW_COUNT, Integer.class);
    }

    public static MarkDistinctStrategy markDistinctStrategy(Session session)
    {
        MarkDistinctStrategy markDistinctStrategy = session.getSystemProperty(MARK_DISTINCT_STRATEGY, MarkDistinctStrategy.class);
        if (markDistinctStrategy != null) {
            // mark_distinct_strategy is set, so it takes precedence over use_mark_distinct
            return markDistinctStrategy;
        }

        Boolean useMarkDistinct = session.getSystemProperty(USE_MARK_DISTINCT, Boolean.class);
        if (useMarkDistinct == null) {
            // both mark_distinct_strategy and use_mark_distinct have default null values, use AUTOMATIC
            return MarkDistinctStrategy.AUTOMATIC;
        }
        // use_mark_distinct is set but mark_distinct_strategy is not, map use_mark_distinct to mark_distinct_strategy
        return useMarkDistinct ? MarkDistinctStrategy.AUTOMATIC : MarkDistinctStrategy.NONE;
    }

    public static boolean preferPartialAggregation(Session session)
    {
        return session.getSystemProperty(PREFER_PARTIAL_AGGREGATION, Boolean.class);
    }

    public static boolean isOptimizeTopNRanking(Session session)
    {
        return session.getSystemProperty(OPTIMIZE_TOP_N_RANKING, Boolean.class);
    }

    public static boolean isDistributedSortEnabled(Session session)
    {
        if (getRetryPolicy(session) != RetryPolicy.NONE) {
            // distributed sort is not supported with failure recovery capabilities enabled
            return false;
        }

        return session.getSystemProperty(DISTRIBUTED_SORT, Boolean.class);
    }

    public static boolean isUsePartialTopN(Session session)
    {
        return session.getSystemProperty(USE_PARTIAL_TOPN, Boolean.class);
    }

    public static boolean isUsePartialDistinctLimit(Session session)
    {
        return session.getSystemProperty(USE_PARTIAL_DISTINCT_LIMIT, Boolean.class);
    }

    public static int getMaxRecursionDepth(Session session)
    {
        return session.getSystemProperty(MAX_RECURSION_DEPTH, Integer.class);
    }

    public static int getMaxGroupingSets(Session session)
    {
        return session.getSystemProperty(MAX_GROUPING_SETS, Integer.class);
    }

    private static void validateHideInaccessibleColumns(boolean value, boolean defaultValue)
    {
        if (defaultValue && !value) {
            throw new TrinoException(INVALID_SESSION_PROPERTY, format("%s cannot be disabled with session property when it was enabled with configuration", HIDE_INACCESSIBLE_COLUMNS));
        }
    }

    public static OptionalInt getMaxDriversPerTask(Session session)
    {
        Integer value = session.getSystemProperty(MAX_DRIVERS_PER_TASK, Integer.class);
        if (value == null) {
            return OptionalInt.empty();
        }
        return OptionalInt.of(value);
    }

    private static void validateValueIsPowerOfTwo(Object value, String property)
    {
        int intValue = (int) value;
        if (Integer.bitCount(intValue) != 1) {
            throw new TrinoException(
                    INVALID_SESSION_PROPERTY,
                    format("%s must be a power of 2: %s", property, intValue));
        }
    }

    private static Integer validateNullablePositiveIntegerValue(Object value, String property)
    {
        return validateIntegerValue(value, property, 1, true);
    }

    private static Integer validateIntegerValue(Object value, String property, int lowerBoundIncluded, boolean allowNull)
    {
        return validateIntegerValue(value, property, lowerBoundIncluded, Integer.MAX_VALUE, allowNull);
    }

    private static Integer validateIntegerValue(Object value, String property, int lowerBoundIncluded, int upperBoundIncluded, boolean allowNull)
    {
        if (value == null && !allowNull) {
            throw new TrinoException(INVALID_SESSION_PROPERTY, format("%s must be non-null", property));
        }

        if (value == null) {
            return null;
        }

        int intValue = (int) value;
        if (intValue < lowerBoundIncluded) {
            throw new TrinoException(INVALID_SESSION_PROPERTY, format("%s must be equal or greater than %s", property, lowerBoundIncluded));
        }
        if (intValue > upperBoundIncluded) {
            throw new TrinoException(INVALID_SESSION_PROPERTY, format("%s must be equal or less than %s", property, upperBoundIncluded));
        }

        return intValue;
    }

    private static void validateNonNegativeLongValue(Long value, String property)
    {
        if (value < 0) {
            throw new TrinoException(INVALID_SESSION_PROPERTY, format("%s must be equal or greater than 0", property));
        }
    }

    private static double validateDoubleRange(Object value, String property, double lowerBoundIncluded, double upperBoundIncluded)
    {
        double doubleValue = (double) value;
        if (doubleValue < lowerBoundIncluded || doubleValue > upperBoundIncluded) {
            throw new TrinoException(
                    INVALID_SESSION_PROPERTY,
                    format("%s must be in the range [%.2f, %.2f]: %.2f", property, lowerBoundIncluded, upperBoundIncluded, doubleValue));
        }
        return doubleValue;
    }

    public static boolean isStatisticsCpuTimerEnabled(Session session)
    {
        return session.getSystemProperty(STATISTICS_CPU_TIMER_ENABLED, Boolean.class);
    }

    public static boolean isEnableStatsCalculator(Session session)
    {
        return session.getSystemProperty(ENABLE_STATS_CALCULATOR, Boolean.class);
    }

    public static boolean isStatisticsPrecalculationForPushdownEnabled(Session session)
    {
        return session.getSystemProperty(STATISTICS_PRECALCULATION_FOR_PUSHDOWN_ENABLED, Boolean.class);
    }

    public static boolean isCollectPlanStatisticsForAllQueries(Session session)
    {
        return session.getSystemProperty(COLLECT_PLAN_STATISTICS_FOR_ALL_QUERIES, Boolean.class);
    }

    public static boolean isIgnoreStatsCalculatorFailures(Session session)
    {
        return session.getSystemProperty(IGNORE_STATS_CALCULATOR_FAILURES, Boolean.class);
    }

    public static boolean isDefaultFilterFactorEnabled(Session session)
    {
        return session.getSystemProperty(DEFAULT_FILTER_FACTOR_ENABLED, Boolean.class);
    }

    public static double getFilterConjunctionIndependenceFactor(Session session)
    {
        return session.getSystemProperty(FILTER_CONJUNCTION_INDEPENDENCE_FACTOR, Double.class);
    }

    public static boolean isNonEstimatablePredicateApproximationEnabled(Session session)
    {
        return session.getSystemProperty(NON_ESTIMATABLE_PREDICATE_APPROXIMATION_ENABLED, Boolean.class);
    }

    public static boolean isSkipRedundantSort(Session session)
    {
        return session.getSystemProperty(SKIP_REDUNDANT_SORT, Boolean.class);
    }

    public static boolean isAllowPushdownIntoConnectors(Session session)
    {
        return session.getSystemProperty(ALLOW_PUSHDOWN_INTO_CONNECTORS, Boolean.class);
    }

    public static boolean isComplexExpressionPushdown(Session session)
    {
        return session.getSystemProperty(COMPLEX_EXPRESSION_PUSHDOWN, Boolean.class);
    }

    public static boolean isPredicatePushdownUseTableProperties(Session session)
    {
        return session.getSystemProperty(PREDICATE_PUSHDOWN_USE_TABLE_PROPERTIES, Boolean.class);
    }

    public static boolean isLateMaterializationEnabled(Session session)
    {
        return session.getSystemProperty(LATE_MATERIALIZATION, Boolean.class);
    }

    public static boolean isEnableDynamicFiltering(Session session)
    {
        return session.getSystemProperty(ENABLE_DYNAMIC_FILTERING, Boolean.class);
    }

    public static boolean isEnableCoordinatorDynamicFiltersDistribution(Session session)
    {
        return session.getSystemProperty(ENABLE_COORDINATOR_DYNAMIC_FILTERS_DISTRIBUTION, Boolean.class);
    }

    public static boolean isEnableLargeDynamicFilters(Session session)
    {
        return session.getSystemProperty(ENABLE_LARGE_DYNAMIC_FILTERS, Boolean.class);
    }

    public static DataSize getQueryMaxMemoryPerNode(Session session)
    {
        return session.getSystemProperty(QUERY_MAX_MEMORY_PER_NODE, DataSize.class);
    }

    public static boolean ignoreDownStreamPreferences(Session session)
    {
        return session.getSystemProperty(IGNORE_DOWNSTREAM_PREFERENCES, Boolean.class);
    }

    public static boolean isRewriteFilteringSemiJoinToInnerJoin(Session session)
    {
        return session.getSystemProperty(FILTERING_SEMI_JOIN_TO_INNER, Boolean.class);
    }

    public static boolean isOptimizeDuplicateInsensitiveJoins(Session session)
    {
        return session.getSystemProperty(OPTIMIZE_DUPLICATE_INSENSITIVE_JOINS, Boolean.class);
    }

    public static int getRequiredWorkers(Session session)
    {
        return session.getSystemProperty(REQUIRED_WORKERS_COUNT, Integer.class);
    }

    public static Duration getRequiredWorkersMaxWait(Session session)
    {
        return session.getSystemProperty(REQUIRED_WORKERS_MAX_WAIT_TIME, Duration.class);
    }

    public static Integer getCostEstimationWorkerCount(Session session)
    {
        return session.getSystemProperty(COST_ESTIMATION_WORKER_COUNT, Integer.class);
    }

    public static boolean isOmitDateTimeTypePrecision(Session session)
    {
        return session.getSystemProperty(OMIT_DATETIME_TYPE_PRECISION, Boolean.class);
    }

    public static boolean useLegacyWindowFilterPushdown(Session session)
    {
        return session.getSystemProperty(USE_LEGACY_WINDOW_FILTER_PUSHDOWN, Boolean.class);
    }

    public static int getMaxUnacknowledgedSplitsPerTask(Session session)
    {
        return session.getSystemProperty(MAX_UNACKNOWLEDGED_SPLITS_PER_TASK, Integer.class);
    }

    public static boolean isMergeProjectWithValues(Session session)
    {
        return session.getSystemProperty(MERGE_PROJECT_WITH_VALUES, Boolean.class);
    }

    public static Optional<String> getTimeZoneId(Session session)
    {
        return Optional.ofNullable(session.getSystemProperty(TIME_ZONE_ID, String.class));
    }

    public static boolean isLegacyCatalogRoles(Session session)
    {
        return session.getSystemProperty(LEGACY_CATALOG_ROLES, Boolean.class);
    }

    public static boolean isIncrementalHashArrayLoadFactorEnabled(Session session)
    {
        return session.getSystemProperty(INCREMENTAL_HASH_ARRAY_LOAD_FACTOR_ENABLED, Boolean.class);
    }

    public static DataSize getMaxPartialTopNMemory(Session session)
    {
        return session.getSystemProperty(MAX_PARTIAL_TOP_N_MEMORY, DataSize.class);
    }

    public static RetryPolicy getRetryPolicy(Session session)
    {
        return session.getSystemProperty(RETRY_POLICY, RetryPolicy.class);
    }

    public static int getQueryRetryAttempts(Session session)
    {
        return session.getSystemProperty(QUERY_RETRY_ATTEMPTS, Integer.class);
    }

    public static int getTaskRetryAttemptsPerTask(Session session)
    {
        return session.getSystemProperty(TASK_RETRY_ATTEMPTS_PER_TASK, Integer.class);
    }

    public static int getMaxTasksWaitingForExecutionPerQuery(Session session)
    {
        return session.getSystemProperty(MAX_TASKS_WAITING_FOR_EXECUTION_PER_QUERY, Integer.class);
    }

    public static int getMaxTasksWaitingForNodePerStage(Session session)
    {
        return session.getSystemProperty(MAX_TASKS_WAITING_FOR_NODE_PER_STAGE, Integer.class);
    }

    public static Duration getRetryInitialDelay(Session session)
    {
        return session.getSystemProperty(RETRY_INITIAL_DELAY, Duration.class);
    }

    public static Duration getRetryMaxDelay(Session session)
    {
        return session.getSystemProperty(RETRY_MAX_DELAY, Duration.class);
    }

    public static double getRetryDelayScaleFactor(Session session)
    {
        return session.getSystemProperty(RETRY_DELAY_SCALE_FACTOR, Double.class);
    }

    @Deprecated
    public static boolean isLegacyMaterializedViewGracePeriod(Session session)
    {
        return session.getSystemProperty(LEGACY_MATERIALIZED_VIEW_GRACE_PERIOD, Boolean.class);
    }

    public static boolean isHideInaccessibleColumns(Session session)
    {
        return session.getSystemProperty(HIDE_INACCESSIBLE_COLUMNS, Boolean.class);
    }

    public static int getFaultTolerantExecutionArbitraryDistributionComputeTaskTargetSizeGrowthPeriod(Session session)
    {
        return session.getSystemProperty(FAULT_TOLERANT_EXECUTION_ARBITRARY_DISTRIBUTION_COMPUTE_TASK_TARGET_SIZE_GROWTH_PERIOD, Integer.class);
    }

    public static double getFaultTolerantExecutionArbitraryDistributionComputeTaskTargetSizeGrowthFactor(Session session)
    {
        return session.getSystemProperty(FAULT_TOLERANT_EXECUTION_ARBITRARY_DISTRIBUTION_COMPUTE_TASK_TARGET_SIZE_GROWTH_FACTOR, Double.class);
    }

    public static DataSize getFaultTolerantExecutionArbitraryDistributionComputeTaskTargetSizeMin(Session session)
    {
        return session.getSystemProperty(FAULT_TOLERANT_EXECUTION_ARBITRARY_DISTRIBUTION_COMPUTE_TASK_TARGET_SIZE_MIN, DataSize.class);
    }

    public static DataSize getFaultTolerantExecutionArbitraryDistributionComputeTaskTargetSizeMax(Session session)
    {
        return session.getSystemProperty(FAULT_TOLERANT_EXECUTION_ARBITRARY_DISTRIBUTION_COMPUTE_TASK_TARGET_SIZE_MAX, DataSize.class);
    }

    public static int getFaultTolerantExecutionArbitraryDistributionWriteTaskTargetSizeGrowthPeriod(Session session)
    {
        return session.getSystemProperty(FAULT_TOLERANT_EXECUTION_ARBITRARY_DISTRIBUTION_WRITE_TASK_TARGET_SIZE_GROWTH_PERIOD, Integer.class);
    }

    public static double getFaultTolerantExecutionArbitraryDistributionWriteTaskTargetSizeGrowthFactor(Session session)
    {
        return session.getSystemProperty(FAULT_TOLERANT_EXECUTION_ARBITRARY_DISTRIBUTION_WRITE_TASK_TARGET_SIZE_GROWTH_FACTOR, Double.class);
    }

    public static DataSize getFaultTolerantExecutionArbitraryDistributionWriteTaskTargetSizeMin(Session session)
    {
        return session.getSystemProperty(FAULT_TOLERANT_EXECUTION_ARBITRARY_DISTRIBUTION_WRITE_TASK_TARGET_SIZE_MIN, DataSize.class);
    }

    public static DataSize getFaultTolerantExecutionArbitraryDistributionWriteTaskTargetSizeMax(Session session)
    {
        return session.getSystemProperty(FAULT_TOLERANT_EXECUTION_ARBITRARY_DISTRIBUTION_WRITE_TASK_TARGET_SIZE_MAX, DataSize.class);
    }

    public static DataSize getFaultTolerantExecutionHashDistributionComputeTaskTargetSize(Session session)
    {
        return session.getSystemProperty(FAULT_TOLERANT_EXECUTION_HASH_DISTRIBUTION_COMPUTE_TASK_TARGET_SIZE, DataSize.class);
    }

    public static DataSize getFaultTolerantExecutionHashDistributionWriteTaskTargetSize(Session session)
    {
        return session.getSystemProperty(FAULT_TOLERANT_EXECUTION_HASH_DISTRIBUTION_WRITE_TASK_TARGET_SIZE, DataSize.class);
    }

    public static int getFaultTolerantExecutionHashDistributionWriteTaskTargetMaxCount(Session session)
    {
        return session.getSystemProperty(FAULT_TOLERANT_EXECUTION_HASH_DISTRIBUTION_WRITE_TASK_TARGET_MAX_COUNT, Integer.class);
    }

    public static DataSize getFaultTolerantExecutionStandardSplitSize(Session session)
    {
        return session.getSystemProperty(FAULT_TOLERANT_EXECUTION_STANDARD_SPLIT_SIZE, DataSize.class);
    }

    public static int getFaultTolerantExecutionMaxTaskSplitCount(Session session)
    {
        return session.getSystemProperty(FAULT_TOLERANT_EXECUTION_MAX_TASK_SPLIT_COUNT, Integer.class);
    }

    public static DataSize getFaultTolerantExecutionDefaultCoordinatorTaskMemory(Session session)
    {
        return session.getSystemProperty(FAULT_TOLERANT_EXECUTION_COORDINATOR_TASK_MEMORY, DataSize.class);
    }

    public static DataSize getFaultTolerantExecutionDefaultTaskMemory(Session session)
    {
        return session.getSystemProperty(FAULT_TOLERANT_EXECUTION_TASK_MEMORY, DataSize.class);
    }

    public static double getFaultTolerantExecutionTaskMemoryGrowthFactor(Session session)
    {
        return session.getSystemProperty(FAULT_TOLERANT_EXECUTION_TASK_MEMORY_GROWTH_FACTOR, Double.class);
    }

    public static double getFaultTolerantExecutionTaskMemoryEstimationQuantile(Session session)
    {
        return session.getSystemProperty(FAULT_TOLERANT_EXECUTION_TASK_MEMORY_ESTIMATION_QUANTILE, Double.class);
    }

    public static int getFaultTolerantExecutionMaxPartitionCount(Session session)
    {
        return session.getSystemProperty(FAULT_TOLERANT_EXECUTION_MAX_PARTITION_COUNT, Integer.class);
    }

    public static int getFaultTolerantExecutionMinPartitionCount(Session session)
    {
        return session.getSystemProperty(FAULT_TOLERANT_EXECUTION_MIN_PARTITION_COUNT, Integer.class);
    }

    public static int getFaultTolerantExecutionMinPartitionCountForWrite(Session session)
    {
        return session.getSystemProperty(FAULT_TOLERANT_EXECUTION_MIN_PARTITION_COUNT_FOR_WRITE, Integer.class);
    }

    public static boolean isAdaptivePartialAggregationEnabled(Session session)
    {
        return session.getSystemProperty(ADAPTIVE_PARTIAL_AGGREGATION_ENABLED, Boolean.class);
    }

    public static double getAdaptivePartialAggregationUniqueRowsRatioThreshold(Session session)
    {
        return session.getSystemProperty(ADAPTIVE_PARTIAL_AGGREGATION_UNIQUE_ROWS_RATIO_THRESHOLD, Double.class);
    }

    public static boolean isRemoteTaskAdaptiveUpdateRequestSizeEnabled(Session session)
    {
        return session.getSystemProperty(REMOTE_TASK_ADAPTIVE_UPDATE_REQUEST_SIZE_ENABLED, Boolean.class);
    }

    public static DataSize getMaxRemoteTaskRequestSize(Session session)
    {
        return session.getSystemProperty(REMOTE_TASK_MAX_REQUEST_SIZE, DataSize.class);
    }

    public static DataSize getRemoteTaskRequestSizeHeadroom(Session session)
    {
        return session.getSystemProperty(REMOTE_TASK_REQUEST_SIZE_HEADROOM, DataSize.class);
    }

    public static int getRemoteTaskGuaranteedSplitsPerRequest(Session session)
    {
        return session.getSystemProperty(REMOTE_TASK_GUARANTEED_SPLITS_PER_REQUEST, Integer.class);
    }

    public static long getJoinPartitionedBuildMinRowCount(Session session)
    {
        return session.getSystemProperty(JOIN_PARTITIONED_BUILD_MIN_ROW_COUNT, Long.class);
    }

    public static DataSize getMinInputSizePerTask(Session session)
    {
        return session.getSystemProperty(MIN_INPUT_SIZE_PER_TASK, DataSize.class);
    }

    public static long getMinInputRowsPerTask(Session session)
    {
        return session.getSystemProperty(MIN_INPUT_ROWS_PER_TASK, Long.class);
    }

    public static boolean isUseExactPartitioning(Session session)
    {
        return session.getSystemProperty(USE_EXACT_PARTITIONING, Boolean.class);
    }

    public static boolean isUseCostBasedPartitioning(Session session)
    {
        return session.getSystemProperty(USE_COST_BASED_PARTITIONING, Boolean.class);
    }

    public static boolean isForceSpillingOperator(Session session)
    {
        return session.getSystemProperty(FORCE_SPILLING_JOIN, Boolean.class);
    }

    public static boolean isFaultTolerantExecutionForcePreferredWritePartitioningEnabled(Session session)
    {
        return session.getSystemProperty(FAULT_TOLERANT_EXECUTION_FORCE_PREFERRED_WRITE_PARTITIONING_ENABLED, Boolean.class);
    }

    public static int getPagePartitioningBufferPoolSize(Session session)
    {
        return session.getSystemProperty(PAGE_PARTITIONING_BUFFER_POOL_SIZE, Integer.class);
    }
}
