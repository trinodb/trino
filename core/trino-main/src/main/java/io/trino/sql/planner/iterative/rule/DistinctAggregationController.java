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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.cost.StatsProvider;
import io.trino.cost.TaskCountEstimator;
import io.trino.metadata.Metadata;
import io.trino.sql.ir.SymbolReference;
import io.trino.sql.planner.OptimizerConfig.DistinctAggregationsStrategy;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.UnionNode;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.SystemSessionProperties.distinctAggregationsStrategy;
import static io.trino.SystemSessionProperties.getTaskConcurrency;
import static io.trino.sql.planner.OptimizerConfig.DistinctAggregationsStrategy.AUTOMATIC;
import static io.trino.sql.planner.OptimizerConfig.DistinctAggregationsStrategy.MARK_DISTINCT;
import static io.trino.sql.planner.OptimizerConfig.DistinctAggregationsStrategy.PRE_AGGREGATE;
import static io.trino.sql.planner.OptimizerConfig.DistinctAggregationsStrategy.SINGLE_STEP;
import static io.trino.sql.planner.OptimizerConfig.DistinctAggregationsStrategy.SPLIT_TO_SUBQUERIES;
import static io.trino.sql.planner.iterative.rule.DistinctAggregationToGroupBy.canUsePreAggregate;
import static io.trino.sql.planner.iterative.rule.DistinctAggregationToGroupBy.distinctAggregationsUniqueArgumentCount;
import static io.trino.sql.planner.iterative.rule.MultipleDistinctAggregationToMarkDistinct.canUseMarkDistinct;
import static io.trino.sql.planner.iterative.rule.MultipleDistinctAggregationsToSubqueries.isAggregationCandidateForSplittingToSubqueries;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static java.lang.Double.isNaN;
import static java.util.Objects.requireNonNull;

/**
 * Controls which implementation of distinct aggregation should be used for particular {@link AggregationNode}
 */
public class DistinctAggregationController
{
    private static final int MARK_DISTINCT_MAX_OUTPUT_ROW_COUNT_MULTIPLIER = 8;
    private static final int PRE_AGGREGATE_MAX_OUTPUT_ROW_COUNT_MULTIPLIER = MARK_DISTINCT_MAX_OUTPUT_ROW_COUNT_MULTIPLIER * 8;
    private static final double MAX_JOIN_GROUPING_KEYS_SIZE = 100 * 1024 * 1024; // 100 MB

    private final TaskCountEstimator taskCountEstimator;
    private final Metadata metadata;

    @Inject
    public DistinctAggregationController(TaskCountEstimator taskCountEstimator, Metadata metadata)
    {
        this.taskCountEstimator = requireNonNull(taskCountEstimator, "taskCountEstimator is null");
        this.metadata = metadata;
    }

    public boolean shouldAddMarkDistinct(AggregationNode aggregationNode, Rule.Context context)
    {
        return chooseMarkDistinctStrategy(aggregationNode, context) == MARK_DISTINCT;
    }

    public boolean shouldUsePreAggregate(AggregationNode aggregationNode, Rule.Context context)
    {
        return chooseMarkDistinctStrategy(aggregationNode, context) == PRE_AGGREGATE;
    }

    public boolean shouldSplitToSubqueries(AggregationNode aggregationNode, Rule.Context context)
    {
        return chooseMarkDistinctStrategy(aggregationNode, context) == SPLIT_TO_SUBQUERIES;
    }

    private DistinctAggregationsStrategy chooseMarkDistinctStrategy(AggregationNode aggregationNode, Rule.Context context)
    {
        DistinctAggregationsStrategy distinctAggregationsStrategy = distinctAggregationsStrategy(context.getSession());
        if (distinctAggregationsStrategy != AUTOMATIC) {
            if (distinctAggregationsStrategy == MARK_DISTINCT && canUseMarkDistinct(aggregationNode)) {
                return MARK_DISTINCT;
            }
            if (distinctAggregationsStrategy == PRE_AGGREGATE && canUsePreAggregate(aggregationNode)) {
                return PRE_AGGREGATE;
            }
            if (distinctAggregationsStrategy == SPLIT_TO_SUBQUERIES && isAggregationCandidateForSplittingToSubqueries(aggregationNode) && isAggregationSourceSupportedForSubqueries(aggregationNode.getSource(), context)) {
                return SPLIT_TO_SUBQUERIES;
            }
            // in case strategy is chosen by the session property, but we cannot use it, lets fallback to single-step
            return SINGLE_STEP;
        }
        double numberOfDistinctValues = getMinDistinctValueCountEstimate(aggregationNode, context);
        int maxNumberOfConcurrentThreadsForAggregation = getMaxNumberOfConcurrentThreadsForAggregation(context);

        // use single_step if it can be parallelized
        // small numberOfDistinctValues reduces the distinct aggregation parallelism, also because the partitioning may be skewed.
        // this makes query to underutilize the cluster CPU but also to possibly concentrate memory on few nodes.
        // single_step alternatives should increase the parallelism at a cost of CPU.
        if (!aggregationNode.getGroupingKeys().isEmpty() && // global distinct aggregation is computed using a single thread. Strategies other than single_step will help parallelize the execution.
                !isNaN(numberOfDistinctValues) && // if the estimate is unknown, use alternatives to avoid query failure
                (numberOfDistinctValues > PRE_AGGREGATE_MAX_OUTPUT_ROW_COUNT_MULTIPLIER * maxNumberOfConcurrentThreadsForAggregation ||
                        (numberOfDistinctValues > MARK_DISTINCT_MAX_OUTPUT_ROW_COUNT_MULTIPLIER * maxNumberOfConcurrentThreadsForAggregation &&
                                // if the NDV and the number of grouping keys is small, pre-aggregate is faster than single_step at a cost of CPU
                                aggregationNode.getGroupingKeys().size() > 2))) {
            return SINGLE_STEP;
        }

        if (isAggregationCandidateForSplittingToSubqueries(aggregationNode) && shouldSplitAggregationToSubqueries(aggregationNode, context)) {
            // for simple distinct aggregations on top of table scan it makes sense to split the aggregation into multiple subqueries,
            // so they can be handled by the SingleDistinctAggregationToGroupBy and use other single column optimizations
            return SPLIT_TO_SUBQUERIES;
        }

        // mark-distinct is better than pre-aggregate if the number of group-by keys is bigger than 2
        // because group-by keys are added to every grouping set and this makes partial aggregation behaves badly
        if (canUsePreAggregate(aggregationNode) && aggregationNode.getGroupingKeys().size() <= 2) {
            return PRE_AGGREGATE;
        }
        else if (canUseMarkDistinct(aggregationNode)) {
            return MARK_DISTINCT;
        }

        // if no strategy found, use single_step by default
        return SINGLE_STEP;
    }

    private int getMaxNumberOfConcurrentThreadsForAggregation(Rule.Context context)
    {
        return taskCountEstimator.estimateHashedTaskCount(context.getSession()) * getTaskConcurrency(context.getSession());
    }

    private double getMinDistinctValueCountEstimate(AggregationNode aggregationNode, Rule.Context context)
    {
        // NDV stats for multiple grouping keys are unreliable, let's pick a conservative lower bound by taking maximum NDV for all grouping keys.
        // this assumes that grouping keys are 100% correlated.
        // in the case of a lower correlation, the NDV can only be higher.
        PlanNodeStatsEstimate sourceStats = context.getStatsProvider().getStats(aggregationNode.getSource());
        double max = Double.NaN;
        for (Symbol groupingKey : aggregationNode.getGroupingKeys()) {
            double distinctValuesCount = sourceStats.getSymbolStatistics(groupingKey).getDistinctValuesCount();
            if (isNaN(max) || distinctValuesCount > max) {
                max = distinctValuesCount;
            }
        }
        return max;
    }

    // Since, to avoid degradation caused by multiple table scans, we want to split to sub-queries only if we are confident
    // it brings big benefits, we are fairly conservative in the decision below.
    private boolean shouldSplitAggregationToSubqueries(AggregationNode aggregationNode, Rule.Context context)
    {
        if (!isAggregationSourceSupportedForSubqueries(aggregationNode.getSource(), context)) {
            // only table scan, union, filter and project are supported
            return false;
        }

        if (searchFrom(aggregationNode.getSource(), context.getLookup()).whereIsInstanceOfAny(UnionNode.class).findFirst().isPresent()) {
            // supporting union with auto decision is complex
            return false;
        }

        // skip if the source has a filter with low selectivity, as the scan and filter can
        // be the main bottleneck in this case, and we want to avoid duplicating this effort.
        if (searchFrom(aggregationNode.getSource(), context.getLookup())
                .where(node -> node instanceof FilterNode filterNode && isSelective(filterNode, context.getStatsProvider()))
                .matches()) {
            return false;
        }

        if (isAdditionalReadOverheadTooExpensive(aggregationNode, context)) {
            return false;
        }

        if (aggregationNode.hasSingleGlobalAggregation()) {
            return true;
        }

        PlanNodeStatsEstimate stats = context.getStatsProvider().getStats(aggregationNode);
        double groupingKeysSizeInBytes = stats.getOutputSizeInBytes(aggregationNode.getGroupingKeys());
        if (isNaN(groupingKeysSizeInBytes) || groupingKeysSizeInBytes > MAX_JOIN_GROUPING_KEYS_SIZE) {
            // estimated group by result size is big so that both calculating aggregation multiple times and join would be inefficient
            return false;
        }

        return true;
    }

    private static boolean isAdditionalReadOverheadTooExpensive(AggregationNode aggregationNode, Rule.Context context)
    {
        Set<Symbol> distinctInputs = aggregationNode.getAggregations()
                .values().stream()
                .filter(AggregationNode.Aggregation::isDistinct)
                .flatMap(aggregation -> aggregation.getArguments().stream())
                .filter(expression -> expression instanceof SymbolReference)
                .map(Symbol::from)
                .collect(toImmutableSet());

        TableScanNode tableScanNode = searchFrom(aggregationNode.getSource(), context.getLookup()).whereIsInstanceOfAny(TableScanNode.class).findOnlyElement();
        Set<Symbol> additionalColumns = Sets.difference(ImmutableSet.copyOf(tableScanNode.getOutputSymbols()), distinctInputs);

        // Group by columns need to read N times, where N is number of sub-queries.
        // Distinct columns are read once.
        double singleTableScanDataSize = context.getStatsProvider().getStats(tableScanNode).getOutputSizeInBytes(tableScanNode.getOutputSymbols());
        double additionalColumnsDataSize = context.getStatsProvider().getStats(tableScanNode).getOutputSizeInBytes(additionalColumns);
        long subqueryCount = distinctAggregationsUniqueArgumentCount(aggregationNode);
        double distinctInputDataSize = singleTableScanDataSize - additionalColumnsDataSize;
        double subqueriesTotalDataSize = additionalColumnsDataSize * subqueryCount + distinctInputDataSize;

        return isNaN(subqueriesTotalDataSize) ||
                isNaN(singleTableScanDataSize) ||
                // we would read more than 50% more data
                subqueriesTotalDataSize / singleTableScanDataSize > 1.5;
    }

    private static boolean isSelective(FilterNode filterNode, StatsProvider statsProvider)
    {
        double filterOutputRowCount = statsProvider.getStats(filterNode).getOutputRowCount();
        double filterSourceRowCount = statsProvider.getStats(filterNode.getSource()).getOutputRowCount();
        return filterOutputRowCount / filterSourceRowCount < 0.5;
    }

    // Only table scan, union, filter and project are supported.
    // PlanCopier.copyPlan must support all supported nodes here.
    // Additionally, we should split the table scan only if reading single columns is efficient in the given connector.
    private boolean isAggregationSourceSupportedForSubqueries(PlanNode source, Rule.Context context)
    {
        if (searchFrom(source, context.getLookup())
                .where(node -> !(node instanceof TableScanNode
                        || node instanceof FilterNode
                        || node instanceof ProjectNode
                        || node instanceof UnionNode))
                .findFirst()
                .isPresent()) {
            return false;
        }

        List<PlanNode> tableScans = searchFrom(source, context.getLookup())
                .whereIsInstanceOfAny(TableScanNode.class)
                .findAll();

        if (tableScans.isEmpty()) {
            // at least one table scan is expected
            return false;
        }

        return tableScans.stream()
                .allMatch(tableScanNode -> metadata.isColumnarTableScan(context.getSession(), ((TableScanNode) tableScanNode).getTable()));
    }
}
