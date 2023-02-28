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

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.trino.cost.TaskCountEstimator;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.OptimizerConfig.MarkDistinctStrategy;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.MarkDistinctNode;
import io.trino.sql.planner.plan.PlanNode;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.SystemSessionProperties.getTaskConcurrency;
import static io.trino.SystemSessionProperties.isOptimizeDistinctAggregationEnabled;
import static io.trino.SystemSessionProperties.markDistinctStrategy;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.planner.OptimizerConfig.MarkDistinctStrategy.AUTOMATIC;
import static io.trino.sql.planner.OptimizerConfig.MarkDistinctStrategy.NONE;
import static io.trino.sql.planner.plan.Patterns.aggregation;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

/**
 * Implements distinct aggregations with different inputs by transforming plans of the following shape:
 * <pre>
 * - Aggregation
 *        GROUP BY (k)
 *        F1(DISTINCT a0, a1, ...)
 *        F2(DISTINCT b0, b1, ...)
 *        F3(c0, c1, ...)
 *     - X
 * </pre>
 * into
 * <pre>
 * - Aggregation
 *        GROUP BY (k)
 *        F1(a0, a1, ...) mask ($0)
 *        F2(b0, b1, ...) mask ($1)
 *        F3(c0, c1, ...)
 *     - MarkDistinct (k, a0, a1, ...) -> $0
 *          - MarkDistinct (k, b0, b1, ...) -> $1
 *              - X
 * </pre>
 */
public class MultipleDistinctAggregationToMarkDistinct
        implements Rule<AggregationNode>
{
    private static final int MARK_DISTINCT_MAX_OUTPUT_ROW_COUNT_MULTIPLIER = 8;
    private static final int OPTIMIZED_DISTINCT_MAX_OUTPUT_ROW_COUNT_MULTIPLIER = MARK_DISTINCT_MAX_OUTPUT_ROW_COUNT_MULTIPLIER * 8;

    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(
                    Predicates.and(
                            MultipleDistinctAggregationToMarkDistinct::hasNoDistinctWithFilterOrMask,
                            Predicates.or(
                                    MultipleDistinctAggregationToMarkDistinct::hasMultipleDistincts,
                                    MultipleDistinctAggregationToMarkDistinct::hasMixedDistinctAndNonDistincts)));

    private static boolean hasNoDistinctWithFilterOrMask(AggregationNode aggregationNode)
    {
        return aggregationNode.getAggregations()
                .values().stream()
                .noneMatch(aggregation -> aggregation.isDistinct() && (aggregation.getFilter().isPresent() || aggregation.getMask().isPresent()));
    }

    private static boolean hasMultipleDistincts(AggregationNode aggregationNode)
    {
        return aggregationNode.getAggregations()
                .values().stream()
                .filter(Aggregation::isDistinct)
                .map(Aggregation::getArguments)
                .map(HashSet::new)
                .distinct()
                .count() > 1;
    }

    private static boolean hasMixedDistinctAndNonDistincts(AggregationNode aggregationNode)
    {
        long distincts = aggregationNode.getAggregations()
                .values().stream()
                .filter(Aggregation::isDistinct)
                .count();

        return distincts > 0 && distincts < aggregationNode.getAggregations().size();
    }

    private final TaskCountEstimator taskCountEstimator;

    public MultipleDistinctAggregationToMarkDistinct(TaskCountEstimator taskCountEstimator)
    {
        this.taskCountEstimator = requireNonNull(taskCountEstimator, "taskCountEstimator is null");
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode parent, Captures captures, Context context)
    {
        MarkDistinctStrategy markDistinctStrategy = markDistinctStrategy(context.getSession());
        if (markDistinctStrategy.equals(NONE)) {
            return Result.empty();
        }

        if (markDistinctStrategy.equals(AUTOMATIC) && !shouldAddMarkDistinct(parent, context)) {
            return Result.empty();
        }

        // the distinct marker for the given set of input columns
        Map<Set<Symbol>, Symbol> markers = new HashMap<>();

        Map<Symbol, Aggregation> newAggregations = new HashMap<>();
        PlanNode subPlan = parent.getSource();

        for (Map.Entry<Symbol, Aggregation> entry : parent.getAggregations().entrySet()) {
            Aggregation aggregation = entry.getValue();

            if (aggregation.isDistinct() && aggregation.getFilter().isEmpty() && aggregation.getMask().isEmpty()) {
                Set<Symbol> inputs = aggregation.getArguments().stream()
                        .map(Symbol::from)
                        .collect(toSet());

                Symbol marker = markers.get(inputs);
                if (marker == null) {
                    marker = context.getSymbolAllocator().newSymbol(Iterables.getLast(inputs).getName(), BOOLEAN, "distinct");
                    markers.put(inputs, marker);

                    ImmutableSet.Builder<Symbol> distinctSymbols = ImmutableSet.<Symbol>builder()
                            .addAll(parent.getGroupingKeys())
                            .addAll(inputs);
                    parent.getGroupIdSymbol().ifPresent(distinctSymbols::add);

                    subPlan = new MarkDistinctNode(
                            context.getIdAllocator().getNextId(),
                            subPlan,
                            marker,
                            ImmutableList.copyOf(distinctSymbols.build()),
                            Optional.empty());
                }

                // remove the distinct flag and set the distinct marker
                newAggregations.put(entry.getKey(),
                        new Aggregation(
                                aggregation.getResolvedFunction(),
                                aggregation.getArguments(),
                                false,
                                aggregation.getFilter(),
                                aggregation.getOrderingScheme(),
                                Optional.of(marker)));
            }
            else {
                newAggregations.put(entry.getKey(), aggregation);
            }
        }

        return Result.ofPlanNode(
                AggregationNode.builderFrom(parent)
                        .setSource(subPlan)
                        .setAggregations(newAggregations)
                        .setPreGroupedSymbols(ImmutableList.of())
                        .build());
    }

    private boolean shouldAddMarkDistinct(AggregationNode aggregationNode, Context context)
    {
        if (aggregationNode.getGroupingKeys().isEmpty()) {
            // global distinct aggregation is computed using a single thread. MarkDistinct will help parallelize the execution.
            return true;
        }
        if (aggregationNode.getGroupingKeys().size() > 1) {
            // NDV stats for multiple grouping keys are unreliable, let's keep MarkDistinct for this case to avoid significant slowdown or OOM/too big hash table issues in case of
            // overestimation of very small NDV with big number of distinct values inside the groups.
            return true;
        }
        double numberOfDistinctValues = context.getStatsProvider().getStats(aggregationNode).getOutputRowCount();
        if (Double.isNaN(numberOfDistinctValues)) {
            // if the estimate is unknown, use MarkDistinct to avoid query failure
            return true;
        }
        int maxNumberOfConcurrentThreadsForAggregation = taskCountEstimator.estimateHashedTaskCount(context.getSession()) * getTaskConcurrency(context.getSession());

        if (numberOfDistinctValues <= MARK_DISTINCT_MAX_OUTPUT_ROW_COUNT_MULTIPLIER * maxNumberOfConcurrentThreadsForAggregation) {
            // small numberOfDistinctValues reduces the distinct aggregation parallelism, also because the partitioning may be skewed.
            // This makes query to underutilize the cluster CPU but also to possibly concentrate memory on few nodes.
            // MarkDistinct should increase the parallelism at a cost of CPU.
            return true;
        }

        if (isOptimizeDistinctAggregationEnabled(context.getSession()) &&
                numberOfDistinctValues <= OPTIMIZED_DISTINCT_MAX_OUTPUT_ROW_COUNT_MULTIPLIER * maxNumberOfConcurrentThreadsForAggregation &&
                hasSingleDistinctAndNonDistincts(aggregationNode)) {
            // with medium number of numberOfDistinctValues, OptimizeMixedDistinctAggregations
            // will be beneficial for query latency (duration) over distinct aggregation at a cost of increased CPU,
            // but it relies on existence of MarkDistinct nodes.
            return true;
        }

        return false;
    }

    private static boolean hasSingleDistinctAndNonDistincts(AggregationNode aggregationNode)
    {
        long distincts = aggregationNode.getAggregations()
                .values().stream()
                .filter(Aggregation::isDistinct)
                .count();

        return distincts == 1 && distincts < aggregationNode.getAggregations().size();
    }
}
