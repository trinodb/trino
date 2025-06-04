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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.trino.cost.TaskCountEstimator;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.sql.planner.OptimizerConfig.DistinctAggregationsStrategy;
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

import static io.trino.SystemSessionProperties.distinctAggregationsStrategy;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.planner.OptimizerConfig.DistinctAggregationsStrategy.AUTOMATIC;
import static io.trino.sql.planner.OptimizerConfig.DistinctAggregationsStrategy.MARK_DISTINCT;
import static io.trino.sql.planner.iterative.rule.DistinctAggregationStrategyChooser.createDistinctAggregationStrategyChooser;
import static io.trino.sql.planner.plan.Patterns.aggregation;
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
    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(MultipleDistinctAggregationToMarkDistinct::canUseMarkDistinct);

    public static boolean canUseMarkDistinct(AggregationNode aggregationNode)
    {
        return hasNoDistinctWithFilterOrMask(aggregationNode) &&
               (hasMultipleDistincts(aggregationNode) || hasMixedDistinctAndNonDistincts(aggregationNode));
    }

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

    private final DistinctAggregationStrategyChooser distinctAggregationStrategyChooser;

    public MultipleDistinctAggregationToMarkDistinct(TaskCountEstimator taskCountEstimator, Metadata metadata)
    {
        this.distinctAggregationStrategyChooser = createDistinctAggregationStrategyChooser(taskCountEstimator, metadata);
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode parent, Captures captures, Context context)
    {
        DistinctAggregationsStrategy distinctAggregationsStrategy = distinctAggregationsStrategy(context.getSession());
        if (!(distinctAggregationsStrategy.equals(MARK_DISTINCT) ||
                (distinctAggregationsStrategy.equals(AUTOMATIC) && distinctAggregationStrategyChooser.shouldAddMarkDistinct(parent, context.getSession(), context.getStatsProvider(), context.getLookup())))) {
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
                    marker = context.getSymbolAllocator().newSymbol(Iterables.getLast(inputs).name() + "_distinct", BOOLEAN);
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
}
