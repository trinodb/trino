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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.tree.Expression;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.sql.planner.plan.AggregationNode.singleAggregation;
import static io.trino.sql.planner.plan.AggregationNode.singleGroupingSet;
import static io.trino.sql.planner.plan.Patterns.aggregation;

/**
 * Implements distinct aggregations with similar inputs by transforming plans of the following shape:
 * <pre>
 * - Aggregation
 *        GROUP BY (k)
 *        F1(DISTINCT s0, s1, ...),
 *        F2(DISTINCT s0, s1, ...),
 *     - X
 * </pre>
 * into
 * <pre>
 * - Aggregation
 *          GROUP BY (k)
 *          F1(s0, s1, ...)
 *          F2(s0, s1, ...)
 *      - Aggregation
 *             GROUP BY (k, s0, s1, ...)
 *          - X
 * </pre>
 * <p>
 * Assumes s0, s1, ... are symbol references (i.e., complex expressions have been pre-projected)
 */
public class SingleDistinctAggregationToGroupBy
        implements Rule<AggregationNode>
{
    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(SingleDistinctAggregationToGroupBy::hasSingleDistinctInput)
            .matching(SingleDistinctAggregationToGroupBy::allDistinctAggregates)
            .matching(SingleDistinctAggregationToGroupBy::noFilters)
            .matching(SingleDistinctAggregationToGroupBy::noOrdering)
            .matching(SingleDistinctAggregationToGroupBy::noMasks);

    private static boolean hasSingleDistinctInput(AggregationNode aggregationNode)
    {
        return extractArgumentSets(aggregationNode)
                .count() == 1;
    }

    private static boolean allDistinctAggregates(AggregationNode aggregationNode)
    {
        return aggregationNode.getAggregations()
                .values().stream()
                .allMatch(Aggregation::isDistinct);
    }

    private static boolean noFilters(AggregationNode aggregationNode)
    {
        return aggregationNode.getAggregations()
                .values().stream()
                .noneMatch(aggregation -> aggregation.getFilter().isPresent());
    }

    private static boolean noOrdering(AggregationNode aggregationNode)
    {
        return aggregationNode.getAggregations()
                .values().stream()
                .noneMatch(aggregation -> aggregation.getOrderingScheme().isPresent());
    }

    private static boolean noMasks(AggregationNode aggregationNode)
    {
        return aggregationNode.getAggregations()
                .values().stream()
                .noneMatch(aggregation -> aggregation.getMask().isPresent());
    }

    private static Stream<Set<Expression>> extractArgumentSets(AggregationNode aggregationNode)
    {
        return aggregationNode.getAggregations()
                .values().stream()
                .filter(Aggregation::isDistinct)
                .map(Aggregation::getArguments)
                .<Set<Expression>>map(HashSet::new)
                .distinct();
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode aggregation, Captures captures, Context context)
    {
        List<Set<Expression>> argumentSets = extractArgumentSets(aggregation)
                .collect(Collectors.toList());

        Set<Symbol> symbols = Iterables.getOnlyElement(argumentSets).stream()
                .map(Symbol::from)
                .collect(Collectors.toSet());

        return Result.ofPlanNode(
                AggregationNode.builderFrom(aggregation)
                        .setSource(
                                singleAggregation(
                                        context.getIdAllocator().getNextId(),
                                        aggregation.getSource(),
                                        ImmutableMap.of(),
                                        singleGroupingSet(ImmutableList.<Symbol>builder()
                                                .addAll(aggregation.getGroupingKeys())
                                                .addAll(symbols)
                                                .build())))
                        .setAggregations(
                                // remove DISTINCT flag from function calls
                                aggregation.getAggregations()
                                        .entrySet().stream()
                                        .collect(Collectors.toMap(
                                                Map.Entry::getKey,
                                                e -> removeDistinct(e.getValue()))))
                        .setPreGroupedSymbols(ImmutableList.of())
                        .build());
    }

    private static Aggregation removeDistinct(Aggregation aggregation)
    {
        checkArgument(aggregation.isDistinct(), "Expected aggregation to have DISTINCT input");

        return new Aggregation(
                aggregation.getResolvedFunction(),
                aggregation.getArguments(),
                false,
                aggregation.getFilter(),
                aggregation.getOrderingScheme(),
                aggregation.getMask());
    }
}
