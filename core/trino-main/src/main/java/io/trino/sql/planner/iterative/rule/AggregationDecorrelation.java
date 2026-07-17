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
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.PlanNode;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;

final class AggregationDecorrelation
{
    private AggregationDecorrelation() {}

    public static boolean isDistinctOperator(PlanNode node)
    {
        return node instanceof AggregationNode aggregationNode &&
                aggregationNode.getAggregations().isEmpty() &&
                aggregationNode.getGroupingSetCount() == 1 &&
                aggregationNode.hasNonEmptyGroupingSet();
    }

    /**
     * Returns true if every aggregation function "ignores null input values" or more
     * precisely that result of Aggregation over relation {@code R′ = R ∪ N} is guaranteed
     * to be the same as result of Aggregation over relation {@code R}, provided that each
     * tuple of {@code N}:
     * <ul>
     *    <li>has the same grouping keys as one of the tuples of {@code R},
     *    <li>has NULL value for each argument of the aggregation function.
     * </ul>
     */
    public static boolean isNullRowInsensitiveAggregation(AggregationNode node)
    {
        // For PARTIAL/FINAL/INTERMEDIATE steps, the aggregation.getResolvedFunction() inspected below does not carry the @InputFunction attributes
        checkArgument(node.getStep() == SINGLE, "Expected SINGLE step aggregation, got %s", node.getStep());

        for (Aggregation aggregation : node.getAggregations().values()) {
            if (!aggregation.getArguments().stream().allMatch(AggregationDecorrelation::isNullOnNullInput)) {
                return false;
            }
            if (aggregation.getResolvedFunction().functionNullability().getArgumentNullable().stream().allMatch(nullable -> nullable)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns true when expression is guaranteed to return NULL when all references in the expression are NULL.
     */
    private static boolean isNullOnNullInput(Expression expression)
    {
        // TODO expand to more expression shapes
        return expression instanceof Reference;
    }

    public static Map<Symbol, Aggregation> rewriteWithMasks(Map<Symbol, Aggregation> aggregations, Map<Symbol, Symbol> masks)
    {
        ImmutableMap.Builder<Symbol, Aggregation> rewritten = ImmutableMap.builder();
        for (Entry<Symbol, Aggregation> entry : aggregations.entrySet()) {
            Symbol symbol = entry.getKey();
            Aggregation aggregation = entry.getValue();
            rewritten.put(symbol, new Aggregation(
                    aggregation.getResolvedFunction(),
                    aggregation.getArguments(),
                    aggregation.isDistinct(),
                    aggregation.getFilter(),
                    aggregation.getOrderingScheme(),
                    Optional.of(masks.get(symbol))));
        }

        return rewritten.buildOrThrow();
    }

    /**
     * Creates distinct aggregation node based on existing distinct aggregation node.
     *
     * @see #isDistinctOperator(PlanNode)
     */
    public static AggregationNode restoreDistinctAggregation(
            AggregationNode distinct,
            PlanNode source,
            List<Symbol> groupingKeys)
    {
        checkArgument(isDistinctOperator(distinct));
        return new AggregationNode(
                distinct.getId(),
                source,
                ImmutableMap.of(),
                AggregationNode.singleGroupingSet(groupingKeys),
                ImmutableList.of(),
                distinct.getStep(),
                Optional.empty(),
                Optional.empty());
    }
}
