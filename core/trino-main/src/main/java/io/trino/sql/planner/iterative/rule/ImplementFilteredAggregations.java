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
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.tree.Expression;

import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.ExpressionUtils.and;
import static io.trino.sql.ExpressionUtils.combineDisjunctsWithDefault;
import static io.trino.sql.planner.plan.Patterns.aggregation;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;

/**
 * Implements filtered aggregations by transforming plans of the following shape:
 * <pre>
 * - Aggregation
 *        F1(...) FILTER (WHERE C1(...)),
 *        F2(...) FILTER (WHERE C2(...)), mask (m)
 *     - X
 * </pre>
 * into
 * <pre>
 * - Aggregation
 *        F1(...) mask ($0)
 *        F2(...) mask ($2)
 *     - Filter(mask ($0) OR mask ($2))
 *     - Project
 *            &lt;identity projections for existing fields&gt;
 *            $2 = m AND $1
 *     - Project
 *            &lt;identity projections for existing fields&gt;
 *            $0 = C1(...)
 *            $1 = C2(...)
 *         - X
 * </pre>
 */
public class ImplementFilteredAggregations
        implements Rule<AggregationNode>
{
    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(ImplementFilteredAggregations::hasFilters);

    private final Metadata metadata;

    public ImplementFilteredAggregations(Metadata metadata)
    {
        this.metadata = metadata;
    }

    private static boolean hasFilters(AggregationNode aggregation)
    {
        return aggregation.getAggregations()
                .values().stream()
                .anyMatch(e -> e.getFilter().isPresent());
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode aggregationNode, Captures captures, Context context)
    {
        Assignments.Builder newAssignments = Assignments.builder();
        ImmutableMap.Builder<Symbol, Aggregation> aggregations = ImmutableMap.builder();
        ImmutableList.Builder<Expression> maskSymbols = ImmutableList.builder();
        boolean aggregateWithoutFilterOrMaskPresent = false;

        for (Map.Entry<Symbol, Aggregation> entry : aggregationNode.getAggregations().entrySet()) {
            Symbol output = entry.getKey();

            // strip the filters
            Aggregation aggregation = entry.getValue();
            Optional<Symbol> mask = aggregation.getMask();

            if (aggregation.getFilter().isPresent()) {
                Symbol filter = aggregation.getFilter().get();
                if (mask.isPresent()) {
                    Symbol newMask = context.getSymbolAllocator().newSymbol("mask", BOOLEAN);
                    Expression expression = and(mask.get().toSymbolReference(), filter.toSymbolReference());
                    newAssignments.put(newMask, expression);
                    mask = Optional.of(newMask);
                    maskSymbols.add(newMask.toSymbolReference());
                }
                else {
                    mask = Optional.of(filter);
                    maskSymbols.add(filter.toSymbolReference());
                }
            }
            else if (mask.isPresent()) {
                maskSymbols.add(mask.get().toSymbolReference());
            }
            else {
                aggregateWithoutFilterOrMaskPresent = true;
            }

            aggregations.put(output, new Aggregation(
                    aggregation.getResolvedFunction(),
                    aggregation.getArguments(),
                    aggregation.isDistinct(),
                    Optional.empty(),
                    aggregation.getOrderingScheme(),
                    mask));
        }

        Expression predicate = TRUE_LITERAL;
        if (!aggregationNode.hasNonEmptyGroupingSet() && !aggregateWithoutFilterOrMaskPresent) {
            predicate = combineDisjunctsWithDefault(metadata, maskSymbols.build(), TRUE_LITERAL);
        }

        // identity projection for all existing inputs
        newAssignments.putIdentities(aggregationNode.getSource().getOutputSymbols());

        return Result.ofPlanNode(
                AggregationNode.builderFrom(aggregationNode)
                        .setId(context.getIdAllocator().getNextId())
                        .setSource(new FilterNode(
                                context.getIdAllocator().getNextId(),
                                new ProjectNode(
                                        context.getIdAllocator().getNextId(),
                                        aggregationNode.getSource(),
                                        newAssignments.build()),
                                predicate))
                        .setAggregations(aggregations.buildOrThrow())
                        .setPreGroupedSymbols(ImmutableList.of())
                        .build());
    }
}
