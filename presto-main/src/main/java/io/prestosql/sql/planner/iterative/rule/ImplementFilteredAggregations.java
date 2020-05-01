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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.metadata.Metadata;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.AggregationNode.Aggregation;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.SymbolReference;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.sql.ExpressionUtils.combineDisjunctsWithDefault;
import static io.prestosql.sql.planner.plan.Patterns.aggregation;
import static io.prestosql.sql.tree.BooleanLiteral.TRUE_LITERAL;

/**
 * Implements filtered aggregations by transforming plans of the following shape:
 * <pre>
 * - Aggregation
 *        F1(...) FILTER (WHERE C1(...)),
 *        F2(...) FILTER (WHERE C2(...))
 *     - X
 * </pre>
 * into
 * <pre>
 * - Aggregation
 *        F1(...) mask ($0)
 *        F2(...) mask ($1)
 *     - Filter(mask ($0) OR mask ($1))
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
                .anyMatch(e -> e.getFilter().isPresent() &&
                        !e.getMask().isPresent()); // can't handle filtered aggregations with DISTINCT (conservatively, if they have a mask)
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
        boolean aggregateWithoutFilterPresent = false;

        for (Map.Entry<Symbol, Aggregation> entry : aggregationNode.getAggregations().entrySet()) {
            Symbol output = entry.getKey();

            // strip the filters
            Aggregation aggregation = entry.getValue();
            Optional<Symbol> mask = aggregation.getMask();

            if (aggregation.getFilter().isPresent()) {
                Symbol filter = aggregation.getFilter().get();
                Symbol symbol = context.getSymbolAllocator().newSymbol(filter.getName(), BOOLEAN);
                verify(!mask.isPresent(), "Expected aggregation without mask symbols, see Rule pattern");
                newAssignments.put(symbol, new SymbolReference(filter.getName()));
                mask = Optional.of(symbol);

                maskSymbols.add(symbol.toSymbolReference());
            }
            else {
                aggregateWithoutFilterPresent = true;
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
        if (!aggregationNode.hasNonEmptyGroupingSet() && !aggregateWithoutFilterPresent) {
            predicate = combineDisjunctsWithDefault(metadata, maskSymbols.build(), TRUE_LITERAL);
        }

        // identity projection for all existing inputs
        newAssignments.putIdentities(aggregationNode.getSource().getOutputSymbols());

        return Result.ofPlanNode(
                new AggregationNode(
                        context.getIdAllocator().getNextId(),
                        new FilterNode(
                                context.getIdAllocator().getNextId(),
                                new ProjectNode(
                                        context.getIdAllocator().getNextId(),
                                        aggregationNode.getSource(),
                                        newAssignments.build()),
                                predicate),
                        aggregations.build(),
                        aggregationNode.getGroupingSets(),
                        ImmutableList.of(),
                        aggregationNode.getStep(),
                        aggregationNode.getHashSymbol(),
                        aggregationNode.getGroupIdSymbol()));
    }
}
