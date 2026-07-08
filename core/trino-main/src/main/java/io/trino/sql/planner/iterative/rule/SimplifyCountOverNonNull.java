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
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import static io.trino.SystemSessionProperties.getCharVarcharCoercion;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.sql.planner.optimizations.NonNullDerivation.deriveNonNullSymbols;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.sql.planner.plan.Patterns.Aggregation.step;
import static io.trino.sql.planner.plan.Patterns.aggregation;
import static java.util.Objects.requireNonNull;

/// Rewrite `count(x)` to `count(*)` when `x` is guaranteed to be non-null, e.g. a column the
/// connector declares as `NOT NULL` or a symbol filtered by a null-rejecting predicate.
///
/// Dropping the argument allows the column to be pruned from the table scan entirely, and
/// produces the aggregation shape recognized by connector `count(*)` pushdown.
public class SimplifyCountOverNonNull
        implements Rule<AggregationNode>
{
    private static final CatalogSchemaFunctionName COUNT_NAME = builtinFunctionName("count");

    // Restricted to SINGLE step: for a split aggregation, the final count's argument is the
    // partial state, and rewriting it to count(*) would count rows instead of combining states
    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .with(step().equalTo(SINGLE))
            .matching(SimplifyCountOverNonNull::hasCandidateCount);

    private final PlannerContext plannerContext;

    public SimplifyCountOverNonNull(PlannerContext plannerContext)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode node, Captures captures, Context context)
    {
        Set<Symbol> nonNull = deriveNonNullSymbols(plannerContext, context.getSession(), node.getSource(), context.getLookup()::resolve);
        if (nonNull.isEmpty()) {
            return Result.empty();
        }

        ResolvedFunction countFunction = plannerContext.getMetadata().resolveBuiltinFunction(getCharVarcharCoercion(context.getSession()), "count", ImmutableList.of());

        boolean changed = false;
        Map<Symbol, AggregationNode.Aggregation> aggregations = new LinkedHashMap<>(node.getAggregations());
        for (Entry<Symbol, AggregationNode.Aggregation> entry : node.getAggregations().entrySet()) {
            AggregationNode.Aggregation aggregation = entry.getValue();
            if (isCandidateCount(aggregation) && nonNull.contains(Symbol.from(aggregation.getArguments().getFirst()))) {
                changed = true;
                aggregations.put(entry.getKey(), new AggregationNode.Aggregation(
                        countFunction,
                        ImmutableList.of(),
                        false,
                        aggregation.getFilter(),
                        Optional.empty(),
                        aggregation.getMask()));
            }
        }

        if (!changed) {
            return Result.empty();
        }

        return Result.ofPlanNode(AggregationNode.builderFrom(node)
                .setAggregations(aggregations)
                .setPreGroupedSymbols(ImmutableList.of())
                .build());
    }

    private static boolean hasCandidateCount(AggregationNode node)
    {
        return node.getAggregations().values().stream().anyMatch(SimplifyCountOverNonNull::isCandidateCount);
    }

    private static boolean isCandidateCount(AggregationNode.Aggregation aggregation)
    {
        BoundSignature signature = aggregation.getResolvedFunction().signature();
        return signature.getName().equals(COUNT_NAME) &&
                signature.getArgumentTypes().size() == 1 &&
                // count(DISTINCT x) counts distinct values, not rows
                !aggregation.isDistinct() &&
                aggregation.getOrderingScheme().isEmpty() &&
                aggregation.getArguments().getFirst() instanceof Reference;
    }
}
