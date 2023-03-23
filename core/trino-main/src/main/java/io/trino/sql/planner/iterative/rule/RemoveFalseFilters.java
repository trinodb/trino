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
import io.trino.metadata.Metadata;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.DomainTranslator;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NullLiteral;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.sql.ExpressionUtils.combineConjuncts;
import static io.trino.sql.ExpressionUtils.extractConjuncts;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static io.trino.sql.planner.plan.Patterns.filter;
import static java.util.Objects.requireNonNull;

public class RemoveFalseFilters
        implements Rule<FilterNode>
{
    private static final Pattern<FilterNode> PATTERN = filter();

    private final PlannerContext plannerContext;

    public RemoveFalseFilters(PlannerContext plannerContext)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
    }

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(FilterNode filterNode, Captures captures, Context context)
    {
        Expression predicate = filterNode.getPredicate();
        checkArgument(!(predicate instanceof NullLiteral), "Unexpected null literal without a cast to boolean");

        Metadata metadata = plannerContext.getMetadata();
        List<Expression> deterministicPredicates = new ArrayList<>();
        for (Expression conjunct : extractConjuncts(filterNode.getPredicate())) {
            if (isDeterministic(conjunct, metadata)) {
                deterministicPredicates.add(conjunct);
            }
        }

        DomainTranslator.ExtractionResult decomposedPredicate = DomainTranslator.getExtractionResult(
                plannerContext,
                context.getSession(),
                combineConjuncts(metadata, deterministicPredicates),
                context.getSymbolAllocator().getTypes());

        if (decomposedPredicate.getTupleDomain().isNone() ||
                extractConjuncts(decomposedPredicate.getRemainingExpression()).stream()
                        .anyMatch(conjunct -> conjunct instanceof Cast cast && cast.getExpression() instanceof NullLiteral)) {
            return Result.ofPlanNode(new ValuesNode(filterNode.getId(), filterNode.getOutputSymbols(), ImmutableList.of()));
        }

        return Result.empty();
    }
}
