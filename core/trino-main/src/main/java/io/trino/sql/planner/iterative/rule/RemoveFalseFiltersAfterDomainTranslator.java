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
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.DomainTranslator;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.tree.Expression;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static io.trino.sql.ExpressionUtils.combineConjuncts;
import static io.trino.sql.ExpressionUtils.extractConjuncts;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static io.trino.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static java.util.Objects.requireNonNull;

/**
 * Uses DomainTranslator#getExtractionResult to infer that the expression is "false" in some cases (TupleDomain.none()).
 */
public class RemoveFalseFiltersAfterDomainTranslator
        extends ExpressionRewriteRuleSet
{
    public RemoveFalseFiltersAfterDomainTranslator(PlannerContext plannerContext)
    {
        super(createRewrite(plannerContext));
    }

    @Override
    public Set<Rule<?>> rules()
    {
        // Can only apply on expressions that are used for filtering, otherwise TupleDomain.none() might represent a NULL value
        return ImmutableSet.of(
                filterExpressionRewrite(),
                joinExpressionRewrite());
    }

    private static ExpressionRewriter createRewrite(PlannerContext plannerContext)
    {
        requireNonNull(plannerContext, "plannerContext is null");

        return (expression, context) -> rewrite(context.getSession(), plannerContext, context.getSymbolAllocator().getTypes(), expression);
    }

    public static Expression rewrite(Session session, PlannerContext plannerContext, TypeProvider types, Expression expression)
    {
        Metadata metadata = plannerContext.getMetadata();
        List<Expression> deterministicPredicates = new ArrayList<>();
        for (Expression conjunct : extractConjuncts(expression)) {
            try {
                if (isDeterministic(conjunct, metadata)) {
                    deterministicPredicates.add(conjunct);
                }
            }
            catch (IllegalArgumentException ignored) {
                // Might fail in case of an unresolved function
            }
        }

        DomainTranslator.ExtractionResult decomposedPredicate = DomainTranslator.getExtractionResult(
                plannerContext,
                session,
                combineConjuncts(metadata, deterministicPredicates),
                types);

        if (decomposedPredicate.getTupleDomain().isNone()) {
            return FALSE_LITERAL;
        }

        return expression;
    }
}
