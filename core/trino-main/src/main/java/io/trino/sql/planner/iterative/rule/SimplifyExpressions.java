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
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.IrExpressionInterpreter;
import io.trino.sql.planner.iterative.Rule;

import java.util.Set;

import static io.trino.sql.planner.iterative.rule.ExtractCommonPredicatesExpressionRewriter.extractCommonPredicates;
import static io.trino.sql.planner.iterative.rule.NormalizeOrExpressionRewriter.normalizeOrExpression;
import static io.trino.sql.planner.iterative.rule.PushDownNegationsExpressionRewriter.pushDownNegations;
import static java.util.Objects.requireNonNull;

public class SimplifyExpressions
        extends ExpressionRewriteRuleSet
{
    public static Expression rewrite(Expression expression, Session session, PlannerContext plannerContext)
    {
        requireNonNull(plannerContext, "plannerContext is null");
        if (expression instanceof Reference) {
            return expression;
        }
        expression = pushDownNegations(expression);
        expression = extractCommonPredicates(expression);
        expression = normalizeOrExpression(expression);
        return new IrExpressionInterpreter(expression, plannerContext, session).optimize();
    }

    public SimplifyExpressions(PlannerContext plannerContext)
    {
        super(createRewrite(plannerContext));
    }

    @Override
    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(
                projectExpressionRewrite(),
                filterExpressionRewrite(),
                joinExpressionRewrite(),
                valuesExpressionRewrite(),
                patternRecognitionExpressionRewrite()); // ApplyNode and AggregationNode are not supported, because ExpressionInterpreter doesn't support them
    }

    private static ExpressionRewriter createRewrite(PlannerContext plannerContext)
    {
        requireNonNull(plannerContext, "plannerContext is null");

        return (expression, context) -> rewrite(expression, context.getSession(), plannerContext);
    }
}
