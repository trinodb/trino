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

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.ExpressionTreeRewriter;
import io.trino.sql.planner.ComparisonPreimages;
import io.trino.sql.planner.SymbolAllocator;

public final class UnwrapFunctionInComparison
        extends ExpressionRewriteRuleSet
{
    public UnwrapFunctionInComparison(PlannerContext plannerContext)
    {
        super((expression, context) -> unwrap(plannerContext, context.getSession(), context.getSymbolAllocator(), expression));
    }

    public static Expression unwrap(PlannerContext plannerContext, Session session, SymbolAllocator allocator, Expression expression)
    {
        expression = plannerContext.getExpressionOptimizer().process(expression, session, allocator, ImmutableMap.of()).orElse(expression);
        ComparisonPreimages preimages = new ComparisonPreimages(plannerContext, session);
        return ExpressionTreeRewriter.rewriteWith(new io.trino.sql.ir.ExpressionRewriter<Void>()
        {
            @Override
            public Expression rewriteExpression(Expression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                Expression rewritten = treeRewriter.defaultRewrite(node, context);
                return preimages.rewrite(rewritten, allocator).orElse(rewritten);
            }
        }, expression);
    }
}
