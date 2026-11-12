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
import io.trino.metadata.FunctionPreimages;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.ExpressionTreeRewriter;
import io.trino.sql.planner.ComparisonPreimages;
import io.trino.sql.planner.SymbolAllocator;

import static io.trino.SystemSessionProperties.isFunctionPreimagesEnabled;

public final class UnwrapFunctionInComparison
        extends ExpressionRewriteRuleSet
{
    public UnwrapFunctionInComparison(PlannerContext plannerContext)
    {
        ComparisonPreimages preimages = new ComparisonPreimages(plannerContext);
        super((expression, context) -> unwrap(plannerContext, preimages, context.getSession(), context.getSymbolAllocator(), expression), session -> isFunctionPreimagesEnabled(session));
    }

    public static Expression unwrap(PlannerContext plannerContext, Session session, SymbolAllocator allocator, Expression expression)
    {
        return unwrap(plannerContext, new ComparisonPreimages(plannerContext), session, allocator, expression);
    }

    public static Expression unwrap(PlannerContext plannerContext, ComparisonPreimages preimages, Session session, SymbolAllocator allocator, Expression expression)
    {
        // Resolves each projection, implementation, and cast once for this expression tree.
        return unwrap(plannerContext, preimages, preimages.newFunctionPreimages(), session, allocator, expression);
    }

    public static Expression unwrap(PlannerContext plannerContext, ComparisonPreimages preimages, FunctionPreimages functionPreimages, Session session, SymbolAllocator allocator, Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new io.trino.sql.ir.ExpressionRewriter<Void>()
        {
            @Override
            public Expression rewriteExpression(Expression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                Expression rewritten = treeRewriter.defaultRewrite(node, context);
                // Fold comparison constants and fixed function parameters during the
                // existing traversal, without optimizing the whole expression again.
                if (!rewritten.children().isEmpty() && rewritten.children().stream().allMatch(Constant.class::isInstance)) {
                    rewritten = plannerContext.getExpressionOptimizer().process(rewritten, session, allocator, ImmutableMap.of()).orElse(rewritten);
                }
                return preimages.rewrite(session, functionPreimages, rewritten, allocator).orElse(rewritten);
            }
        }, expression);
    }
}
