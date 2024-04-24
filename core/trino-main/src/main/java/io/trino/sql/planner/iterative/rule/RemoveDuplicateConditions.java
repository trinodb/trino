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

import io.trino.sql.ir.Expression;
import io.trino.sql.ir.ExpressionTreeRewriter;
import io.trino.sql.ir.Logical;

import static io.trino.sql.ir.IrUtils.combinePredicates;
import static io.trino.sql.ir.IrUtils.extractPredicates;

/**
 * Flattens and removes duplicate conjuncts or disjuncts. E.g.,
 * <p>
 * a = 1 AND a = 1
 * <p>
 * rewrites to:
 * <p>
 * a = 1
 */
public class RemoveDuplicateConditions
        extends ExpressionRewriteRuleSet
{
    public RemoveDuplicateConditions()
    {
        super(createRewrite());
    }

    private static ExpressionRewriter createRewrite()
    {
        return (expression, context) -> ExpressionTreeRewriter.rewriteWith(new Visitor(), expression);
    }

    private static class Visitor
            extends io.trino.sql.ir.ExpressionRewriter<Void>
    {
        @Override
        public Expression rewriteLogical(Logical node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            return combinePredicates(node.operator(), extractPredicates(node));
        }
    }
}
