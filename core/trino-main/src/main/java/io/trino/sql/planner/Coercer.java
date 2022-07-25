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
package io.trino.sql.planner;

import io.trino.spi.type.Type;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.ExpressionRewriter;
import io.trino.sql.tree.ExpressionTreeRewriter;
import io.trino.sql.tree.NodeRef;

import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static io.trino.type.UnknownType.UNKNOWN;

public final class Coercer
{
    private Coercer() {}

    public static Expression addCoercions(Expression expression, Analysis analysis)
    {
        return addCoercions(expression, analysis.getCoercions(), analysis.getTypeOnlyCoercions());
    }

    public static Expression addCoercions(Expression expression, Map<NodeRef<Expression>, Type> coercions, Set<NodeRef<Expression>> typeOnlyCoercions)
    {
        // ExpressionAnalyzer checks for explicit Cast to unknown. The check here is to prevent engine from adding such invalid coercion.
        checkArgument(coercions.values().stream().noneMatch(UNKNOWN::equals), "Cannot add coercion to UNKNOWN");
        return ExpressionTreeRewriter.rewriteWith(new Rewriter(coercions, typeOnlyCoercions), expression);
    }

    private static class Rewriter
            extends ExpressionRewriter<Void>
    {
        private final Map<NodeRef<Expression>, Type> coercions;
        private final Set<NodeRef<Expression>> typeOnlyCoercions;

        public Rewriter(Map<NodeRef<Expression>, Type> coercions, Set<NodeRef<Expression>> typeOnlyCoercions)
        {
            this.coercions = coercions;
            this.typeOnlyCoercions = typeOnlyCoercions;
        }

        @Override
        protected Expression rewriteExpression(Expression expression, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            Type target = coercions.get(NodeRef.of(expression));

            Expression rewritten = treeRewriter.defaultRewrite(expression, null);
            if (target != null) {
                rewritten = new Cast(
                        rewritten,
                        toSqlType(target),
                        false,
                        typeOnlyCoercions.contains(NodeRef.of(expression)));
            }

            return rewritten;
        }
    }
}
