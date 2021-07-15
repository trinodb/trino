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

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.operator.scalar.TryFunction;
import io.trino.spi.type.Type;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.ExpressionRewriter;
import io.trino.sql.tree.ExpressionTreeRewriter;
import io.trino.sql.tree.LambdaExpression;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.SymbolReference;
import io.trino.sql.tree.TryExpression;
import io.trino.type.FunctionType;

import java.util.Map;

public final class DesugarTryExpressionRewriter
{
    private DesugarTryExpressionRewriter() {}

    public static Expression rewrite(Expression expression, Metadata metadata, TypeAnalyzer typeAnalyzer, Session session, SymbolAllocator symbolAllocator)
    {
        if (expression instanceof SymbolReference) {
            return expression;
        }

        Map<NodeRef<Expression>, Type> expressionTypes = typeAnalyzer.getTypes(
                session,
                symbolAllocator.getTypes(),
                expression);

        return ExpressionTreeRewriter.rewriteWith(new Visitor(metadata, expressionTypes), expression);
    }

    private static class Visitor
            extends ExpressionRewriter<Void>
    {
        private final Metadata metadata;
        private final Map<NodeRef<Expression>, Type> expressionTypes;

        public Visitor(Metadata metadata, Map<NodeRef<Expression>, Type> expressionTypes)
        {
            this.metadata = metadata;
            this.expressionTypes = expressionTypes;
        }

        @Override
        public Expression rewriteTryExpression(TryExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            Type type = expressionTypes.get(NodeRef.of(node));
            Expression expression = treeRewriter.rewrite(node.getInnerExpression(), context);

            return new FunctionCallBuilder(metadata)
                    .setName(QualifiedName.of(TryFunction.NAME))
                    .addArgument(new FunctionType(ImmutableList.of(), type), new LambdaExpression(ImmutableList.of(), expression))
                    .build();
        }
    }
}
