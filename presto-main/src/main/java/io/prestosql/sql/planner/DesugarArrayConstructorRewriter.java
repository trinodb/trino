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
package io.prestosql.sql.planner;

import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.tree.ArrayConstructor;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.ExpressionRewriter;
import io.prestosql.sql.tree.ExpressionTreeRewriter;
import io.prestosql.sql.tree.NodeRef;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.SymbolReference;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public final class DesugarArrayConstructorRewriter
{
    public static Expression rewrite(Expression expression, Map<NodeRef<Expression>, Type> expressionTypes, Metadata metadata)
    {
        return ExpressionTreeRewriter.rewriteWith(new Visitor(expressionTypes, metadata), expression);
    }

    private DesugarArrayConstructorRewriter() {}

    public static Expression rewrite(Expression expression, Session session, Metadata metadata, TypeAnalyzer typeAnalyzer, TypeProvider typeProvider)
    {
        requireNonNull(metadata, "metadata is null");
        requireNonNull(typeAnalyzer, "typeAnalyzer is null");

        if (expression instanceof SymbolReference) {
            return expression;
        }
        Map<NodeRef<Expression>, Type> expressionTypes = typeAnalyzer.getTypes(session, typeProvider, expression);

        return rewrite(expression, expressionTypes, metadata);
    }

    private static class Visitor
            extends ExpressionRewriter<Void>
    {
        private final Map<NodeRef<Expression>, Type> expressionTypes;
        private final Metadata metadata;

        public Visitor(Map<NodeRef<Expression>, Type> expressionTypes, Metadata metadata)
        {
            this.expressionTypes = ImmutableMap.copyOf(requireNonNull(expressionTypes, "expressionTypes is null"));
            this.metadata = metadata;
        }

        @Override
        public Expression rewriteArrayConstructor(ArrayConstructor node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            ArrayConstructor rewritten = treeRewriter.defaultRewrite(node, context);
            return new FunctionCallBuilder(metadata)
                    .setName(QualifiedName.of(ArrayConstructor.ARRAY_CONSTRUCTOR))
                    .setArguments(getTypes(node.getValues()), rewritten.getValues())
                    .build();
        }

        private List<Type> getTypes(List<Expression> expressions)
        {
            return expressions.stream()
                    .map(NodeRef::of)
                    .map(expressionTypes::get)
                    .collect(toImmutableList());
        }
    }
}
