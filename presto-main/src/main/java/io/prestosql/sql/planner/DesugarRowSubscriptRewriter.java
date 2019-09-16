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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.DereferenceExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.ExpressionRewriter;
import io.prestosql.sql.tree.ExpressionTreeRewriter;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.NodeRef;
import io.prestosql.sql.tree.SubscriptExpression;

import java.util.Map;
import java.util.Optional;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * Replaces subscript expression on Row
 * with cast and dereference:
 * <pre>
 *     ROW (1, 'a', 2) [2]
 * </pre>
 * is transformed into:
 * <pre>
 *     (CAST (ROW (1, 'a', 2) AS ROW (field_0 bigint, field_1 varchar(1), field_2 bigint))).field_1
 * </pre>
 */
public final class DesugarRowSubscriptRewriter
{
    private DesugarRowSubscriptRewriter() {}

    public static Expression rewrite(Expression expression, Session session, TypeAnalyzer typeAnalyzer, SymbolAllocator symbolAllocator)
    {
        requireNonNull(typeAnalyzer, "typeAnalyzer is null");
        Map<NodeRef<Expression>, Type> expressionTypes = typeAnalyzer.getTypes(session, symbolAllocator.getTypes(), expression);
        return rewrite(expression, expressionTypes);
    }

    public static Expression rewrite(Expression expression, Map<NodeRef<Expression>, Type> expressionTypes)
    {
        return ExpressionTreeRewriter.rewriteWith(new Visitor(expressionTypes), expression);
    }

    private static class Visitor
            extends ExpressionRewriter<Void>
    {
        private final Map<NodeRef<Expression>, Type> expressionTypes;

        public Visitor(Map<NodeRef<Expression>, Type> expressionTypes)
        {
            this.expressionTypes = ImmutableMap.copyOf(requireNonNull(expressionTypes, "expressionTypes is null"));
        }

        @Override
        public Expression rewriteSubscriptExpression(SubscriptExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            Expression base = node.getBase();
            Expression index = node.getIndex();

            Expression result = node;

            Type type = getType(base);
            if (type instanceof RowType) {
                RowType rowType = (RowType) type;
                int position = toIntExact(((LongLiteral) index).getValue() - 1);

                Optional<String> fieldName = rowType.getFields().get(position).getName();

                // Do not cast if Row fields are named
                if (fieldName.isPresent()) {
                    result = new DereferenceExpression(base, new Identifier(fieldName.get()));
                }
                else {
                    // Cast to Row with named fields
                    ImmutableList.Builder<RowType.Field> namedFields = new ImmutableList.Builder<>();
                    for (int i = 0; i < rowType.getFields().size(); i++) {
                        namedFields.add(new RowType.Field(Optional.of("f" + i), rowType.getTypeParameters().get(i)));
                    }
                    RowType namedRowType = RowType.from(namedFields.build());
                    Cast cast = new Cast(base, namedRowType.getTypeSignature().toString());
                    result = new DereferenceExpression(cast, new Identifier("f" + position));
                }
            }

            return treeRewriter.defaultRewrite(result, context);
        }

        private Type getType(Expression expression)
        {
            return expressionTypes.get(NodeRef.of(expression));
        }
    }
}
