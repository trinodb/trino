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
import io.prestosql.sql.tree.AtTimeZone;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.ExpressionRewriter;
import io.prestosql.sql.tree.ExpressionTreeRewriter;
import io.prestosql.sql.tree.NodeRef;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.SymbolReference;

import java.util.Map;

import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static java.util.Objects.requireNonNull;

public final class DesugarAtTimeZoneRewriter
{
    public static Expression rewrite(Expression expression, Map<NodeRef<Expression>, Type> expressionTypes, Metadata metadata)
    {
        return ExpressionTreeRewriter.rewriteWith(new Visitor(expressionTypes, metadata), expression);
    }

    private DesugarAtTimeZoneRewriter() {}

    public static Expression rewrite(Expression expression, Session session, Metadata metadata, TypeAnalyzer typeAnalyzer, SymbolAllocator symbolAllocator)
    {
        requireNonNull(metadata, "metadata is null");
        requireNonNull(typeAnalyzer, "typeAnalyzer is null");

        if (expression instanceof SymbolReference) {
            return expression;
        }
        Map<NodeRef<Expression>, Type> expressionTypes = typeAnalyzer.getTypes(session, symbolAllocator.getTypes(), expression);

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
        public Expression rewriteAtTimeZone(AtTimeZone node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            Type valueType = getType(node.getValue());
            Expression value = treeRewriter.rewrite(node.getValue(), context);

            if (valueType.equals(TIME)) {
                valueType = TIME_WITH_TIME_ZONE;
                value = new Cast(value, toSqlType(valueType));
            }
            else if (valueType.equals(TIMESTAMP)) {
                valueType = TIMESTAMP_WITH_TIME_ZONE;
                value = new Cast(value, toSqlType(TIMESTAMP_WITH_TIME_ZONE));
            }

            Type timeZoneType = getType(node.getTimeZone());
            Expression timeZone = treeRewriter.rewrite(node.getTimeZone(), context);

            return new FunctionCallBuilder(metadata)
                    .setName(QualifiedName.of("at_timezone"))
                    .addArgument(valueType, value)
                    .addArgument(timeZoneType, timeZone)
                    .build();
        }

        private Type getType(Expression expression)
        {
            return expressionTypes.get(NodeRef.of(expression));
        }
    }
}
