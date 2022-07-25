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

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.sql.tree.AtTimeZone;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.ExpressionRewriter;
import io.trino.sql.tree.ExpressionTreeRewriter;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.SymbolReference;

import java.util.Map;

import static io.trino.spi.type.TimeWithTimeZoneType.createTimeWithTimeZoneType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static java.util.Objects.requireNonNull;

public final class DesugarAtTimeZoneRewriter
{
    public static Expression rewrite(Expression expression, Map<NodeRef<Expression>, Type> expressionTypes, Metadata metadata, Session session)
    {
        return ExpressionTreeRewriter.rewriteWith(new Visitor(expressionTypes, metadata, session), expression);
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

        return rewrite(expression, expressionTypes, metadata, session);
    }

    private static class Visitor
            extends ExpressionRewriter<Void>
    {
        private final Map<NodeRef<Expression>, Type> expressionTypes;
        private final Metadata metadata;
        private final Session session;

        public Visitor(Map<NodeRef<Expression>, Type> expressionTypes, Metadata metadata, Session session)
        {
            this.expressionTypes = ImmutableMap.copyOf(requireNonNull(expressionTypes, "expressionTypes is null"));
            this.metadata = metadata;
            this.session = session;
        }

        @Override
        public Expression rewriteAtTimeZone(AtTimeZone node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            Type valueType = getType(node.getValue());
            Expression value = treeRewriter.rewrite(node.getValue(), context);

            Type timeZoneType = getType(node.getTimeZone());
            Expression timeZone = treeRewriter.rewrite(node.getTimeZone(), context);

            if (valueType instanceof TimeType) {
                return FunctionCallBuilder.resolve(session, metadata)
                        .setName(QualifiedName.of("$at_timezone"))
                        .addArgument(createTimeWithTimeZoneType(((TimeType) valueType).getPrecision()), new Cast(value, toSqlType(createTimeWithTimeZoneType(((TimeType) valueType).getPrecision()))))
                        .addArgument(getType(node.getTimeZone()), treeRewriter.rewrite(node.getTimeZone(), context))
                        .build();
            }

            if (valueType instanceof TimeWithTimeZoneType) {
                return FunctionCallBuilder.resolve(session, metadata)
                        .setName(QualifiedName.of("$at_timezone"))
                        .addArgument(valueType, value)
                        .addArgument(getType(node.getTimeZone()), treeRewriter.rewrite(node.getTimeZone(), context))
                        .build();
            }

            if (valueType instanceof TimestampType) {
                return FunctionCallBuilder.resolve(session, metadata)
                        .setName(QualifiedName.of("at_timezone"))
                        .addArgument(createTimestampWithTimeZoneType(((TimestampType) valueType).getPrecision()), new Cast(value, toSqlType(createTimestampWithTimeZoneType(((TimestampType) valueType).getPrecision()))))
                        .addArgument(timeZoneType, timeZone)
                        .build();
            }

            if (valueType instanceof TimestampWithTimeZoneType) {
                return FunctionCallBuilder.resolve(session, metadata)
                        .setName(QualifiedName.of("at_timezone"))
                        .addArgument(valueType, value)
                        .addArgument(timeZoneType, timeZone)
                        .build();
            }

            throw new IllegalArgumentException("Unexpected type: " + valueType);
        }

        private Type getType(Expression expression)
        {
            return expressionTypes.get(NodeRef.of(expression));
        }
    }
}
