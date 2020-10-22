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
import io.airlift.slice.Slice;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.ResolvedFunction;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.Decimals;
import io.prestosql.spi.type.TimeType;
import io.prestosql.spi.type.TimeWithTimeZoneType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TimestampWithTimeZoneType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeNotFoundException;
import io.prestosql.sql.InterpretedFunctionInvoker;
import io.prestosql.sql.tree.AstVisitor;
import io.prestosql.sql.tree.BinaryLiteral;
import io.prestosql.sql.tree.BooleanLiteral;
import io.prestosql.sql.tree.CharLiteral;
import io.prestosql.sql.tree.DecimalLiteral;
import io.prestosql.sql.tree.DoubleLiteral;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.GenericLiteral;
import io.prestosql.sql.tree.IntervalLiteral;
import io.prestosql.sql.tree.Literal;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.NodeRef;
import io.prestosql.sql.tree.NullLiteral;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.StringLiteral;
import io.prestosql.sql.tree.TimeLiteral;
import io.prestosql.sql.tree.TimestampLiteral;

import java.util.Map;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.spi.StandardErrorCode.INVALID_LITERAL;
import static io.prestosql.spi.StandardErrorCode.TYPE_NOT_FOUND;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.sql.analyzer.SemanticExceptions.semanticException;
import static io.prestosql.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.prestosql.type.DateTimes.parseTime;
import static io.prestosql.type.DateTimes.parseTimeWithTimeZone;
import static io.prestosql.type.DateTimes.parseTimestamp;
import static io.prestosql.type.DateTimes.parseTimestampWithTimeZone;
import static io.prestosql.type.JsonType.JSON;
import static io.prestosql.util.DateTimeUtils.parseDayTimeInterval;
import static io.prestosql.util.DateTimeUtils.parseYearMonthInterval;
import static java.util.Objects.requireNonNull;

public final class LiteralInterpreter
{
    private LiteralInterpreter() {}

    public static Object evaluate(Metadata metadata, ConnectorSession session, Map<NodeRef<Expression>, Type> types, Expression node)
    {
        if (!(node instanceof Literal)) {
            throw new IllegalArgumentException("node must be a Literal");
        }
        return new LiteralVisitor(metadata, types).process(node, session);
    }

    private static class LiteralVisitor
            extends AstVisitor<Object, ConnectorSession>
    {
        private final Metadata metadata;
        private final InterpretedFunctionInvoker functionInvoker;
        private final Map<NodeRef<Expression>, Type> types;

        private LiteralVisitor(Metadata metadata, Map<NodeRef<Expression>, Type> types)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.functionInvoker = new InterpretedFunctionInvoker(metadata);
            this.types = requireNonNull(types, "types is null");
        }

        @Override
        protected Object visitLiteral(Literal node, ConnectorSession session)
        {
            throw new UnsupportedOperationException("Unhandled literal type: " + node);
        }

        @Override
        protected Object visitBooleanLiteral(BooleanLiteral node, ConnectorSession session)
        {
            return node.getValue();
        }

        @Override
        protected Long visitLongLiteral(LongLiteral node, ConnectorSession session)
        {
            return node.getValue();
        }

        @Override
        protected Double visitDoubleLiteral(DoubleLiteral node, ConnectorSession session)
        {
            return node.getValue();
        }

        @Override
        protected Object visitDecimalLiteral(DecimalLiteral node, ConnectorSession context)
        {
            return Decimals.parse(node.getValue()).getObject();
        }

        @Override
        protected Slice visitStringLiteral(StringLiteral node, ConnectorSession session)
        {
            return node.getSlice();
        }

        @Override
        protected Object visitCharLiteral(CharLiteral node, ConnectorSession context)
        {
            return node.getSlice();
        }

        @Override
        protected Slice visitBinaryLiteral(BinaryLiteral node, ConnectorSession session)
        {
            return node.getValue();
        }

        @Override
        protected Object visitGenericLiteral(GenericLiteral node, ConnectorSession session)
        {
            Type type;
            try {
                type = metadata.fromSqlType(node.getType());
            }
            catch (TypeNotFoundException e) {
                throw semanticException(TYPE_NOT_FOUND, node, "Unknown type: %s", node.getType());
            }

            if (JSON.equals(type)) {
                ResolvedFunction resolvedFunction = metadata.resolveFunction(QualifiedName.of("json_parse"), fromTypes(VARCHAR));
                return functionInvoker.invoke(resolvedFunction, session, ImmutableList.of(utf8Slice(node.getValue())));
            }

            try {
                ResolvedFunction resolvedFunction = metadata.getCoercion(VARCHAR, type);
                return functionInvoker.invoke(resolvedFunction, session, ImmutableList.of(utf8Slice(node.getValue())));
            }
            catch (IllegalArgumentException e) {
                throw semanticException(INVALID_LITERAL, node, "No literal form for type %s", type);
            }
        }

        @Override
        protected Object visitTimeLiteral(TimeLiteral node, ConnectorSession session)
        {
            Type type = types.get(NodeRef.of(node));

            if (type instanceof TimeType) {
                return parseTime(node.getValue());
            }
            else if (type instanceof TimeWithTimeZoneType) {
                return parseTimeWithTimeZone(((TimeWithTimeZoneType) type).getPrecision(), node.getValue());
            }

            throw new IllegalStateException("Unexpected type: " + type);
        }

        @Override
        protected Object visitTimestampLiteral(TimestampLiteral node, ConnectorSession session)
        {
            Type type = types.get(NodeRef.of(node));

            if (type instanceof TimestampType) {
                int precision = ((TimestampType) type).getPrecision();
                return parseTimestamp(precision, node.getValue());
            }
            else if (type instanceof TimestampWithTimeZoneType) {
                int precision = ((TimestampWithTimeZoneType) type).getPrecision();
                return parseTimestampWithTimeZone(precision, node.getValue());
            }

            throw new IllegalStateException("Unexpected type: " + type);
        }

        @Override
        protected Long visitIntervalLiteral(IntervalLiteral node, ConnectorSession session)
        {
            if (node.isYearToMonth()) {
                return node.getSign().multiplier() * parseYearMonthInterval(node.getValue(), node.getStartField(), node.getEndField());
            }
            else {
                return node.getSign().multiplier() * parseDayTimeInterval(node.getValue(), node.getStartField(), node.getEndField());
            }
        }

        @Override
        protected Object visitNullLiteral(NullLiteral node, ConnectorSession session)
        {
            return null;
        }
    }
}
