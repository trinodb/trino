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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.Session;
import io.trino.collect.cache.CacheUtils;
import io.trino.metadata.ResolvedFunction;
import io.trino.operator.scalar.VarbinaryFunctions;
import io.trino.operator.scalar.timestamp.TimestampToVarcharCast;
import io.trino.operator.scalar.timestamptz.TimestampWithTimeZoneToVarcharCast;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.SqlDate;
import io.trino.spi.type.SqlVarbinary;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import io.trino.sql.InterpretedFunctionInvoker;
import io.trino.sql.PlannerContext;
import io.trino.sql.relational.ConstantExpression;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.BinaryLiteral;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.CharLiteral;
import io.trino.sql.tree.DecimalLiteral;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.IntervalLiteral;
import io.trino.sql.tree.Literal;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.TimeLiteral;
import io.trino.sql.tree.TimestampLiteral;
import io.trino.type.IntervalDayTimeType;
import io.trino.type.IntervalYearMonthType;
import io.trino.type.SqlIntervalDayTime;
import io.trino.type.SqlIntervalYearMonth;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.function.Function;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.collect.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.spi.StandardErrorCode.INVALID_LITERAL;
import static io.trino.spi.type.Decimals.isShortDecimal;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.type.DateTimes.parseTime;
import static io.trino.type.DateTimes.parseTimeWithTimeZone;
import static io.trino.type.DateTimes.parseTimestamp;
import static io.trino.type.DateTimes.parseTimestampWithTimeZone;
import static io.trino.type.JsonType.JSON;
import static io.trino.util.DateTimeUtils.parseDayTimeInterval;
import static io.trino.util.DateTimeUtils.parseYearMonthInterval;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public final class LiteralInterpreter
{
    private final PlannerContext plannerContext;
    private final Session session;
    private final ConnectorSession connectorSession;
    private final InterpretedFunctionInvoker functionInvoker;

    private final Cache<Type, Function<GenericLiteral, Object>> genericLiteralEvaluatorCache = buildNonEvictableCache(CacheBuilder.newBuilder().maximumSize(1000));

    public LiteralInterpreter(PlannerContext plannerContext, Session session)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.session = requireNonNull(session, "session is null");
        this.connectorSession = session.toConnectorSession();
        this.functionInvoker = new InterpretedFunctionInvoker(plannerContext.getFunctionManager());
    }

    public Object evaluate(Expression node, Type type)
    {
        if (!(node instanceof Literal)) {
            throw new IllegalArgumentException("node must be a Literal");
        }
        return new LiteralVisitor(type).process(node, null);
    }

    public static Object evaluate(ConstantExpression node)
    {
        Type type = node.getType();

        if (node.getValue() == null) {
            return null;
        }
        if (type instanceof BooleanType) {
            return node.getValue();
        }
        if (type instanceof BigintType || type instanceof TinyintType || type instanceof SmallintType || type instanceof IntegerType) {
            return node.getValue();
        }
        if (type instanceof DoubleType) {
            return node.getValue();
        }
        if (type instanceof RealType) {
            Long number = (Long) node.getValue();
            return intBitsToFloat(number.intValue());
        }
        if (type instanceof DecimalType) {
            if (isShortDecimal(type)) {
                return Decimals.toString((long) node.getValue(), ((DecimalType) type).getScale());
            }
            else {
                return Decimals.toString((Int128) node.getValue(), ((DecimalType) type).getScale());
            }
        }
        if (type instanceof VarcharType || type instanceof CharType) {
            return ((Slice) node.getValue()).toStringUtf8();
        }
        if (type instanceof VarbinaryType) {
            return new SqlVarbinary(((Slice) node.getValue()).getBytes());
        }
        if (type instanceof DateType) {
            return new SqlDate(toIntExact((Long) node.getValue())).toString();
        }
        if (type instanceof TimestampType) {
            TimestampType timestampType = (TimestampType) type;
            if (timestampType.isShort()) {
                return TimestampToVarcharCast.cast(timestampType.getPrecision(), (Long) node.getValue()).toStringUtf8();
            }
            else {
                return TimestampToVarcharCast.cast(timestampType.getPrecision(), (LongTimestamp) node.getValue()).toStringUtf8();
            }
        }

        if (type instanceof TimestampWithTimeZoneType) {
            TimestampWithTimeZoneType timestampWithTimeZoneType = (TimestampWithTimeZoneType) type;
            String representation;
            if (timestampWithTimeZoneType.isShort()) {
                representation = TimestampWithTimeZoneToVarcharCast.cast(timestampWithTimeZoneType.getPrecision(), (long) node.getValue()).toStringUtf8();
            }
            else {
                representation = TimestampWithTimeZoneToVarcharCast.cast(timestampWithTimeZoneType.getPrecision(), (LongTimestampWithTimeZone) node.getValue()).toStringUtf8();
            }
            if (!node.getValue().equals(parseTimestampWithTimeZone(timestampWithTimeZoneType.getPrecision(), representation))) {
                // Certain (point in time, time zone) pairs cannot be represented as a TIMESTAMP literal, as the literal uses local date/time in given time zone.
                // Thus, during DST backwards change by e.g. 1 hour, the local time is "repeated" twice and thus one local date/time logically corresponds to two
                // points in time, leaving one of them non-referencable.
                // TODO (https://github.com/trinodb/trino/issues/5781) consider treating such values as illegal
            }
            else {
                return representation;
            }
        }

        if (type instanceof IntervalDayTimeType) {
            return new SqlIntervalDayTime((long) node.getValue());
        }
        if (type instanceof IntervalYearMonthType) {
            return new SqlIntervalYearMonth(((Long) node.getValue()).intValue());
        }

        if (type.getJavaType().equals(Slice.class)) {
            // DO NOT ever remove toBase64. Calling toString directly on Slice whose base is not byte[] will cause JVM to crash.
            return "'" + VarbinaryFunctions.toBase64((Slice) node.getValue()).toStringUtf8() + "'";
        }

        // We should not fail at the moment; just return the raw value (block, regex, etc) to the user
        return node.getValue();
    }

    private class LiteralVisitor
            extends AstVisitor<Object, Void>
    {
        private final Type type;

        private LiteralVisitor(Type type)
        {
            this.type = requireNonNull(type, "type is null");
        }

        @Override
        protected Object visitLiteral(Literal node, Void context)
        {
            throw new UnsupportedOperationException("Unhandled literal type: " + node);
        }

        @Override
        protected Object visitBooleanLiteral(BooleanLiteral node, Void context)
        {
            return node.getValue();
        }

        @Override
        protected Long visitLongLiteral(LongLiteral node, Void context)
        {
            return node.getValue();
        }

        @Override
        protected Double visitDoubleLiteral(DoubleLiteral node, Void context)
        {
            return node.getValue();
        }

        @Override
        protected Object visitDecimalLiteral(DecimalLiteral node, Void context)
        {
            return Decimals.parse(node.getValue()).getObject();
        }

        @Override
        protected Slice visitStringLiteral(StringLiteral node, Void context)
        {
            return Slices.utf8Slice(node.getValue());
        }

        @Override
        protected Object visitCharLiteral(CharLiteral node, Void context)
        {
            return Slices.utf8Slice(node.getValue());
        }

        @Override
        protected Slice visitBinaryLiteral(BinaryLiteral node, Void context)
        {
            return Slices.wrappedBuffer(node.getValue());
        }

        @Override
        protected Object visitGenericLiteral(GenericLiteral node, Void context)
        {
            Function<GenericLiteral, Object> evaluator = CacheUtils.uncheckedCacheGet(genericLiteralEvaluatorCache, type, () -> {
                boolean isJson = JSON.equals(type);
                ResolvedFunction resolvedFunction;
                if (isJson) {
                    resolvedFunction = plannerContext.getMetadata().resolveFunction(session, QualifiedName.of("json_parse"), fromTypes(VARCHAR));
                }
                else {
                    resolvedFunction = plannerContext.getMetadata().getCoercion(session, VARCHAR, type);
                }
                return evaluatedNode -> {
                    try {
                        return functionInvoker.invoke(resolvedFunction, connectorSession, ImmutableList.of(utf8Slice(evaluatedNode.getValue())));
                    }
                    catch (IllegalArgumentException e) {
                        if (isJson) {
                            // TODO it may be not correct to propagate this exceptiion as-is in JSON case
                            throw e;
                        }
                        throw semanticException(INVALID_LITERAL, evaluatedNode, "No literal form for type %s", type);
                    }
                };
            });
            return evaluator.apply(node);
        }

        @Override
        protected Object visitTimeLiteral(TimeLiteral node, Void session)
        {
            if (type instanceof TimeType) {
                return parseTime(node.getValue());
            }
            if (type instanceof TimeWithTimeZoneType) {
                return parseTimeWithTimeZone(((TimeWithTimeZoneType) type).getPrecision(), node.getValue());
            }

            throw new IllegalStateException("Unexpected type: " + type);
        }

        @Override
        protected Object visitTimestampLiteral(TimestampLiteral node, Void session)
        {
            if (type instanceof TimestampType) {
                int precision = ((TimestampType) type).getPrecision();
                return parseTimestamp(precision, node.getValue());
            }
            if (type instanceof TimestampWithTimeZoneType) {
                int precision = ((TimestampWithTimeZoneType) type).getPrecision();
                return parseTimestampWithTimeZone(precision, node.getValue());
            }

            throw new IllegalStateException("Unexpected type: " + type);
        }

        @Override
        protected Long visitIntervalLiteral(IntervalLiteral node, Void context)
        {
            if (node.isYearToMonth()) {
                return node.getSign().multiplier() * parseYearMonthInterval(node.getValue(), node.getStartField(), node.getEndField());
            }
            return node.getSign().multiplier() * parseDayTimeInterval(node.getValue(), node.getStartField(), node.getEndField());
        }

        @Override
        protected Object visitNullLiteral(NullLiteral node, Void context)
        {
            return null;
        }
    }

    private static Number decodeDecimal(BigInteger unscaledValue, DecimalType type)
    {
        return new BigDecimal(unscaledValue, type.getScale(), new MathContext(type.getPrecision()));
    }
}
