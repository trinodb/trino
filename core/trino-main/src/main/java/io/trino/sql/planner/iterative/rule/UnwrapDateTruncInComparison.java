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

import com.google.common.base.Enums;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.Session;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.InterpretedFunctionInvoker;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.ExpressionInterpreter;
import io.trino.sql.planner.LiteralEncoder;
import io.trino.sql.planner.NoOpSymbolResolver;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.tree.BetweenPredicate;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.ExpressionTreeRewriter;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.IsNotNullPredicate;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.NullLiteral;

import java.lang.invoke.MethodHandle;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.metadata.ResolvedFunction.extractFunctionName;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.sql.ExpressionUtils.isEffectivelyLiteral;
import static io.trino.sql.ExpressionUtils.or;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static io.trino.sql.planner.iterative.rule.UnwrapCastInComparison.falseIfNotNull;
import static io.trino.sql.planner.iterative.rule.UnwrapCastInComparison.trueIfNotNull;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.type.DateTimes.PICOSECONDS_PER_MICROSECOND;
import static io.trino.type.DateTimes.scaleFactor;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * Given constant temporal unit U and a constant date/time expression t that's rounded to unit,
 * converts expression of the form
 * <pre>
 *     date_trunc(unit, date_time) = t
 * </pre>
 * <p>
 * into
 * <pre>
 *     date_time BETWEEN t AND (t + unit)
 * </pre>
 * <p>
 * It also applies to comparison operators other than equality and detects expressions that
 * are known to be true or false, e.g. {@code date_trunc('month', ...) = DATE '2005-09-10'}
 * is known to be false.
 *
 * @see UnwrapCastInComparison
 */
public class UnwrapDateTruncInComparison
        extends ExpressionRewriteRuleSet
{
    public UnwrapDateTruncInComparison(PlannerContext plannerContext, TypeAnalyzer typeAnalyzer)
    {
        super(createRewrite(plannerContext, typeAnalyzer));
    }

    private static ExpressionRewriter createRewrite(PlannerContext plannerContext, TypeAnalyzer typeAnalyzer)
    {
        requireNonNull(plannerContext, "plannerContext is null");
        requireNonNull(typeAnalyzer, "typeAnalyzer is null");

        return (expression, context) -> unwrapDateTrunc(context.getSession(), plannerContext, typeAnalyzer, context.getSymbolAllocator().getTypes(), expression);
    }

    private static Expression unwrapDateTrunc(Session session,
            PlannerContext plannerContext,
            TypeAnalyzer typeAnalyzer,
            TypeProvider types,
            Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new Visitor(plannerContext, typeAnalyzer, session, types), expression);
    }

    private static class Visitor
            extends io.trino.sql.tree.ExpressionRewriter<Void>
    {
        private final PlannerContext plannerContext;
        private final TypeAnalyzer typeAnalyzer;
        private final Session session;
        private final TypeProvider types;
        private final InterpretedFunctionInvoker functionInvoker;
        private final LiteralEncoder literalEncoder;

        public Visitor(PlannerContext plannerContext, TypeAnalyzer typeAnalyzer, Session session, TypeProvider types)
        {
            this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
            this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
            this.session = requireNonNull(session, "session is null");
            this.types = requireNonNull(types, "types is null");
            this.functionInvoker = new InterpretedFunctionInvoker(plannerContext.getFunctionManager());
            this.literalEncoder = new LiteralEncoder(plannerContext);
        }

        @Override
        public Expression rewriteComparisonExpression(ComparisonExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            ComparisonExpression expression = (ComparisonExpression) treeRewriter.defaultRewrite((Expression) node, null);
            return unwrapDateTrunc(expression);
        }

        // Simplify `date_trunc(unit, d) ? value`
        private Expression unwrapDateTrunc(ComparisonExpression expression)
        {
            // Expect date_trunc on the left side and value on the right side of the comparison.
            // This is provided by CanonicalizeExpressionRewriter.

            if (!(expression.getLeft() instanceof FunctionCall call) ||
                    !extractFunctionName(call.getName()).equals("date_trunc") ||
                    call.getArguments().size() != 2) {
                return expression;
            }

            Map<NodeRef<Expression>, Type> expressionTypes = typeAnalyzer.getTypes(session, types, expression);
            Expression unitExpression = call.getArguments().get(0);
            if (!(expressionTypes.get(NodeRef.of(unitExpression)) instanceof VarcharType) || !isEffectivelyLiteral(plannerContext, session, unitExpression)) {
                return expression;
            }
            Slice unitName = (Slice) new ExpressionInterpreter(unitExpression, plannerContext, session, expressionTypes)
                    .optimize(NoOpSymbolResolver.INSTANCE);
            if (unitName == null) {
                return expression;
            }

            Expression argument = call.getArguments().get(1);
            Type argumentType = expressionTypes.get(NodeRef.of(argument));

            Type rightType = expressionTypes.get(NodeRef.of(expression.getRight()));
            verify(argumentType.equals(rightType), "Mismatched types: %s and %s", argumentType, rightType);

            Object right = new ExpressionInterpreter(expression.getRight(), plannerContext, session, expressionTypes)
                    .optimize(NoOpSymbolResolver.INSTANCE);

            if (right == null || right instanceof NullLiteral) {
                return switch (expression.getOperator()) {
                    case EQUAL, NOT_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL -> new Cast(new NullLiteral(), toSqlType(BOOLEAN));
                    case IS_DISTINCT_FROM -> new IsNotNullPredicate(argument);
                };
            }

            if (right instanceof Expression) {
                return expression;
            }
            if (rightType instanceof TimestampWithTimeZoneType) {
                // Cannot replace with a range due to how date_trunc operates on value's local date/time.
                // I.e. unwrapping is possible only when values are all of some fixed zone and the zone is known.
                return expression;
            }

            ResolvedFunction resolvedFunction = plannerContext.getMetadata().decodeFunction(call.getName());

            Optional<SupportedUnit> unitIfSupported = Enums.getIfPresent(SupportedUnit.class, unitName.toStringUtf8().toUpperCase(Locale.ENGLISH)).toJavaUtil();
            if (unitIfSupported.isEmpty()) {
                return expression;
            }
            SupportedUnit unit = unitIfSupported.get();
            if (rightType == DATE && (unit == SupportedUnit.DAY || unit == SupportedUnit.HOUR)) {
                // DAY case handled by CanonicalizeExpressionRewriter, other is illegal, will fail
                return expression;
            }

            Object rangeLow = functionInvoker.invoke(resolvedFunction, session.toConnectorSession(), ImmutableList.of(unitName, right));
            int compare = compare(rightType, rangeLow, right);
            verify(compare <= 0, "Truncation of %s value %s resulted in a bigger value %s", rightType, right, rangeLow);
            boolean rightValueAtRangeLow = compare == 0;

            return switch (expression.getOperator()) {
                case EQUAL -> {
                    if (!rightValueAtRangeLow) {
                        yield falseIfNotNull(argument);
                    }
                    yield between(argument, rightType, rangeLow, calculateRangeEndInclusive(rangeLow, rightType, unit));
                }
                case NOT_EQUAL -> {
                    if (!rightValueAtRangeLow) {
                        yield trueIfNotNull(argument);
                    }
                    yield new NotExpression(between(argument, rightType, rangeLow, calculateRangeEndInclusive(rangeLow, rightType, unit)));
                }
                case IS_DISTINCT_FROM -> {
                    if (!rightValueAtRangeLow) {
                        yield TRUE_LITERAL;
                    }
                    yield or(
                            new IsNullPredicate(argument),
                            new NotExpression(between(argument, rightType, rangeLow, calculateRangeEndInclusive(rangeLow, rightType, unit))));
                }
                case LESS_THAN -> {
                    if (rightValueAtRangeLow) {
                        yield new ComparisonExpression(LESS_THAN, argument, toExpression(rangeLow, rightType));
                    }
                    yield new ComparisonExpression(LESS_THAN_OR_EQUAL, argument, toExpression(calculateRangeEndInclusive(rangeLow, rightType, unit), rightType));
                }
                case LESS_THAN_OR_EQUAL -> new ComparisonExpression(LESS_THAN_OR_EQUAL, argument, toExpression(calculateRangeEndInclusive(rangeLow, rightType, unit), rightType));
                case GREATER_THAN -> new ComparisonExpression(GREATER_THAN, argument, toExpression(calculateRangeEndInclusive(rangeLow, rightType, unit), rightType));
                case GREATER_THAN_OR_EQUAL -> {
                    if (rightValueAtRangeLow) {
                        yield new ComparisonExpression(GREATER_THAN_OR_EQUAL, argument, toExpression(rangeLow, rightType));
                    }
                    yield new ComparisonExpression(GREATER_THAN, argument, toExpression(calculateRangeEndInclusive(rangeLow, rightType, unit), rightType));
                }
            };
        }

        private Object calculateRangeEndInclusive(Object rangeStart, Type type, SupportedUnit rangeUnit)
        {
            if (type == DATE) {
                LocalDate date = LocalDate.ofEpochDay((long) rangeStart);
                LocalDate endExclusive = switch (rangeUnit) {
                    case HOUR, DAY -> throw new UnsupportedOperationException("Unsupported type and unit: %s, %s".formatted(type, rangeUnit));
                    case MONTH -> date.plusMonths(1);
                    case YEAR -> date.plusYears(1);
                };
                return endExclusive.toEpochDay() - 1;
            }
            if (type instanceof TimestampType timestampType) {
                if (timestampType.isShort()) {
                    long epochMicros = (long) rangeStart;
                    long epochSecond = floorDiv(epochMicros, MICROSECONDS_PER_SECOND);
                    int microOfSecond = floorMod(epochMicros, MICROSECONDS_PER_SECOND);
                    verify(microOfSecond == 0, "Unexpected micros, value should be rounded to %s: %s", rangeUnit, microOfSecond);
                    LocalDateTime dateTime = LocalDateTime.ofEpochSecond(epochSecond, 0, ZoneOffset.UTC);
                    LocalDateTime endExclusive = switch (rangeUnit) {
                        case HOUR -> dateTime.plusHours(1);
                        case DAY -> dateTime.plusDays(1);
                        case MONTH -> dateTime.plusMonths(1);
                        case YEAR -> dateTime.plusYears(1);
                    };
                    verify(endExclusive.getNano() == 0, "Unexpected nanos in %s, value not rounded to %s", endExclusive, rangeUnit);
                    long endExclusiveMicros = endExclusive.toEpochSecond(ZoneOffset.UTC) * MICROSECONDS_PER_SECOND;
                    return endExclusiveMicros - scaleFactor(timestampType.getPrecision(), 6);
                }
                LongTimestamp longTimestamp = (LongTimestamp) rangeStart;
                verify(longTimestamp.getPicosOfMicro() == 0, "Unexpected picos in %s, value not rounded to %s", rangeStart, rangeUnit);
                long endInclusiveMicros = (long) calculateRangeEndInclusive(longTimestamp.getEpochMicros(), createTimestampType(6), rangeUnit);
                return new LongTimestamp(endInclusiveMicros, toIntExact(PICOSECONDS_PER_MICROSECOND - scaleFactor(timestampType.getPrecision(), 12)));
            }
            throw new UnsupportedOperationException("Unsupported type: " + type);
        }

        private BetweenPredicate between(Expression argument, Type type, Object minInclusive, Object maxInclusive)
        {
            return new BetweenPredicate(
                    argument,
                    toExpression(minInclusive, type),
                    toExpression(maxInclusive, type));
        }

        private Expression toExpression(Object value, Type type)
        {
            return literalEncoder.toExpression(session, value, type);
        }

        private int compare(Type type, Object first, Object second)
        {
            requireNonNull(first, "first is null");
            requireNonNull(second, "second is null");
            // choice of placing unordered values first or last does not matter for this code
            MethodHandle comparisonOperator = plannerContext.getTypeOperators().getComparisonUnorderedLastOperator(type, InvocationConvention.simpleConvention(FAIL_ON_NULL, NEVER_NULL, NEVER_NULL));
            try {
                return toIntExact((long) comparisonOperator.invoke(first, second));
            }
            catch (Throwable throwable) {
                Throwables.throwIfUnchecked(throwable);
                throw new TrinoException(GENERIC_INTERNAL_ERROR, throwable);
            }
        }
    }

    private enum SupportedUnit
    {
        HOUR,
        DAY,
        MONTH,
        YEAR,
    }
}
