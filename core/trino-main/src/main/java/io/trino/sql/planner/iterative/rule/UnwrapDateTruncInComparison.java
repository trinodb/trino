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
import com.google.common.collect.ImmutableMap;
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
import io.trino.sql.ir.Call;
import io.trino.sql.ir.ComparisonOperator;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.ExpressionTreeRewriter;
import io.trino.sql.ir.IrExpressions.Comparison;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.optimizer.IrExpressionEvaluator;
import io.trino.sql.ir.optimizer.IrExpressionOptimizer;
import io.trino.sql.planner.SymbolAllocator;

import java.lang.invoke.MethodHandle;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Locale;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.IrExpressions.between;
import static io.trino.sql.ir.IrExpressions.comparison;
import static io.trino.sql.ir.IrExpressions.matchComparison;
import static io.trino.sql.ir.IrExpressions.not;
import static io.trino.sql.ir.IrUtils.or;
import static io.trino.sql.ir.Logical.and;
import static io.trino.sql.planner.iterative.rule.UnwrapCastInComparison.falseIfNotNull;
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
    public UnwrapDateTruncInComparison(PlannerContext plannerContext)
    {
        super(createRewrite(plannerContext));
    }

    private static ExpressionRewriter createRewrite(PlannerContext plannerContext)
    {
        requireNonNull(plannerContext, "plannerContext is null");

        return (expression, context) -> unwrapDateTrunc(context.getSession(), plannerContext, context.getSymbolAllocator(), expression);
    }

    private static Expression unwrapDateTrunc(
            Session session,
            PlannerContext plannerContext,
            SymbolAllocator symbolAllocator,
            Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new Visitor(plannerContext, session, symbolAllocator), expression);
    }

    private static class Visitor
            extends io.trino.sql.ir.ExpressionRewriter<Void>
    {
        private final PlannerContext plannerContext;
        private final Session session;
        private final SymbolAllocator symbolAllocator;
        private final InterpretedFunctionInvoker functionInvoker;
        private final IrExpressionEvaluator evaluator;
        private final IrExpressionOptimizer optimizer;

        public Visitor(PlannerContext plannerContext, Session session, SymbolAllocator symbolAllocator)
        {
            this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
            this.session = requireNonNull(session, "session is null");
            this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
            this.functionInvoker = new InterpretedFunctionInvoker(plannerContext.getFunctionManager());
            evaluator = plannerContext.getExpressionEvaluator();
            optimizer = plannerContext.getExpressionOptimizer();
        }

        @Override
        public Expression rewriteCall(Call node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            Call expression = treeRewriter.defaultRewrite(node, null);
            if (!(matchComparison(expression) instanceof Comparison comparison)) {
                return expression;
            }
            // The unwrap logic expects date_trunc(...) on the left. A comparison whose date_trunc operand is
            // on the right (e.g. the canonical form of date_trunc(...) > literal, which is literal <
            // date_trunc(...)) is flipped so the date_trunc call lands on the left; the rebuilt comparison is
            // canonicalized again on the way out.
            if (isDateTruncCall(comparison.right()) && !isDateTruncCall(comparison.left())) {
                return unwrapDateTrunc(comparison.operator().flip(), comparison.right(), comparison.left());
            }
            return unwrapDateTrunc(comparison.operator(), comparison.left(), comparison.right());
        }

        private static boolean isDateTruncCall(Expression expression)
        {
            return expression instanceof Call call &&
                    call.function().name().equals(builtinFunctionName("date_trunc")) &&
                    call.arguments().size() == 2;
        }

        // Simplify `date_trunc(unit, d) ? value`
        private Expression unwrapDateTrunc(ComparisonOperator operator, Expression left, Expression originalRight)
        {
            // Expect date_trunc on the left side and value on the right side of the comparison.
            // This is provided by CanonicalizeExpressionRewriter.

            if (!(left instanceof Call call) ||
                    !call.function().name().equals(builtinFunctionName("date_trunc")) ||
                    call.arguments().size() != 2) {
                return comparison(plannerContext.getMetadata(), operator, left, originalRight);
            }

            Expression unitExpression = call.arguments().get(0);
            if (!(unitExpression.type() instanceof VarcharType) || !(unitExpression instanceof Constant)) {
                return comparison(plannerContext.getMetadata(), operator, left, originalRight);
            }
            Slice unitName = (Slice) evaluator.evaluate(unitExpression, session, ImmutableMap.of());
            if (unitName == null) {
                return comparison(plannerContext.getMetadata(), operator, left, originalRight);
            }

            Expression argument = call.arguments().get(1);
            Expression right = optimizer.process(originalRight, session, symbolAllocator, ImmutableMap.of()).orElse(originalRight);

            if (right instanceof Constant constant && constant.value() == null) {
                return switch (operator) {
                    case EQUAL, NOT_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL -> new Constant(BOOLEAN, null);
                    case IDENTICAL -> new IsNull(argument);
                };
            }

            if (!(right instanceof Constant(Type rightType, Object rightValue))) {
                return comparison(plannerContext.getMetadata(), operator, left, originalRight);
            }
            if (rightType instanceof TimestampWithTimeZoneType) {
                // Cannot replace with a range due to how date_trunc operates on value's local date/time.
                // I.e. unwrapping is possible only when values are all of some fixed zone and the zone is known.
                return comparison(plannerContext.getMetadata(), operator, left, originalRight);
            }

            ResolvedFunction resolvedFunction = call.function();

            Optional<SupportedUnit> unitIfSupported = Enums.getIfPresent(SupportedUnit.class, unitName.toStringUtf8().toUpperCase(Locale.ENGLISH)).toJavaUtil();
            if (unitIfSupported.isEmpty()) {
                return comparison(plannerContext.getMetadata(), operator, left, originalRight);
            }
            SupportedUnit unit = unitIfSupported.get();
            if (rightType == DATE && (unit == SupportedUnit.DAY || unit == SupportedUnit.HOUR)) {
                // DAY case handled by CanonicalizeExpressionRewriter, other is illegal, will fail
                return comparison(plannerContext.getMetadata(), operator, left, originalRight);
            }

            Object rangeLow = functionInvoker.invoke(resolvedFunction, session.toConnectorSession(), ImmutableList.of(unitName, rightValue));
            int compare = compare(rightType, rangeLow, rightValue);
            verify(compare <= 0, "Truncation of %s value %s resulted in a bigger value %s", rightType, rightValue, rangeLow);
            boolean rightValueAtRangeLow = compare == 0;

            return switch (operator) {
                case EQUAL -> {
                    if (!rightValueAtRangeLow) {
                        yield falseIfNotNull(argument);
                    }
                    yield between(
                            plannerContext.getMetadata(),
                            symbolAllocator,
                            argument,
                            new Constant(rightType, rangeLow),
                            new Constant(rightType, calculateRangeEndInclusive(rangeLow, rightType, unit)));
                }
                case NOT_EQUAL -> {
                    if (!rightValueAtRangeLow) {
                        yield trueIfNotNull(argument);
                    }
                    yield not(plannerContext.getMetadata(), between(
                            plannerContext.getMetadata(),
                            symbolAllocator,
                            argument,
                            new Constant(rightType, rangeLow),
                            new Constant(rightType, calculateRangeEndInclusive(rangeLow, rightType, unit))));
                }
                case IDENTICAL -> {
                    if (!rightValueAtRangeLow) {
                        yield FALSE;
                    }
                    yield and(
                            not(plannerContext.getMetadata(), new IsNull(argument)),
                            between(
                                    plannerContext.getMetadata(),
                                    symbolAllocator,
                                    argument,
                                    new Constant(rightType, rangeLow),
                                    new Constant(rightType, calculateRangeEndInclusive(rangeLow, rightType, unit))));
                }
                case LESS_THAN -> {
                    if (rightValueAtRangeLow) {
                        yield comparison(plannerContext.getMetadata(), LESS_THAN, argument, new Constant(rightType, rangeLow));
                    }
                    yield comparison(plannerContext.getMetadata(), LESS_THAN_OR_EQUAL, argument, new Constant(rightType, calculateRangeEndInclusive(rangeLow, rightType, unit)));
                }
                case LESS_THAN_OR_EQUAL -> {
                    yield comparison(plannerContext.getMetadata(), LESS_THAN_OR_EQUAL, argument, new Constant(rightType, calculateRangeEndInclusive(rangeLow, rightType, unit)));
                }
                case GREATER_THAN -> {
                    yield comparison(plannerContext.getMetadata(), GREATER_THAN, argument, new Constant(rightType, calculateRangeEndInclusive(rangeLow, rightType, unit)));
                }
                case GREATER_THAN_OR_EQUAL -> {
                    if (rightValueAtRangeLow) {
                        yield comparison(plannerContext.getMetadata(), GREATER_THAN_OR_EQUAL, argument, new Constant(rightType, rangeLow));
                    }
                    yield comparison(plannerContext.getMetadata(), GREATER_THAN, argument, new Constant(rightType, calculateRangeEndInclusive(rangeLow, rightType, unit)));
                }
            };
        }

        public Expression trueIfNotNull(Expression argument)
        {
            return or(not(plannerContext.getMetadata(), new IsNull(argument)), new Constant(BOOLEAN, null));
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
