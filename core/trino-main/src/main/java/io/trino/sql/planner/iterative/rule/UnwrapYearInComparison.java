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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Between;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.ExpressionTreeRewriter;
import io.trino.sql.ir.In;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Not;
import io.trino.sql.planner.IrExpressionInterpreter;

import java.time.LocalDate;
import java.time.LocalDateTime;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.IrUtils.or;
import static io.trino.type.DateTimes.PICOSECONDS_PER_MICROSECOND;
import static io.trino.type.DateTimes.scaleFactor;
import static java.lang.Math.multiplyExact;
import static java.lang.Math.toIntExact;
import static java.time.ZoneOffset.UTC;
import static java.time.temporal.TemporalAdjusters.lastDayOfYear;
import static java.util.Objects.requireNonNull;

/**
 * Transforms a constant date/time expression t
 * <pre>
 *     year(date_time) = t
 * </pre>
 * <p>
 * into
 * <pre>
 *     date_time BETWEEN (beginning of the year t) AND (end of the year t)
 * </pre>
 *
 * @see UnwrapCastInComparison
 */
public class UnwrapYearInComparison
        extends ExpressionRewriteRuleSet
{
    public UnwrapYearInComparison(PlannerContext plannerContext)
    {
        super(createRewrite(plannerContext));
    }

    private static ExpressionRewriter createRewrite(PlannerContext plannerContext)
    {
        requireNonNull(plannerContext, "plannerContext is null");

        return (expression, context) -> unwrapYear(context.getSession(), plannerContext, expression);
    }

    private static Expression unwrapYear(Session session,
            PlannerContext plannerContext,
            Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new Visitor(plannerContext, session), expression);
    }

    private static class Visitor
            extends io.trino.sql.ir.ExpressionRewriter<Void>
    {
        private final PlannerContext plannerContext;
        private final Session session;

        public Visitor(PlannerContext plannerContext, Session session)
        {
            this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
            this.session = requireNonNull(session, "session is null");
        }

        @Override
        public Expression rewriteComparison(Comparison node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            Comparison expression = treeRewriter.defaultRewrite(node, null);
            return unwrapYear(expression);
        }

        @Override
        public Expression rewriteIn(In node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            In in = treeRewriter.defaultRewrite(node, null);
            Expression value = in.value();

            if (!(value instanceof Call call) ||
                    !call.function().name().equals(builtinFunctionName("year")) ||
                    call.arguments().size() != 1) {
                return in;
            }

            // Convert each value to a comparison expression and try to unwrap it.
            // unwrap the InPredicate only in case we manage to unwrap the entire value list
            ImmutableList.Builder<Expression> comparisonExpressions = ImmutableList.builderWithExpectedSize(node.valueList().size());
            for (Expression rightExpression : node.valueList()) {
                Comparison comparison = new Comparison(EQUAL, value, rightExpression);
                Expression unwrappedExpression = unwrapYear(comparison);
                if (unwrappedExpression == comparison) {
                    return in;
                }
                comparisonExpressions.add(unwrappedExpression);
            }

            return or(comparisonExpressions.build());
        }

        // Simplify `year(d) ? value`
        private Expression unwrapYear(Comparison expression)
        {
            // Expect year on the left side and value on the right side of the comparison.
            // This is provided by CanonicalizeExpressionRewriter.
            if (!(expression.left() instanceof Call call) ||
                    !call.function().name().equals(builtinFunctionName("year")) ||
                    call.arguments().size() != 1) {
                return expression;
            }

            Expression argument = getOnlyElement(call.arguments());
            Type argumentType = argument.type();

            Expression right = new IrExpressionInterpreter(expression.right(), plannerContext, session).optimize();

            if (right instanceof Constant constant && constant.value() == null) {
                return switch (expression.operator()) {
                    case EQUAL, NOT_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL -> new Constant(BOOLEAN, null);
                    case IS_DISTINCT_FROM -> new Not(new IsNull(argument));
                };
            }

            if (!(right instanceof Constant(Type rightType, Object rightValue))) {
                return expression;
            }
            if (argumentType instanceof TimestampWithTimeZoneType) {
                // Cannot replace with a range due to how year operates on value's local date/time.
                // I.e. unwrapping is possible only when values are all of some fixed zone and the zone is known.
                return expression;
            }
            if (argumentType != DATE && !(argumentType instanceof TimestampType)) {
                // e.g. year(INTERVAL) not handled here
                return expression;
            }

            int year = toIntExact((Long) rightValue);
            return switch (expression.operator()) {
                case EQUAL -> between(argument, argumentType, calculateRangeStartInclusive(year, argumentType), calculateRangeEndInclusive(year, argumentType));
                case NOT_EQUAL -> new Not(between(argument, argumentType, calculateRangeStartInclusive(year, argumentType), calculateRangeEndInclusive(year, argumentType)));
                case IS_DISTINCT_FROM -> or(
                        new IsNull(argument),
                        new Not(between(argument, argumentType, calculateRangeStartInclusive(year, argumentType), calculateRangeEndInclusive(year, argumentType))));
                case LESS_THAN -> {
                    Object value = calculateRangeStartInclusive(year, argumentType);
                    yield new Comparison(LESS_THAN, argument, new Constant(argumentType, value));
                }
                case LESS_THAN_OR_EQUAL -> {
                    Object value = calculateRangeEndInclusive(year, argumentType);
                    yield new Comparison(LESS_THAN_OR_EQUAL, argument, new Constant(argumentType, value));
                }
                case GREATER_THAN -> {
                    Object value = calculateRangeEndInclusive(year, argumentType);
                    yield new Comparison(GREATER_THAN, argument, new Constant(argumentType, value));
                }
                case GREATER_THAN_OR_EQUAL -> {
                    Object value = calculateRangeStartInclusive(year, argumentType);
                    yield new Comparison(GREATER_THAN_OR_EQUAL, argument, new Constant(argumentType, value));
                }
            };
        }

        private Between between(Expression argument, Type type, Object minInclusive, Object maxInclusive)
        {
            return new Between(
                    argument,
                    new Constant(type, minInclusive),
                    new Constant(type, maxInclusive));
        }
    }

    private static Object calculateRangeStartInclusive(int year, Type type)
    {
        if (type == DATE) {
            LocalDate firstDay = LocalDate.ofYearDay(year, 1);
            return firstDay.toEpochDay();
        }
        if (type instanceof TimestampType timestampType) {
            long yearStartEpochSecond = LocalDateTime.of(year, 1, 1, 0, 0).toEpochSecond(UTC);
            long yearStartEpochMicros = multiplyExact(yearStartEpochSecond, MICROSECONDS_PER_SECOND);
            if (timestampType.isShort()) {
                return yearStartEpochMicros;
            }
            return new LongTimestamp(yearStartEpochMicros, 0);
        }
        throw new UnsupportedOperationException("Unsupported type: " + type);
    }

    @VisibleForTesting
    public static Object calculateRangeEndInclusive(int year, Type type)
    {
        if (type == DATE) {
            LocalDate lastDay = LocalDate.ofYearDay(year, 1).with(lastDayOfYear());
            return lastDay.toEpochDay();
        }
        if (type instanceof TimestampType timestampType) {
            long nextYearStartEpochSecond = LocalDateTime.of(year + 1, 1, 1, 0, 0).toEpochSecond(UTC);
            long nextYearStartEpochMicros = multiplyExact(nextYearStartEpochSecond, MICROSECONDS_PER_SECOND);
            if (timestampType.isShort()) {
                return nextYearStartEpochMicros - scaleFactor(timestampType.getPrecision(), 6);
            }
            int picosOfMicro = toIntExact(PICOSECONDS_PER_MICROSECOND - scaleFactor(timestampType.getPrecision(), 12));
            return new LongTimestamp(nextYearStartEpochMicros - 1, picosOfMicro);
        }
        throw new UnsupportedOperationException("Unsupported type: " + type);
    }
}
