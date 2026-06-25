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
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.ComparisonOperator;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.ExpressionTreeRewriter;
import io.trino.sql.ir.In;
import io.trino.sql.ir.IrExpressions.Comparison;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.optimizer.IrExpressionOptimizer;
import io.trino.sql.planner.SymbolAllocator;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
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

        return (expression, context) -> unwrapYear(context.getSession(), plannerContext, context.getSymbolAllocator(), expression);
    }

    private static Expression unwrapYear(
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
        private final IrExpressionOptimizer optimizer;
        private final Metadata metadata;
        private final Session session;
        private final SymbolAllocator symbolAllocator;

        public Visitor(PlannerContext plannerContext, Session session, SymbolAllocator symbolAllocator)
        {
            this.optimizer = plannerContext.getExpressionOptimizer();
            this.metadata = plannerContext.getMetadata();
            this.session = requireNonNull(session, "session is null");
            this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
        }

        @Override
        public Expression rewriteCall(Call node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            Call expression = treeRewriter.defaultRewrite(node, null);
            if (!(matchComparison(expression) instanceof Comparison comparison)) {
                return expression;
            }
            // The unwrap logic expects year(...) on the left. A comparison whose year operand is on the
            // right (e.g. the canonical form of year(x) > literal, which is literal < year(x)) is flipped
            // so the year call lands on the left; the rebuilt comparison is canonicalized again on the way out.
            if (isYearCall(comparison.right()) && !isYearCall(comparison.left())) {
                return unwrapYear(comparison.operator().flip(), comparison.right(), comparison.left());
            }
            return unwrapYear(comparison.operator(), comparison.left(), comparison.right());
        }

        private static boolean isYearCall(Expression expression)
        {
            return expression instanceof Call call &&
                    call.function().name().equals(builtinFunctionName("year")) &&
                    call.arguments().size() == 1;
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
                Optional<Expression> unwrappedExpression = tryUnwrapYear(EQUAL, value, rightExpression);
                if (unwrappedExpression.isEmpty()) {
                    return in;
                }
                comparisonExpressions.add(unwrappedExpression.get());
            }

            return or(comparisonExpressions.build());
        }

        // Simplify `year(d) ? value`
        private Expression unwrapYear(ComparisonOperator operator, Expression left, Expression originalRight)
        {
            return tryUnwrapYear(operator, left, originalRight)
                    .orElseGet(() -> comparison(metadata, operator, left, originalRight));
        }

        // Returns the unwrapped form of `year(d) ? value`, or empty when the comparison cannot be unwrapped
        private Optional<Expression> tryUnwrapYear(ComparisonOperator operator, Expression left, Expression originalRight)
        {
            // Expect year on the left side and value on the right side of the comparison.
            // This is provided by CanonicalizeExpressionRewriter.
            if (!(left instanceof Call call) ||
                    !call.function().name().equals(builtinFunctionName("year")) ||
                    call.arguments().size() != 1) {
                return Optional.empty();
            }

            Expression argument = getOnlyElement(call.arguments());
            Type argumentType = argument.type();

            Expression right = optimizer.process(originalRight, session, symbolAllocator, ImmutableMap.of()).orElse(originalRight);

            if (right instanceof Constant constant && constant.value() == null) {
                return Optional.of(switch (operator) {
                    case EQUAL, NOT_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL -> new Constant(BOOLEAN, null);
                    case IDENTICAL -> new IsNull(argument);
                });
            }

            if (!(right instanceof Constant(Type _, Object rightValue))) {
                return Optional.empty();
            }
            if (argumentType instanceof TimestampWithTimeZoneType) {
                // Cannot replace with a range due to how year operates on value's local date/time.
                // I.e. unwrapping is possible only when values are all of some fixed zone and the zone is known.
                return Optional.empty();
            }
            if (argumentType != DATE && !(argumentType instanceof TimestampType)) {
                // e.g. year(INTERVAL) not handled here
                return Optional.empty();
            }

            int year = toIntExact((Long) rightValue);
            return Optional.of(switch (operator) {
                case EQUAL -> between(
                        metadata,
                        symbolAllocator,
                        argument,
                        new Constant(argumentType, calculateRangeStartInclusive(year, argumentType)),
                        new Constant(argumentType, calculateRangeEndInclusive(year, argumentType)));
                case NOT_EQUAL -> not(metadata, between(
                        metadata,
                        symbolAllocator,
                        argument,
                        new Constant(argumentType, calculateRangeStartInclusive(year, argumentType)),
                        new Constant(argumentType, calculateRangeEndInclusive(year, argumentType))));
                case IDENTICAL -> and(
                        not(metadata, new IsNull(argument)),
                        between(
                                metadata,
                                symbolAllocator,
                                argument,
                                new Constant(argumentType, calculateRangeStartInclusive(year, argumentType)),
                                new Constant(argumentType, calculateRangeEndInclusive(year, argumentType))));
                case LESS_THAN -> {
                    Object value = calculateRangeStartInclusive(year, argumentType);
                    yield comparison(metadata, LESS_THAN, argument, new Constant(argumentType, value));
                }
                case LESS_THAN_OR_EQUAL -> {
                    Object value = calculateRangeEndInclusive(year, argumentType);
                    yield comparison(metadata, LESS_THAN_OR_EQUAL, argument, new Constant(argumentType, value));
                }
                case GREATER_THAN -> {
                    Object value = calculateRangeEndInclusive(year, argumentType);
                    yield comparison(metadata, GREATER_THAN, argument, new Constant(argumentType, value));
                }
                case GREATER_THAN_OR_EQUAL -> {
                    Object value = calculateRangeStartInclusive(year, argumentType);
                    yield comparison(metadata, GREATER_THAN_OR_EQUAL, argument, new Constant(argumentType, value));
                }
            });
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
