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
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.trino.Session;
import io.trino.metadata.OperatorNotFoundException;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.InterpretedFunctionInvoker;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.ComparisonOperator;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.ExpressionTreeRewriter;
import io.trino.sql.ir.IrExpressions.Between;
import io.trino.sql.ir.IrExpressions.Comparison;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Let;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.optimizer.IrExpressionOptimizer;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.type.TypeCoercion;

import java.lang.invoke.MethodHandle;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.time.zone.ZoneOffsetTransition;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SliceUtf8.countCodePoints;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.TypeUtils.isFloatingPointNaN;
import static io.trino.spi.type.TypeUtils.typeHasNaN;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.ComparisonOperator.IDENTICAL;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.ComparisonOperator.NOT_EQUAL;
import static io.trino.sql.ir.IrExpressions.between;
import static io.trino.sql.ir.IrExpressions.bindIfNecessary;
import static io.trino.sql.ir.IrExpressions.comparison;
import static io.trino.sql.ir.IrExpressions.matchBetween;
import static io.trino.sql.ir.IrExpressions.matchComparison;
import static io.trino.sql.ir.IrExpressions.not;
import static io.trino.sql.ir.IrUtils.and;
import static io.trino.sql.ir.IrUtils.or;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * Given s of type S, a constant expression t of type T, and when an implicit
 * cast exists between S->T, converts expression of the form:
 *
 * <pre>
 * CAST(s as T) = t
 * </pre>
 * <p>
 * into
 *
 * <pre>
 * s = CAST(t as S)
 * </pre>
 * <p>
 * For example:
 *
 * <pre>
 * CAST(x AS bigint) = bigint '1'
 * </pre>
 * <p>
 * turns into
 *
 * <pre>
 * x = smallint '1'
 * </pre>
 * <p>
 * It can simplify expressions that are known to be true or false, and
 * remove the comparisons altogether. For example, give x::smallint,
 * for an expression like:
 *
 * <pre>
 * CAST(x AS bigint) > bigint '10000000'
 * </pre>
 * <p>
 * The same unwrapping applies to a BETWEEN range, rewriting
 *
 * <pre>
 * CAST(s AS T) BETWEEN t1 AND t2
 * </pre>
 * <p>
 * into an inclusive range on s. For example, given ts::timestamp(6),
 *
 * <pre>
 * CAST(ts AS date) BETWEEN date '2020-01-01' AND date '2020-01-02'
 * </pre>
 * <p>
 * turns into
 *
 * <pre>
 * ts BETWEEN timestamp '2020-01-01 00:00:00.000000' AND timestamp '2020-01-02 23:59:59.999999'
 * </pre>
 */
public class UnwrapCastInComparison
        extends ExpressionRewriteRuleSet
{
    public UnwrapCastInComparison(PlannerContext plannerContext)
    {
        super(createRewrite(plannerContext));
    }

    private static ExpressionRewriter createRewrite(PlannerContext plannerContext)
    {
        requireNonNull(plannerContext, "plannerContext is null");

        return (expression, context) -> unwrapCasts(context.getSession(), plannerContext, context.getSymbolAllocator(), expression);
    }

    public static Expression unwrapCasts(
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
        private final IrExpressionOptimizer optimizer;

        public Visitor(PlannerContext plannerContext, Session session, SymbolAllocator symbolAllocator)
        {
            this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
            this.session = requireNonNull(session, "session is null");
            this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
            this.functionInvoker = new InterpretedFunctionInvoker(plannerContext.getFunctionManager());
            this.optimizer = plannerContext.getExpressionOptimizer();
        }

        @Override
        public Expression rewriteCall(Call node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            Call expression = treeRewriter.defaultRewrite(node, null);
            if (!(matchComparison(expression) instanceof Comparison comparison)) {
                return expression;
            }
            // The unwrap logic expects the cast on the left. A comparison whose cast operand is on the
            // right (e.g. the canonical form of cast(x) > literal, which is literal < cast(x)) is flipped
            // so the cast lands on the left; the rebuilt comparison is canonicalized again on the way out.
            if (comparison.right() instanceof Cast && !(comparison.left() instanceof Cast)) {
                return unwrapCast(comparison.operator().flip(), comparison.right(), comparison.left());
            }
            return unwrapCast(comparison.operator(), comparison.left(), comparison.right());
        }

        @Override
        public Expression rewriteLet(Let node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            Let expression = treeRewriter.defaultRewrite(node, null);
            // A BETWEEN over a non-trivial value binds it in a Let; unwrap a cast in that bound value here.
            return unwrapCastInBetween(expression).orElse(expression);
        }

        private Optional<Expression> unwrapCastInBetween(Expression expression)
        {
            if (!(matchBetween(expression) instanceof Between range) || !(range.value() instanceof Cast cast)) {
                return Optional.empty();
            }

            Expression source = cast.expression();
            Type sourceType = source.type();
            // Unwrap each half of the BETWEEN independently, as the lower and upper comparison against the cast.
            Expression low = unwrapCast(GREATER_THAN_OR_EQUAL, cast, range.min());
            Expression high = unwrapCast(LESS_THAN_OR_EQUAL, cast, range.max());

            // unwrapCast collapses a bound to a constant truth value when the literal falls outside the source
            // type range: a never-satisfied bound empties the range, an always-satisfied bound drops out.
            if (isNeverSatisfied(low, source) || isNeverSatisfied(high, source)) {
                return Optional.of(falseIfNotNull(source));
            }
            boolean lowAlwaysHolds = trueIfNotNull(source).equals(low);
            boolean highAlwaysHolds = trueIfNotNull(source).equals(high);
            if (lowAlwaysHolds && highAlwaysHolds) {
                return Optional.of(trueIfNotNull(source));
            }
            if (lowAlwaysHolds) {
                return Optional.of(high);
            }
            if (highAlwaysHolds) {
                return Optional.of(low);
            }

            // Each surviving bound must be a comparison of the source against a constant; otherwise leave the
            // BETWEEN untouched.
            Optional<ComparisonBound> lowBound = comparisonBound(low, source);
            Optional<ComparisonBound> highBound = comparisonBound(high, source);
            if (lowBound.isEmpty() || highBound.isEmpty()) {
                return Optional.empty();
            }

            // Rebuild an inclusive BETWEEN when the two comparisons form a lower/upper pair whose strict ends have
            // an inclusive neighbor; otherwise keep them as a conjunction.
            Optional<Object> inclusiveLow = inclusiveLowBound(sourceType, lowBound.get());
            Optional<Object> inclusiveHigh = inclusiveHighBound(sourceType, highBound.get());
            if (inclusiveLow.isEmpty() || inclusiveHigh.isEmpty()) {
                // The conjunction references the cast source in both halves; bind a non-trivial source once so it is
                // evaluated a single time, and recompute each half against the bound operand.
                return Optional.of(bindSourceIfNecessary(source, operand -> {
                    Cast boundCast = new Cast(operand, cast.type(), cast.kind());
                    return and(
                            unwrapCast(GREATER_THAN_OR_EQUAL, boundCast, range.min()),
                            unwrapCast(LESS_THAN_OR_EQUAL, boundCast, range.max()));
                }));
            }
            if (compare(sourceType, inclusiveLow.get(), inclusiveHigh.get()) > 0) {
                return Optional.of(falseIfNotNull(source));
            }
            return Optional.of(between(plannerContext.getMetadata(), symbolAllocator, source, new Constant(sourceType, inclusiveLow.get()), new Constant(sourceType, inclusiveHigh.get())));
        }

        private static boolean isNeverSatisfied(Expression bound, Expression source)
        {
            return FALSE.equals(bound) || falseIfNotNull(source).equals(bound);
        }

        // A comparison of the source against a non-null constant, with the source normalized to the left operand.
        private record ComparisonBound(ComparisonOperator operator, Object value) {}

        private static Optional<ComparisonBound> comparisonBound(Expression bound, Expression source)
        {
            if (!(matchComparison(bound) instanceof Comparison comparison)) {
                return Optional.empty();
            }
            ComparisonOperator operator;
            Expression other;
            if (source.equals(comparison.left())) {
                operator = comparison.operator();
                other = comparison.right();
            }
            else if (source.equals(comparison.right())) {
                // comparison() canonicalizes operand order, so an unwrapped `source >= x` surfaces as `x <= source`.
                operator = comparison.operator().flip();
                other = comparison.left();
            }
            else {
                return Optional.empty();
            }
            if (other instanceof Constant(Type _, Object value) && value != null) {
                return Optional.of(new ComparisonBound(operator, value));
            }
            return Optional.empty();
        }

        // The inclusive lower bound value, tightening a strict `>` to its next value; empty when the operator is not
        // a lower bound or the next value does not exist.
        private static Optional<Object> inclusiveLowBound(Type sourceType, ComparisonBound bound)
        {
            return switch (bound.operator()) {
                case GREATER_THAN_OR_EQUAL -> Optional.of(bound.value());
                case GREATER_THAN -> sourceType.getNextValue(bound.value());
                default -> Optional.empty();
            };
        }

        // The inclusive upper bound value, tightening a strict `<` to its previous value; empty when the operator is
        // not an upper bound or the previous value does not exist.
        private static Optional<Object> inclusiveHighBound(Type sourceType, ComparisonBound bound)
        {
            return switch (bound.operator()) {
                case LESS_THAN_OR_EQUAL -> Optional.of(bound.value());
                case LESS_THAN -> sourceType.getPreviousValue(bound.value());
                default -> Optional.empty();
            };
        }

        private Expression unwrapCast(ComparisonOperator operator, Expression originalLeft, Expression originalRight)
        {
            // Canonicalization is handled by CanonicalizeExpressionRewriter
            if (!(originalLeft instanceof Cast cast)) {
                return comparison(plannerContext.getMetadata(), operator, originalLeft, originalRight);
            }

            Expression right = optimizer.process(originalRight, session, symbolAllocator, ImmutableMap.of()).orElse(originalRight);

            if (right instanceof Constant constant && constant.value() == null) {
                return switch (operator) {
                    case EQUAL, NOT_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL -> new Constant(BOOLEAN, null);
                    case IDENTICAL -> new IsNull(cast);
                };
            }

            if (!(right instanceof Constant(Type _, Object rightValue))) {
                return comparison(plannerContext.getMetadata(), operator, cast, originalRight);
            }

            Type sourceType = cast.expression().type();
            Type targetType = originalRight.type();

            if (sourceType instanceof TimestampType timestampType && targetType == DATE) {
                return unwrapTimestampToDateCast(timestampType, operator, cast.expression(), (long) rightValue).orElse(comparison(plannerContext.getMetadata(), operator, cast, originalRight));
            }

            if (targetType instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
                // Note: two TIMESTAMP WITH TIME ZONE values differing in zone only (same instant) are considered equal.
                rightValue = withTimeZone(timestampWithTimeZoneType, rightValue, session.getTimeZoneKey());
            }

            if (sourceType instanceof CharType charType && targetType instanceof VarcharType varcharType) {
                return unwrapCharToVarcharCast(charType, varcharType, operator, cast.expression(), (Slice) rightValue)
                        .orElse(comparison(plannerContext.getMetadata(), operator, cast, originalRight));
            }

            if (!hasInjectiveImplicitCoercion(sourceType, targetType, rightValue)) {
                return comparison(plannerContext.getMetadata(), operator, cast, originalRight);
            }

            // Handle comparison against NaN.
            // It must be done before source type range bounds are compared to target value.
            if (isFloatingPointNaN(targetType, rightValue)) {
                switch (operator) {
                    case EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL -> {
                        return falseIfNotNull(cast.expression());
                    }
                    case NOT_EQUAL -> {
                        return trueIfNotNull(cast.expression());
                    }
                    case IDENTICAL -> {
                        if (!typeHasNaN(sourceType)) {
                            return FALSE;
                        }
                        // NaN on the right of comparison will be cast to source type later
                    }
                    default -> throw new UnsupportedOperationException("Not yet implemented: " + operator);
                }
            }

            ResolvedFunction sourceToTarget = plannerContext.getMetadata().getCoercion(sourceType, targetType);

            Optional<Type.Range> sourceRange = sourceType.getRange();
            if (sourceRange.isPresent()) {
                Object max = sourceRange.get().getMax();
                Object maxInTargetType = null;
                try {
                    maxInTargetType = coerce(max, sourceToTarget);
                }
                catch (RuntimeException e) {
                    // Coercion may fail e.g. for out of range values, it's not guaranteed to be "saturated"
                }
                if (maxInTargetType != null) {
                    // NaN values of `right` are excluded at this point. Otherwise, NaN would be recognized as
                    // greater than source type upper bound, and incorrect expression might be derived.
                    int upperBoundComparison = compare(targetType, rightValue, maxInTargetType);
                    if (upperBoundComparison > 0) {
                        // larger than maximum representable value
                        return switch (operator) {
                            case EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL -> falseIfNotNull(cast.expression());
                            case NOT_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL -> trueIfNotNull(cast.expression());
                            case IDENTICAL -> FALSE;
                        };
                    }

                    if (upperBoundComparison == 0) {
                        // equal to max representable value
                        return switch (operator) {
                            case GREATER_THAN -> falseIfNotNull(cast.expression());
                            case GREATER_THAN_OR_EQUAL -> comparison(plannerContext.getMetadata(), EQUAL, cast.expression(), new Constant(sourceType, max));
                            case LESS_THAN_OR_EQUAL -> trueIfNotNull(cast.expression());
                            case LESS_THAN -> comparison(plannerContext.getMetadata(), NOT_EQUAL, cast.expression(), new Constant(sourceType, max));
                            case EQUAL, NOT_EQUAL, IDENTICAL -> comparison(plannerContext.getMetadata(), operator, cast.expression(), new Constant(sourceType, max));
                        };
                    }

                    Object min = sourceRange.get().getMin();
                    Object minInTargetType = coerce(min, sourceToTarget);

                    int lowerBoundComparison = compare(targetType, rightValue, minInTargetType);
                    if (lowerBoundComparison < 0) {
                        // smaller than minimum representable value
                        return switch (operator) {
                            case NOT_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL -> trueIfNotNull(cast.expression());
                            case EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL -> falseIfNotNull(cast.expression());
                            case IDENTICAL -> FALSE;
                        };
                    }

                    if (lowerBoundComparison == 0) {
                        // equal to min representable value
                        return switch (operator) {
                            case LESS_THAN -> falseIfNotNull(cast.expression());
                            case LESS_THAN_OR_EQUAL -> comparison(plannerContext.getMetadata(), EQUAL, cast.expression(), new Constant(sourceType, min));
                            case GREATER_THAN_OR_EQUAL -> trueIfNotNull(cast.expression());
                            case GREATER_THAN -> comparison(plannerContext.getMetadata(), NOT_EQUAL, cast.expression(), new Constant(sourceType, min));
                            case EQUAL, NOT_EQUAL, IDENTICAL -> comparison(plannerContext.getMetadata(), operator, cast.expression(), new Constant(sourceType, min));
                        };
                    }
                }
            }

            ResolvedFunction targetToSource;
            try {
                targetToSource = plannerContext.getMetadata().getCoercion(targetType, sourceType);
            }
            catch (OperatorNotFoundException e) {
                // Without a cast between target -> source, there's nothing more we can do
                return comparison(plannerContext.getMetadata(), operator, cast, originalRight);
            }

            Object literalInSourceType;
            try {
                literalInSourceType = coerce(rightValue, targetToSource);
            }
            catch (TrinoException e) {
                // A failure to cast from target -> source type could be because:
                //  1. missing cast
                //  2. bad implementation
                //  3. out of range or otherwise unrepresentable value
                // Since we can't distinguish between those cases, take the conservative option
                // and bail out.
                return comparison(plannerContext.getMetadata(), operator, cast, originalRight);
            }

            if (targetType.isOrderable()) {
                Object roundtripLiteral = coerce(literalInSourceType, sourceToTarget);

                int literalVsRoundtripped = compare(targetType, rightValue, roundtripLiteral);

                if (literalVsRoundtripped > 0) {
                    // cast rounded down
                    return switch (operator) {
                        case EQUAL -> falseIfNotNull(cast.expression());
                        case NOT_EQUAL -> trueIfNotNull(cast.expression());
                        case IDENTICAL -> FALSE;
                        case LESS_THAN, LESS_THAN_OR_EQUAL -> {
                            if (sourceRange.isPresent() && compare(sourceType, sourceRange.get().getMin(), literalInSourceType) == 0) {
                                yield comparison(plannerContext.getMetadata(), EQUAL, cast.expression(), new Constant(sourceType, literalInSourceType));
                            }
                            yield comparison(plannerContext.getMetadata(), LESS_THAN_OR_EQUAL, cast.expression(), new Constant(sourceType, literalInSourceType));
                        }
                        // We expect implicit coercions to be order-preserving, so the result of converting back from target -> source cannot produce a value
                        // larger than the next value in the source type
                        case GREATER_THAN, GREATER_THAN_OR_EQUAL -> comparison(plannerContext.getMetadata(), GREATER_THAN, cast.expression(), new Constant(sourceType, literalInSourceType));
                    };
                }

                if (literalVsRoundtripped < 0) {
                    // cast rounded up
                    return switch (operator) {
                        case EQUAL -> falseIfNotNull(cast.expression());
                        case NOT_EQUAL -> trueIfNotNull(cast.expression());
                        case IDENTICAL -> FALSE;
                        // We expect implicit coercions to be order-preserving, so the result of converting back from target -> source cannot produce a value
                        // smaller than the next value in the source type
                        case LESS_THAN, LESS_THAN_OR_EQUAL -> comparison(plannerContext.getMetadata(), LESS_THAN, cast.expression(), new Constant(sourceType, literalInSourceType));
                        case GREATER_THAN, GREATER_THAN_OR_EQUAL -> sourceRange.isPresent() && compare(sourceType, sourceRange.get().getMax(), literalInSourceType) == 0 ?
                                comparison(plannerContext.getMetadata(), EQUAL, cast.expression(), new Constant(sourceType, literalInSourceType)) :
                                comparison(plannerContext.getMetadata(), GREATER_THAN_OR_EQUAL, cast.expression(), new Constant(sourceType, literalInSourceType));
                    };
                }
            }

            return comparison(plannerContext.getMetadata(), operator, cast.expression(), new Constant(sourceType, literalInSourceType));
        }

        private Optional<Expression> unwrapCharToVarcharCast(CharType charType, VarcharType varcharType, ComparisonOperator operator, Expression charExpression, Slice value)
        {
            // CHAR -> VARCHAR trims trailing spaces, and CHAR comparison is PAD SPACE while VARCHAR comparison is
            // NO PAD, so an ordering comparison does not translate to a CHAR comparison. Only the equality family is
            // safe to unwrap; leave ordering comparisons as a residual filter.
            if (operator != EQUAL && operator != NOT_EQUAL && operator != IDENTICAL) {
                return Optional.empty();
            }

            // CHAR -> VARCHAR(y) with y shorter than the char length is not injective on the source: distinct char
            // values that share their first y characters collapse to the same varchar, so a single char equality
            // cannot represent the comparison. The implicit coercion is always CHAR(n) -> VARCHAR(n) (y == n), so this
            // only guards explicit narrowing casts.
            if (!varcharType.isUnbounded() && varcharType.getBoundedLength() < charType.getLength()) {
                return Optional.empty();
            }

            ResolvedFunction varcharToChar;
            ResolvedFunction charToVarchar;
            try {
                varcharToChar = plannerContext.getMetadata().getCoercion(varcharType, charType);
                charToVarchar = plannerContext.getMetadata().getCoercion(charType, varcharType);
            }
            catch (OperatorNotFoundException e) {
                return Optional.empty();
            }

            Object literalInChar;
            try {
                literalInChar = coerce(value, varcharToChar);
            }
            catch (TrinoException e) {
                return Optional.empty();
            }

            // The literal round-trips through char(n) unchanged exactly when it has no trailing spaces and fits in
            // char(n). In that case CAST(c AS varchar) = v is equivalent to c = CAST(v AS char(n)); otherwise no char
            // value's (trimmed) varchar form can equal the literal, so the equality is unsatisfiable.
            if (value.equals(coerce(literalInChar, charToVarchar))) {
                return Optional.of(comparison(plannerContext.getMetadata(), operator, charExpression, new Constant(charType, literalInChar)));
            }
            return Optional.of(switch (operator) {
                case EQUAL -> falseIfNotNull(charExpression);
                case NOT_EQUAL -> trueIfNotNull(charExpression);
                case IDENTICAL -> FALSE;
                default -> throw new IllegalStateException("Unexpected operator: " + operator);
            });
        }

        private Optional<Expression> unwrapTimestampToDateCast(TimestampType sourceType, ComparisonOperator operator, Expression timestampExpression, long date)
        {
            ResolvedFunction targetToSource;
            try {
                targetToSource = plannerContext.getMetadata().getCoercion(DATE, sourceType);
            }
            catch (OperatorNotFoundException e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
            }

            Expression dateTimestamp = new Constant(sourceType, coerce(date, targetToSource));
            Expression nextDateTimestamp = new Constant(sourceType, coerce(date + 1, targetToSource));

            return switch (operator) {
                case EQUAL -> Optional.of(bindSourceIfNecessary(timestampExpression, operand ->
                        and(comparison(plannerContext.getMetadata(), GREATER_THAN_OR_EQUAL, operand, dateTimestamp),
                                comparison(plannerContext.getMetadata(), LESS_THAN, operand, nextDateTimestamp))));
                case NOT_EQUAL -> Optional.of(bindSourceIfNecessary(timestampExpression, operand ->
                        or(comparison(plannerContext.getMetadata(), LESS_THAN, operand, dateTimestamp),
                                comparison(plannerContext.getMetadata(), GREATER_THAN_OR_EQUAL, operand, nextDateTimestamp))));
                case LESS_THAN -> Optional.of(comparison(plannerContext.getMetadata(), LESS_THAN, timestampExpression, dateTimestamp));
                case LESS_THAN_OR_EQUAL -> Optional.of(comparison(plannerContext.getMetadata(), LESS_THAN, timestampExpression, nextDateTimestamp));
                case GREATER_THAN -> Optional.of(comparison(plannerContext.getMetadata(), GREATER_THAN_OR_EQUAL, timestampExpression, nextDateTimestamp));
                case GREATER_THAN_OR_EQUAL -> Optional.of(comparison(plannerContext.getMetadata(), GREATER_THAN_OR_EQUAL, timestampExpression, dateTimestamp));
                case IDENTICAL -> Optional.of(bindSourceIfNecessary(timestampExpression, operand ->
                        and(not(plannerContext.getMetadata(), new IsNull(operand)),
                                comparison(plannerContext.getMetadata(), GREATER_THAN_OR_EQUAL, operand, dateTimestamp),
                                comparison(plannerContext.getMetadata(), LESS_THAN, operand, nextDateTimestamp))));
            };
        }

        private Expression bindSourceIfNecessary(Expression source, Function<Expression, Expression> body)
        {
            // A cast over a reference or constant is cheap and deterministic, and must stay inline so a later
            // unwrap pass can reach the underlying column and push the predicate into the scan; bind anything
            // else once so a non-trivial source is evaluated a single time.
            if (isCastOverTrivial(source)) {
                return body.apply(source);
            }
            return bindIfNecessary(symbolAllocator, "operand", source, body);
        }

        private static boolean isCastOverTrivial(Expression value)
        {
            return value instanceof Cast cast
                    && (cast.expression() instanceof Reference
                    || cast.expression() instanceof Constant
                    || isCastOverTrivial(cast.expression()));
        }

        private boolean hasInjectiveImplicitCoercion(Type source, Type target, Object value)
        {
            if ((source.equals(BIGINT) && target.equals(DOUBLE)) ||
                    (source.equals(BIGINT) && target.equals(REAL)) ||
                    (source.equals(INTEGER) && target.equals(REAL))) {
                // Not every BIGINT fits in DOUBLE/REAL due to 64 bit vs 53-bit/23-bit mantissa. Similarly,
                // not every INTEGER fits in a REAL (32-bit vs 23-bit mantissa)
                if (target.equals(DOUBLE)) {
                    double doubleValue = (double) value;
                    return doubleValue > Long.MAX_VALUE ||
                            doubleValue < Long.MIN_VALUE ||
                            Double.isNaN(doubleValue) ||
                            (doubleValue > -1L << 53 && doubleValue < 1L << 53); // in (-2^53, 2^53), bigint follows an injective implicit coercion w.r.t double
                }
                float realValue = intBitsToFloat(toIntExact((long) value));
                return (source.equals(BIGINT) && (realValue > Long.MAX_VALUE || realValue < Long.MIN_VALUE)) ||
                        (source.equals(INTEGER) && (realValue > Integer.MAX_VALUE || realValue < Integer.MIN_VALUE)) ||
                        Float.isNaN(realValue) ||
                        (realValue > -1L << 23 && realValue < 1L << 23); // in (-2^23, 2^23), bigint (and integer) follows an injective implicit coercion w.r.t real
            }

            if (source instanceof DecimalType decimalType) {
                int precision = decimalType.getPrecision();

                if (precision > 15 && target.equals(DOUBLE)) {
                    // decimal(p,s) with p > 15 doesn't fit in a double without loss
                    return false;
                }

                if (precision > 7 && target.equals(REAL)) {
                    // decimal(p,s) with p > 7 doesn't fit in a double without loss
                    return false;
                }
            }

            if (target instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
                if (source instanceof DateType) {
                    // Cast from TIMESTAMP WITH TIME ZONE to DATE and back to TIMESTAMP WITH TIME ZONE does not round trip, unless the value's zone is equal to session zone
                    if (!getTimeZone(timestampWithTimeZoneType, value).equals(session.getTimeZoneKey())) {
                        return false;
                    }

                    // Cast from DATE to TIMESTAMP WITH TIME ZONE is not monotonic when there is a forward DST change in the session zone
                    return isTimestampToTimestampWithTimeZoneInjectiveAt(session.getTimeZoneKey().getZoneId(), getInstantWithTruncation(timestampWithTimeZoneType, value));
                }
                if (source instanceof TimestampType) {
                    // Cast from TIMESTAMP WITH TIME ZONE to TIMESTAMP and back to TIMESTAMP WITH TIME ZONE does not round trip, unless the value's zone is equal to session zone
                    if (!getTimeZone(timestampWithTimeZoneType, value).equals(session.getTimeZoneKey())) {
                        return false;
                    }

                    // Cast from TIMESTAMP to TIMESTAMP WITH TIME ZONE is not monotonic when there is a forward DST change in the session zone
                    return isTimestampToTimestampWithTimeZoneInjectiveAt(session.getTimeZoneKey().getZoneId(), getInstantWithTruncation(timestampWithTimeZoneType, value));
                }
                // CAST from TIMESTAMP WITH TIME ZONE to d and back to TIMESTAMP WITH TIME ZONE does not round trip for most types d
                // TODO add test coverage
                return false;
            }

            if (target instanceof TimeWithTimeZoneType) {
                // For example, CAST from TIME WITH TIME ZONE to TIME and back to TIME WITH TIME ZONE does not round trip

                // TODO add test coverage
                return false;
            }

            boolean coercible = new TypeCoercion(plannerContext.getTypeManager()::getType, plannerContext.isLegacyVarcharToCharCoercion()).canCoerce(source, target);
            if (source instanceof VarcharType sourceVarchar && target instanceof CharType targetChar) {
                if (sourceVarchar.isUnbounded() || sourceVarchar.getBoundedLength() > targetChar.getLength()) {
                    // Truncation, not injective.
                    return false;
                }
                // char should probably be coercible to varchar, not vice-versa. The code here needs to be updated when things change.
                verify(coercible, "%s was expected to be coercible to %s", source, target);
                if (sourceVarchar.getBoundedLength() == 0) {
                    // the source domain is single-element set
                    return true;
                }
                int actualLengthWithoutSpaces = countCodePoints((Slice) value);
                verify(actualLengthWithoutSpaces <= targetChar.getLength(), "Incorrect char value [%s] for %s", ((Slice) value).toStringUtf8(), targetChar);
                return sourceVarchar.getBoundedLength() == actualLengthWithoutSpaces;
            }

            // Well-behaved implicit casts are injective
            return coercible;
        }

        private Object coerce(Object value, ResolvedFunction coercion)
        {
            return functionInvoker.invoke(coercion, session.toConnectorSession(), value);
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

        public Expression trueIfNotNull(Expression argument)
        {
            return or(not(plannerContext.getMetadata(), new IsNull(argument)), new Constant(BOOLEAN, null));
        }
    }

    /**
     * Replace time zone component of a {@link TimestampWithTimeZoneType} value with a given one, preserving point in time
     * (equivalent to {@link java.time.ZonedDateTime#withZoneSameInstant}.
     */
    private static Object withTimeZone(TimestampWithTimeZoneType type, Object value, TimeZoneKey newZone)
    {
        if (type.isShort()) {
            return packDateTimeWithZone(unpackMillisUtc((long) value), newZone);
        }
        LongTimestampWithTimeZone longTimestampWithTimeZone = (LongTimestampWithTimeZone) value;
        return LongTimestampWithTimeZone.fromEpochMillisAndFraction(longTimestampWithTimeZone.getEpochMillis(), longTimestampWithTimeZone.getPicosOfMilli(), newZone);
    }

    private static TimeZoneKey getTimeZone(TimestampWithTimeZoneType type, Object value)
    {
        if (type.isShort()) {
            return unpackZoneKey(((long) value));
        }
        return TimeZoneKey.getTimeZoneKey(((LongTimestampWithTimeZone) value).getTimeZoneKey());
    }

    @VisibleForTesting
    static boolean isTimestampToTimestampWithTimeZoneInjectiveAt(ZoneId zone, Instant instant)
    {
        ZoneOffsetTransition transition = zone.getRules().previousTransition(instant.plusNanos(1));
        if (transition != null) {
            // DST change forward and the instant is ambiguous, being within the 'gap' area non-monotonic remapping
            return transition.getDuration().isNegative() || transition.getDateTimeAfter().minusNanos(1).atZone(zone).toInstant().isBefore(instant);
        }
        return true;
    }

    private static Instant getInstantWithTruncation(TimestampWithTimeZoneType type, Object value)
    {
        if (type.isShort()) {
            return Instant.ofEpochMilli(unpackMillisUtc(((long) value)));
        }
        LongTimestampWithTimeZone longTimestampWithTimeZone = (LongTimestampWithTimeZone) value;
        return Instant.ofEpochMilli(longTimestampWithTimeZone.getEpochMillis())
                .plus(longTimestampWithTimeZone.getPicosOfMilli() / PICOSECONDS_PER_NANOSECOND, ChronoUnit.NANOS);
    }

    public static Expression falseIfNotNull(Expression argument)
    {
        return and(new IsNull(argument), new Constant(BOOLEAN, null));
    }
}
