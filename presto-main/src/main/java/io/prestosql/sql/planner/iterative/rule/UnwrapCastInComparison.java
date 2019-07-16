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
package io.prestosql.sql.planner.iterative.rule;

import io.prestosql.Session;
import io.prestosql.SystemSessionProperties;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.OperatorNotFoundException;
import io.prestosql.metadata.Signature;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.predicate.Utils;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.InterpretedFunctionInvoker;
import io.prestosql.sql.planner.ExpressionInterpreter;
import io.prestosql.sql.planner.LiteralEncoder;
import io.prestosql.sql.planner.NoOpSymbolResolver;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.ExpressionTreeRewriter;
import io.prestosql.sql.tree.IsNotNullPredicate;
import io.prestosql.sql.tree.IsNullPredicate;
import io.prestosql.sql.tree.NullLiteral;
import io.prestosql.type.TypeCoercion;

import java.util.Optional;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.sql.ExpressionUtils.and;
import static io.prestosql.sql.ExpressionUtils.or;
import static io.prestosql.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.EQUAL;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.LESS_THAN;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.NOT_EQUAL;
import static java.util.Objects.requireNonNull;

/**
 * Given s of type S, a constant expression t of type T, and when an implicit
 * cast exists between S->T, converts expression of the form:
 *
 * <pre>
 * CAST(s as T) = t
 * </pre>
 *
 * into
 *
 * <pre>
 * s = CAST(t as S)
 * </pre>
 *
 * For example:
 *
 * <pre>
 * CAST(x AS bigint) = bigint '1'
 *</pre>
 *
 * turns into
 *
 * <pre>
 * x = smallint '1'
 * </pre>
 *
 * It can simplify expressions that are known to be true or false, and
 * remove the comparisons altogether. For example, give x::smallint,
 * for an expression like:
 *
 * <pre>
 * CAST(x AS bigint) > bigint '10000000'
 *</pre>
 */
public class UnwrapCastInComparison
        extends ExpressionRewriteRuleSet
{
    public UnwrapCastInComparison(Metadata metadata, TypeAnalyzer typeAnalyzer)
    {
        super(createRewrite(metadata, typeAnalyzer));
    }

    private static ExpressionRewriter createRewrite(Metadata metadata, TypeAnalyzer typeAnalyzer)
    {
        requireNonNull(metadata, "metadata is null");
        requireNonNull(typeAnalyzer, "typeAnalyzer is null");

        return (expression, context) -> {
            if (SystemSessionProperties.isUnwrapCasts(context.getSession())) {
                return ExpressionTreeRewriter.rewriteWith(new Visitor(metadata, typeAnalyzer, context.getSession(), context.getSymbolAllocator().getTypes()), expression);
            }

            return expression;
        };
    }

    private static class Visitor
            extends io.prestosql.sql.tree.ExpressionRewriter<Void>
    {
        private final Metadata metadata;
        private final TypeAnalyzer typeAnalyzer;
        private final Session session;
        private final TypeProvider types;
        private final InterpretedFunctionInvoker functionInvoker;
        private final LiteralEncoder literalEncoder;

        public Visitor(Metadata metadata, TypeAnalyzer typeAnalyzer, Session session, TypeProvider types)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
            this.session = requireNonNull(session, "session is null");
            this.types = requireNonNull(types, "types is null");
            this.functionInvoker = new InterpretedFunctionInvoker(metadata);
            this.literalEncoder = new LiteralEncoder(metadata);
        }

        @Override
        public Expression rewriteComparisonExpression(ComparisonExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            ComparisonExpression expression = (ComparisonExpression) treeRewriter.defaultRewrite((Expression) node, null);
            return unwrapCast(expression);
        }

        private Expression unwrapCast(ComparisonExpression expression)
        {
            // Canonicalization is handled by CanonicalizeExpressionRewriter
            if (!(expression.getLeft() instanceof Cast)) {
                return expression;
            }

            Object right = ExpressionInterpreter.expressionOptimizer(expression.getRight(), metadata, session, typeAnalyzer.getTypes(session, types, expression.getRight()))
                    .optimize(NoOpSymbolResolver.INSTANCE);

            Cast cast = (Cast) expression.getLeft();
            ComparisonExpression.Operator operator = expression.getOperator();

            if (right == null || right instanceof NullLiteral) {
                switch (operator) {
                    case EQUAL:
                    case NOT_EQUAL:
                    case LESS_THAN:
                    case LESS_THAN_OR_EQUAL:
                    case GREATER_THAN:
                    case GREATER_THAN_OR_EQUAL:
                        return new Cast(new NullLiteral(), BooleanType.BOOLEAN.toString());
                    case IS_DISTINCT_FROM:
                        return new IsNotNullPredicate(cast);
                    default:
                        throw new UnsupportedOperationException("Not yet implemented");
                }
            }

            if (right instanceof Expression) {
                return expression;
            }

            Type sourceType = typeAnalyzer.getType(session, types, cast.getExpression());
            Type targetType = typeAnalyzer.getType(session, types, expression.getRight());

            if (!hasInjectiveImplicitCoercion(sourceType, targetType)) {
                return expression;
            }

            Signature sourceToTarget = metadata.getCoercion(sourceType, targetType);

            Optional<Type.Range> sourceRange = sourceType.getRange();
            if (sourceRange.isPresent()) {
                Object max = sourceRange.get().getMax();
                Object maxInTargetType = coerce(max, sourceToTarget);

                int upperBoundComparison = compare(targetType, right, maxInTargetType);
                if (upperBoundComparison > 0) {
                    // larger than maximum representable value
                    switch (operator) {
                        case EQUAL:
                        case GREATER_THAN:
                        case GREATER_THAN_OR_EQUAL:
                            return falseIfNotNull(cast.getExpression());
                        case NOT_EQUAL:
                        case LESS_THAN:
                        case LESS_THAN_OR_EQUAL:
                            return trueIfNotNull(cast.getExpression());
                        case IS_DISTINCT_FROM:
                            return TRUE_LITERAL;
                        default:
                            throw new UnsupportedOperationException("Not yet implemented: " + operator);
                    }
                }

                if (upperBoundComparison == 0) {
                    // equal to max representable value
                    switch (operator) {
                        case GREATER_THAN:
                            return falseIfNotNull(cast.getExpression());
                        case GREATER_THAN_OR_EQUAL:
                            return new ComparisonExpression(EQUAL, cast.getExpression(), literalEncoder.toExpression(max, sourceType));
                        case LESS_THAN_OR_EQUAL:
                            return trueIfNotNull(cast.getExpression());
                        case LESS_THAN:
                            return new ComparisonExpression(NOT_EQUAL, cast.getExpression(), literalEncoder.toExpression(max, sourceType));
                        case EQUAL:
                        case NOT_EQUAL:
                        case IS_DISTINCT_FROM:
                            return new ComparisonExpression(operator, cast.getExpression(), literalEncoder.toExpression(max, sourceType));
                        default:
                            throw new UnsupportedOperationException("Not yet implemented: " + operator);
                    }
                }

                Object min = sourceRange.get().getMin();
                Object minInTargetType = coerce(min, sourceToTarget);

                int lowerBoundComparison = compare(targetType, right, minInTargetType);
                if (lowerBoundComparison < 0) {
                    // smaller than minimum representable value
                    switch (operator) {
                        case NOT_EQUAL:
                        case GREATER_THAN:
                        case GREATER_THAN_OR_EQUAL:
                            return trueIfNotNull(cast.getExpression());
                        case EQUAL:
                        case LESS_THAN:
                        case LESS_THAN_OR_EQUAL:
                            return falseIfNotNull(cast.getExpression());
                        case IS_DISTINCT_FROM:
                            return TRUE_LITERAL;
                        default:
                            throw new UnsupportedOperationException("Not yet implemented: " + operator);
                    }
                }

                if (lowerBoundComparison == 0) {
                    // equal to min representable value
                    switch (operator) {
                        case LESS_THAN:
                            return falseIfNotNull(cast.getExpression());
                        case LESS_THAN_OR_EQUAL:
                            return new ComparisonExpression(EQUAL, cast.getExpression(), literalEncoder.toExpression(min, sourceType));
                        case GREATER_THAN_OR_EQUAL:
                            return trueIfNotNull(cast.getExpression());
                        case GREATER_THAN:
                            return new ComparisonExpression(NOT_EQUAL, cast.getExpression(), literalEncoder.toExpression(min, sourceType));
                        case EQUAL:
                        case NOT_EQUAL:
                        case IS_DISTINCT_FROM:
                            return new ComparisonExpression(operator, cast.getExpression(), literalEncoder.toExpression(min, sourceType));
                        default:
                            throw new UnsupportedOperationException("Not yet implemented: " + operator);
                    }
                }
            }

            Signature targetToSource;
            try {
                targetToSource = metadata.getCoercion(targetType, sourceType);
            }
            catch (OperatorNotFoundException e) {
                // Without a cast between target -> source, there's nothing more we can do
                return expression;
            }

            Object literalInSourceType;
            try {
                literalInSourceType = coerce(right, targetToSource);
            }
            catch (PrestoException e) {
                // A failure to cast from target -> source type could be because:
                //  1. missing cast
                //  2. bad implementation
                //  3. out of range or otherwise unrepresentable value
                // Since we can't distinguish between those cases, take the conservative option
                // and bail out.
                return expression;
            }

            Object roundtripLiteral = coerce(literalInSourceType, sourceToTarget);

            int literalVsRoundtripped = compare(targetType, right, roundtripLiteral);

            if (literalVsRoundtripped > 0) {
                // cast rounded down
                switch (operator) {
                    case EQUAL:
                        return falseIfNotNull(cast.getExpression());
                    case NOT_EQUAL:
                        return trueIfNotNull(cast.getExpression());
                    case IS_DISTINCT_FROM:
                        return TRUE_LITERAL;
                    case LESS_THAN:
                    case LESS_THAN_OR_EQUAL:
                        if (sourceRange.isPresent() && compare(sourceType, sourceRange.get().getMin(), literalInSourceType) == 0) {
                            return new ComparisonExpression(EQUAL, cast.getExpression(), literalEncoder.toExpression(literalInSourceType, sourceType));
                        }
                        return new ComparisonExpression(LESS_THAN_OR_EQUAL, cast.getExpression(), literalEncoder.toExpression(literalInSourceType, sourceType));
                    case GREATER_THAN:
                    case GREATER_THAN_OR_EQUAL:
                        // We expect implicit coercions to be order-preserving, so the result of converting back from target -> source cannot produce a value
                        // larger than the next value in the source type
                        return new ComparisonExpression(GREATER_THAN, cast.getExpression(), literalEncoder.toExpression(literalInSourceType, sourceType));
                    default:
                        throw new UnsupportedOperationException("Not yet implemented: " + operator);
                }
            }

            if (literalVsRoundtripped < 0) {
                // cast rounded up
                switch (operator) {
                    case EQUAL:
                        return falseIfNotNull(cast.getExpression());
                    case NOT_EQUAL:
                        return trueIfNotNull(cast.getExpression());
                    case IS_DISTINCT_FROM:
                        return TRUE_LITERAL;
                    case LESS_THAN:
                    case LESS_THAN_OR_EQUAL:
                        // We expect implicit coercions to be order-preserving, so the result of converting back from target -> source cannot produce a value
                        // smaller than the next value in the source type
                        return new ComparisonExpression(LESS_THAN, cast.getExpression(), literalEncoder.toExpression(literalInSourceType, sourceType));
                    case GREATER_THAN:
                    case GREATER_THAN_OR_EQUAL:
                        if (sourceRange.isPresent() && compare(sourceType, sourceRange.get().getMax(), literalInSourceType) == 0) {
                            return new ComparisonExpression(EQUAL, cast.getExpression(), literalEncoder.toExpression(literalInSourceType, sourceType));
                        }
                        return new ComparisonExpression(GREATER_THAN_OR_EQUAL, cast.getExpression(), literalEncoder.toExpression(literalInSourceType, sourceType));
                    default:
                        throw new UnsupportedOperationException("Not yet implemented: " + operator);
                }
            }

            return new ComparisonExpression(operator, cast.getExpression(), literalEncoder.toExpression(literalInSourceType, sourceType));
        }

        private boolean hasInjectiveImplicitCoercion(Type source, Type target)
        {
            if ((source.equals(BIGINT) && target.equals(DOUBLE)) ||
                    (source.equals(BIGINT) && target.equals(REAL)) ||
                    (source.equals(INTEGER) && target.equals(REAL))) {
                // Not every BIGINT fits in DOUBLE/REAL due to 64 bit vs 53-bit/23-bit mantissa. Similarly,
                // not every INTEGER fits in a REAL (32-bit vs 23-bit mantissa)
                return false;
            }

            if (source instanceof DecimalType) {
                int precision = ((DecimalType) source).getPrecision();

                if (precision > 15 && target.equals(DOUBLE)) {
                    // decimal(p,s) with p > 15 doesn't fit in a double without loss
                    return false;
                }

                if (precision > 7 && target.equals(REAL)) {
                    // decimal(p,s) with p > 7 doesn't fit in a double without loss
                    return false;
                }
            }

            // Well-behaved implicit casts are injective
            return new TypeCoercion(metadata::getType).canCoerce(source, target);
        }

        private Object coerce(Object value, Signature coercion)
        {
            return functionInvoker.invoke(coercion, session.toConnectorSession(), value);
        }
    }

    private static int compare(Type type, Object first, Object second)
    {
        return type.compareTo(
                Utils.nativeValueToBlock(type, first),
                0,
                Utils.nativeValueToBlock(type, second),
                0);
    }

    private static Expression falseIfNotNull(Expression argument)
    {
        return and(new IsNullPredicate(argument), new NullLiteral());
    }

    private static Expression trueIfNotNull(Expression argument)
    {
        return or(new IsNotNullPredicate(argument), new NullLiteral());
    }
}
