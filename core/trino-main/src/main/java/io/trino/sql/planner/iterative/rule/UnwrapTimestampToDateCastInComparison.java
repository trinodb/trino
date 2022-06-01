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

import io.trino.Session;
import io.trino.metadata.OperatorNotFoundException;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.type.DateType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.sql.InterpretedFunctionInvoker;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.ExpressionInterpreter;
import io.trino.sql.planner.LiteralEncoder;
import io.trino.sql.planner.NoOpSymbolResolver;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.ExpressionTreeRewriter;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.NullLiteral;

import java.util.Optional;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.sql.ExpressionUtils.and;
import static io.trino.sql.ExpressionUtils.or;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN;
import static java.util.Objects.requireNonNull;

/**
 * Rewrites CAST(ts_column as DATE) OP date_literal to range expression on ts_column. Dropping cast
 * allows for further optimizations, such as pushdown into connectors.
 * <p>
 * TODO: replace with more general mechanism supporting broader range of types
 *
 * @see io.trino.sql.planner.iterative.rule.UnwrapCastInComparison
 */
public class UnwrapTimestampToDateCastInComparison
        extends ExpressionRewriteRuleSet
{
    public UnwrapTimestampToDateCastInComparison(PlannerContext plannerContext, TypeAnalyzer typeAnalyzer)
    {
        super(createRewrite(plannerContext, typeAnalyzer));
    }

    private static ExpressionRewriter createRewrite(PlannerContext plannerContext, TypeAnalyzer typeAnalyzer)
    {
        requireNonNull(plannerContext, "plannerContext is null");
        requireNonNull(typeAnalyzer, "typeAnalyzer is null");

        return (expression, context) -> unwrapCasts(context.getSession(), plannerContext, typeAnalyzer, context.getSymbolAllocator().getTypes(), expression);
    }

    public static Expression unwrapCasts(Session session,
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
            return unwrapCast(expression);
        }

        private Expression unwrapCast(ComparisonExpression expression)
        {
            // Canonicalization is handled by CanonicalizeExpressionRewriter
            if (!(expression.getLeft() instanceof Cast)) {
                return expression;
            }

            Object right = new ExpressionInterpreter(expression.getRight(), plannerContext, session, typeAnalyzer.getTypes(session, types, expression.getRight()))
                    .optimize(NoOpSymbolResolver.INSTANCE);

            Cast cast = (Cast) expression.getLeft();
            ComparisonExpression.Operator operator = expression.getOperator();

            if (right == null || right instanceof NullLiteral) {
                // handled by general UnwrapCastInComparison
                return expression;
            }

            if (right instanceof Expression) {
                return expression;
            }

            Type sourceType = typeAnalyzer.getType(session, types, cast.getExpression());
            Type targetType = typeAnalyzer.getType(session, types, expression.getRight());

            if (sourceType instanceof TimestampType && targetType == DATE) {
                return unwrapTimestampToDateCast(session, (TimestampType) sourceType, (DateType) targetType, operator, cast.getExpression(), (long) right).orElse(expression);
            }

            return expression;
        }

        private Optional<Expression> unwrapTimestampToDateCast(Session session, TimestampType sourceType, DateType targetType, ComparisonExpression.Operator operator, Expression timestampExpression, long date)
        {
            ResolvedFunction targetToSource;
            try {
                targetToSource = plannerContext.getMetadata().getCoercion(session, targetType, sourceType);
            }
            catch (OperatorNotFoundException e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
            }

            Expression dateTimestamp = literalEncoder.toExpression(session, coerce(date, targetToSource), sourceType);
            Expression nextDateTimestamp = literalEncoder.toExpression(session, coerce(date + 1, targetToSource), sourceType);

            switch (operator) {
                case EQUAL:
                    return Optional.of(
                            and(
                                    new ComparisonExpression(GREATER_THAN_OR_EQUAL, timestampExpression, dateTimestamp),
                                    new ComparisonExpression(LESS_THAN, timestampExpression, nextDateTimestamp)));
                case NOT_EQUAL:
                    return Optional.of(
                            or(
                                    new ComparisonExpression(LESS_THAN, timestampExpression, dateTimestamp),
                                    new ComparisonExpression(GREATER_THAN_OR_EQUAL, timestampExpression, nextDateTimestamp)));
                case LESS_THAN:
                    return Optional.of(new ComparisonExpression(LESS_THAN, timestampExpression, dateTimestamp));
                case LESS_THAN_OR_EQUAL:
                    return Optional.of(new ComparisonExpression(LESS_THAN, timestampExpression, nextDateTimestamp));
                case GREATER_THAN:
                    return Optional.of(new ComparisonExpression(GREATER_THAN_OR_EQUAL, timestampExpression, nextDateTimestamp));
                case GREATER_THAN_OR_EQUAL:
                    return Optional.of(new ComparisonExpression(GREATER_THAN_OR_EQUAL, timestampExpression, dateTimestamp));
                case IS_DISTINCT_FROM:
                    return Optional.of(
                            or(
                                    new IsNullPredicate(timestampExpression),
                                    new ComparisonExpression(LESS_THAN, timestampExpression, dateTimestamp),
                                    new ComparisonExpression(GREATER_THAN_OR_EQUAL, timestampExpression, nextDateTimestamp)));
            }
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unsupported operator: " + operator);
        }

        private Object coerce(Object value, ResolvedFunction coercion)
        {
            return functionInvoker.invoke(coercion, session.toConnectorSession(), value);
        }
    }
}
