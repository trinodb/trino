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

import com.google.common.collect.ImmutableList;
import com.google.common.math.LongMath;
import io.trino.Session;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.ExpressionInterpreter;
import io.trino.sql.planner.FunctionCallBuilder;
import io.trino.sql.planner.LiteralEncoder;
import io.trino.sql.planner.NoOpSymbolResolver;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.ComparisonExpression.Operator;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.ExpressionTreeRewriter;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.IsNotNullPredicate;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.StringLiteral;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.metadata.ResolvedFunction.extractFunctionName;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static java.lang.Math.toIntExact;
import static java.math.RoundingMode.UNNECESSARY;
import static java.util.Objects.requireNonNull;

/**
 * Transforms a constant date/time expression t
 * <pre>
 *     year(date_time) = t
 * </pre>
 * <p>
 * into
 * <pre>
 *     date_trunc('year', date_time) = (beginning of the year t)
 * </pre>
 * <p>
 *
 * @see UnwrapDateTruncInComparison
 */
public class RewriteYearFunctionToDateTrunc
        extends ExpressionRewriteRuleSet
{
    public RewriteYearFunctionToDateTrunc(PlannerContext plannerContext, TypeAnalyzer typeAnalyzer)
    {
        super(createRewrite(plannerContext, typeAnalyzer));
    }

    private static ExpressionRewriter createRewrite(PlannerContext plannerContext, TypeAnalyzer typeAnalyzer)
    {
        requireNonNull(plannerContext, "plannerContext is null");
        requireNonNull(typeAnalyzer, "typeAnalyzer is null");

        return (expression, context) -> rewriteYearFunctionToDateTrunc(context.getSession(), plannerContext, typeAnalyzer, context.getSymbolAllocator().getTypes(), expression);
    }

    private static Expression rewriteYearFunctionToDateTrunc(Session session,
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
        private final LiteralEncoder literalEncoder;

        public Visitor(PlannerContext plannerContext, TypeAnalyzer typeAnalyzer, Session session, TypeProvider types)
        {
            this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
            this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
            this.session = requireNonNull(session, "session is null");
            this.types = requireNonNull(types, "types is null");
            this.literalEncoder = new LiteralEncoder(plannerContext);
        }

        @Override
        public Expression rewriteComparisonExpression(ComparisonExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            ComparisonExpression expression = (ComparisonExpression) treeRewriter.defaultRewrite((Expression) node, null);
            return rewriteYearFunctionToDateTrunc(expression);
        }

        private Expression rewriteYearFunctionToDateTrunc(ComparisonExpression expression)
        {
            // Expect year on the left side and value on the right side of the comparison.
            // This is provided by CanonicalizeExpressionRewriter.
            if (!(expression.getLeft() instanceof FunctionCall call) ||
                    !extractFunctionName(call.getName()).equals("year") ||
                    call.getArguments().size() != 1) {
                return expression;
            }

            Map<NodeRef<Expression>, Type> expressionTypes = typeAnalyzer.getTypes(session, types, expression);

            Expression argument = getOnlyElement(call.getArguments());
            Type argumentType = expressionTypes.get(NodeRef.of(argument));

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
            if (argumentType instanceof TimestampWithTimeZoneType) {
                // Cannot replace with a range due to how year operates on value's local date/time.
                // I.e. unwrapping is possible only when values are all of some fixed zone and the zone is known.
                return expression;
            }
            if (argumentType != DATE && !(argumentType instanceof TimestampType)) {
                // e.g. year(INTERVAL) not handled here
                return expression;
            }

            int year = toIntExact((Long) right);
            return dateTruncExpression(expression.getOperator(), argument, argumentType, toExpression(firstDayOfYear(year, argumentType), argumentType));
        }

        private Object firstDayOfYear(int year, Type type)
        {
            if (type == DATE) {
                LocalDate firstDay = LocalDate.ofYearDay(year, 1);
                return firstDay.toEpochDay();
            }
            if (type instanceof TimestampType timestampType) {
                if (timestampType.isShort()) {
                    LocalDateTime dateTime = LocalDateTime.of(year, 1, 1, 0, 0);
                    return dateTime.toEpochSecond(ZoneOffset.UTC) * MICROSECONDS_PER_SECOND + LongMath.divide(dateTime.getNano(), NANOSECONDS_PER_MICROSECOND, UNNECESSARY);
                }
                LocalDateTime dateTime = LocalDateTime.of(year, 1, 1, 0, 0);
                long endInclusiveMicros = dateTime.toEpochSecond(ZoneOffset.UTC) * MICROSECONDS_PER_SECOND
                        + LongMath.divide(dateTime.getNano(), NANOSECONDS_PER_MICROSECOND, UNNECESSARY);
                return new LongTimestamp(endInclusiveMicros, 0);
            }
            throw new UnsupportedOperationException("Unsupported type: " + type);
        }

        private ComparisonExpression dateTruncExpression(Operator operator, Expression argument, Type type, Expression right)
        {
            FunctionCall functionCall = FunctionCallBuilder.resolve(session, plannerContext.getMetadata())
                    .setName(QualifiedName.of("date_trunc"))
                    .setArguments(
                            ImmutableList.of(VARCHAR, type),
                            ImmutableList.of(new StringLiteral("year"), argument))
                    .build();

            return new ComparisonExpression(operator, functionCall, right);
        }

        private Expression toExpression(Object value, Type type)
        {
            return literalEncoder.toExpression(session, value, type);
        }
    }
}
