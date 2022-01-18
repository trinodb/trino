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
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.operator.scalar.FormatFunction;
import io.trino.spi.type.DateType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.FunctionCallBuilder;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.CurrentTime;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.ExpressionRewriter;
import io.trino.sql.tree.ExpressionTreeRewriter;
import io.trino.sql.tree.Extract;
import io.trino.sql.tree.Format;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.IfExpression;
import io.trino.sql.tree.IsNotNullPredicate;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.Literal;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.SearchedCaseExpression;
import io.trino.sql.tree.SymbolReference;
import io.trino.sql.tree.WhenClause;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.metadata.ResolvedFunction.extractFunctionName;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static io.trino.sql.tree.ArithmeticBinaryExpression.Operator.ADD;
import static io.trino.sql.tree.ArithmeticBinaryExpression.Operator.MULTIPLY;
import static java.util.Objects.requireNonNull;

public final class CanonicalizeExpressionRewriter
{
    public static Expression canonicalizeExpression(Expression expression, Map<NodeRef<Expression>, Type> expressionTypes, PlannerContext plannerContext, Session session)
    {
        return ExpressionTreeRewriter.rewriteWith(new Visitor(session, plannerContext, expressionTypes), expression);
    }

    private CanonicalizeExpressionRewriter() {}

    public static Expression rewrite(Expression expression, Session session, PlannerContext plannerContext, TypeAnalyzer typeAnalyzer, TypeProvider types)
    {
        requireNonNull(plannerContext, "plannerContext is null");
        requireNonNull(typeAnalyzer, "typeAnalyzer is null");

        if (expression instanceof SymbolReference) {
            return expression;
        }
        Map<NodeRef<Expression>, Type> expressionTypes = typeAnalyzer.getTypes(session, types, expression);

        return ExpressionTreeRewriter.rewriteWith(new Visitor(session, plannerContext, expressionTypes), expression);
    }

    private static class Visitor
            extends ExpressionRewriter<Void>
    {
        private final Session session;
        private final PlannerContext plannerContext;
        private final Metadata metadata;
        private final Map<NodeRef<Expression>, Type> expressionTypes;

        public Visitor(Session session, PlannerContext plannerContext, Map<NodeRef<Expression>, Type> expressionTypes)
        {
            this.session = session;
            this.plannerContext = plannerContext;
            this.metadata = plannerContext.getMetadata();
            this.expressionTypes = expressionTypes;
        }

        @Override
        public Expression rewriteComparisonExpression(ComparisonExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            // if we have a comparison of the form <constant> <op> <expr>, normalize it to
            // <expr> <op-flipped> <constant>
            if (isConstant(node.getLeft()) && !isConstant(node.getRight())) {
                node = new ComparisonExpression(node.getOperator().flip(), node.getRight(), node.getLeft());
            }

            return treeRewriter.defaultRewrite(node, context);
        }

        @Override
        public Expression rewriteArithmeticBinary(ArithmeticBinaryExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            if (node.getOperator() == MULTIPLY || node.getOperator() == ADD) {
                // if we have a operation of the form <constant> [+|*] <expr>, normalize it to
                // <expr> [+|*] <constant>
                if (isConstant(node.getLeft()) && !isConstant(node.getRight())) {
                    node = new ArithmeticBinaryExpression(node.getOperator(), node.getRight(), node.getLeft());
                }
            }

            return treeRewriter.defaultRewrite(node, context);
        }

        @Override
        public Expression rewriteIsNotNullPredicate(IsNotNullPredicate node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            Expression value = treeRewriter.rewrite(node.getValue(), context);
            return new NotExpression(new IsNullPredicate(value));
        }

        @Override
        public Expression rewriteIfExpression(IfExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            Expression condition = treeRewriter.rewrite(node.getCondition(), context);
            Expression trueValue = treeRewriter.rewrite(node.getTrueValue(), context);

            Optional<Expression> falseValue = node.getFalseValue().map((value) -> treeRewriter.rewrite(value, context));

            return new SearchedCaseExpression(ImmutableList.of(new WhenClause(condition, trueValue)), falseValue);
        }

        @Override
        public Expression rewriteCurrentTime(CurrentTime node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            switch (node.getFunction()) {
                case DATE:
                    checkArgument(node.getPrecision() == null);
                    return FunctionCallBuilder.resolve(session, metadata)
                            .setName(QualifiedName.of("current_date"))
                            .build();
                case TIME:
                    return FunctionCallBuilder.resolve(session, metadata)
                            .setName(QualifiedName.of("$current_time"))
                            .setArguments(ImmutableList.of(expressionTypes.get(NodeRef.of(node))), ImmutableList.of(new NullLiteral()))
                            .build();
                case LOCALTIME:
                    return FunctionCallBuilder.resolve(session, metadata)
                            .setName(QualifiedName.of("$localtime"))
                            .setArguments(ImmutableList.of(expressionTypes.get(NodeRef.of(node))), ImmutableList.of(new NullLiteral()))
                            .build();
                case TIMESTAMP:
                    return FunctionCallBuilder.resolve(session, metadata)
                            .setName(QualifiedName.of("$current_timestamp"))
                            .setArguments(ImmutableList.of(expressionTypes.get(NodeRef.of(node))), ImmutableList.of(new NullLiteral()))
                            .build();
                case LOCALTIMESTAMP:
                    return FunctionCallBuilder.resolve(session, metadata)
                            .setName(QualifiedName.of("$localtimestamp"))
                            .setArguments(ImmutableList.of(expressionTypes.get(NodeRef.of(node))), ImmutableList.of(new NullLiteral()))
                            .build();
            }
            throw new UnsupportedOperationException("not yet implemented: " + node.getFunction());
        }

        @Override
        public Expression rewriteExtract(Extract node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            Expression value = treeRewriter.rewrite(node.getExpression(), context);
            Type type = expressionTypes.get(NodeRef.of(node.getExpression()));

            switch (node.getField()) {
                case YEAR:
                    return FunctionCallBuilder.resolve(session, metadata)
                            .setName(QualifiedName.of("year"))
                            .addArgument(type, value)
                            .build();
                case QUARTER:
                    return FunctionCallBuilder.resolve(session, metadata)
                            .setName(QualifiedName.of("quarter"))
                            .addArgument(type, value)
                            .build();
                case MONTH:
                    return FunctionCallBuilder.resolve(session, metadata)
                            .setName(QualifiedName.of("month"))
                            .addArgument(type, value)
                            .build();
                case WEEK:
                    return FunctionCallBuilder.resolve(session, metadata)
                            .setName(QualifiedName.of("week"))
                            .addArgument(type, value)
                            .build();
                case DAY:
                case DAY_OF_MONTH:
                    return FunctionCallBuilder.resolve(session, metadata)
                            .setName(QualifiedName.of("day"))
                            .addArgument(type, value)
                            .build();
                case DAY_OF_WEEK:
                case DOW:
                    return FunctionCallBuilder.resolve(session, metadata)
                            .setName(QualifiedName.of("day_of_week"))
                            .addArgument(type, value)
                            .build();
                case DAY_OF_YEAR:
                case DOY:
                    return FunctionCallBuilder.resolve(session, metadata)
                            .setName(QualifiedName.of("day_of_year"))
                            .addArgument(type, value)
                            .build();
                case YEAR_OF_WEEK:
                case YOW:
                    return FunctionCallBuilder.resolve(session, metadata)
                            .setName(QualifiedName.of("year_of_week"))
                            .addArgument(type, value)
                            .build();
                case HOUR:
                    return FunctionCallBuilder.resolve(session, metadata)
                            .setName(QualifiedName.of("hour"))
                            .addArgument(type, value)
                            .build();
                case MINUTE:
                    return FunctionCallBuilder.resolve(session, metadata)
                            .setName(QualifiedName.of("minute"))
                            .addArgument(type, value)
                            .build();
                case SECOND:
                    return FunctionCallBuilder.resolve(session, metadata)
                            .setName(QualifiedName.of("second"))
                            .addArgument(type, value)
                            .build();
                case TIMEZONE_MINUTE:
                    return FunctionCallBuilder.resolve(session, metadata)
                            .setName(QualifiedName.of("timezone_minute"))
                            .addArgument(type, value)
                            .build();
                case TIMEZONE_HOUR:
                    return FunctionCallBuilder.resolve(session, metadata)
                            .setName(QualifiedName.of("timezone_hour"))
                            .addArgument(type, value)
                            .build();
            }

            throw new UnsupportedOperationException("not yet implemented: " + node.getField());
        }

        @Override
        public Expression rewriteFunctionCall(FunctionCall node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            String functionName = extractFunctionName(node.getName());
            if (functionName.equals("date") && node.getArguments().size() == 1) {
                Expression argument = node.getArguments().get(0);
                Type argumentType = expressionTypes.get(NodeRef.of(argument));
                if (argumentType instanceof TimestampType
                        || argumentType instanceof TimestampWithTimeZoneType
                        || argumentType instanceof VarcharType) {
                    // prefer `CAST(x as DATE)` to `date(x)`
                    return new Cast(argument, toSqlType(DateType.DATE));
                }
            }

            return treeRewriter.defaultRewrite(node, context);
        }

        @Override
        public Expression rewriteFormat(Format node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            List<Expression> arguments = node.getArguments().stream()
                    .map(value -> treeRewriter.rewrite(value, context))
                    .collect(toImmutableList());
            List<Type> argumentTypes = node.getArguments().stream()
                    .map(NodeRef::of)
                    .map(expressionTypes::get)
                    .collect(toImmutableList());

            return FunctionCallBuilder.resolve(session, metadata)
                    .setName(QualifiedName.of(FormatFunction.NAME))
                    .addArgument(VARCHAR, arguments.get(0))
                    .addArgument(RowType.anonymous(argumentTypes.subList(1, arguments.size())), new Row(arguments.subList(1, arguments.size())))
                    .build();
        }
    }

    private static boolean isConstant(Expression expression)
    {
        // Current IR has no way to represent typed constants. It encodes simple ones as Cast(Literal)
        // This is the simplest possible check that
        //   1) doesn't require ExpressionInterpreter.optimize(), which is not cheap
        //   2) doesn't try to duplicate all the logic in LiteralEncoder
        //   3) covers a sufficient portion of the use cases that occur in practice
        // TODO: this should eventually be removed when IR includes types
        if (expression instanceof Cast && ((Cast) expression).getExpression() instanceof Literal) {
            return true;
        }

        return expression instanceof Literal;
    }
}
