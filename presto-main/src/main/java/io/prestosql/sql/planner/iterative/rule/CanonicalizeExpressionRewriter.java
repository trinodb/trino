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

import com.google.common.collect.ImmutableList;
import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.operator.scalar.FormatFunction;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.FunctionCallBuilder;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.tree.ArithmeticBinaryExpression;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.CurrentTime;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.ExpressionRewriter;
import io.prestosql.sql.tree.ExpressionTreeRewriter;
import io.prestosql.sql.tree.Extract;
import io.prestosql.sql.tree.Format;
import io.prestosql.sql.tree.IfExpression;
import io.prestosql.sql.tree.IsNotNullPredicate;
import io.prestosql.sql.tree.IsNullPredicate;
import io.prestosql.sql.tree.Literal;
import io.prestosql.sql.tree.NodeRef;
import io.prestosql.sql.tree.NotExpression;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.Row;
import io.prestosql.sql.tree.SearchedCaseExpression;
import io.prestosql.sql.tree.SymbolReference;
import io.prestosql.sql.tree.WhenClause;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.sql.tree.ArithmeticBinaryExpression.Operator.ADD;
import static io.prestosql.sql.tree.ArithmeticBinaryExpression.Operator.MULTIPLY;
import static java.util.Objects.requireNonNull;

public final class CanonicalizeExpressionRewriter
{
    public static Expression canonicalizeExpression(Expression expression, Map<NodeRef<Expression>, Type> expressionTypes, Metadata metadata)
    {
        return ExpressionTreeRewriter.rewriteWith(new Visitor(metadata, expressionTypes), expression);
    }

    private CanonicalizeExpressionRewriter() {}

    public static Expression rewrite(Expression expression, Session session, Metadata metadata, TypeAnalyzer typeAnalyzer, TypeProvider types)
    {
        requireNonNull(metadata, "metadata is null");
        requireNonNull(typeAnalyzer, "typeAnalyzer is null");

        if (expression instanceof SymbolReference) {
            return expression;
        }
        Map<NodeRef<Expression>, Type> expressionTypes = typeAnalyzer.getTypes(session, types, expression);

        return ExpressionTreeRewriter.rewriteWith(new Visitor(metadata, expressionTypes), expression);
    }

    private static class Visitor
            extends ExpressionRewriter<Void>
    {
        private final Metadata metadata;
        private final Map<NodeRef<Expression>, Type> expressionTypes;

        public Visitor(Metadata metadata, Map<NodeRef<Expression>, Type> expressionTypes)
        {
            this.metadata = metadata;
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
            if (node.getPrecision() != null) {
                throw new UnsupportedOperationException("not yet implemented: non-default precision");
            }

            switch (node.getFunction()) {
                case DATE:
                    return new FunctionCallBuilder(metadata)
                            .setName(QualifiedName.of("current_date"))
                            .build();
                case TIME:
                    return new FunctionCallBuilder(metadata)
                            .setName(QualifiedName.of("current_time"))
                            .build();
                case LOCALTIME:
                    return new FunctionCallBuilder(metadata)
                            .setName(QualifiedName.of("localtime"))
                            .build();
                case TIMESTAMP:
                    return new FunctionCallBuilder(metadata)
                            .setName(QualifiedName.of("current_timestamp"))
                            .build();
                case LOCALTIMESTAMP:
                    return new FunctionCallBuilder(metadata)
                            .setName(QualifiedName.of("localtimestamp"))
                            .build();
                default:
                    throw new UnsupportedOperationException("not yet implemented: " + node.getFunction());
            }
        }

        @Override
        public Expression rewriteExtract(Extract node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            Expression value = treeRewriter.rewrite(node.getExpression(), context);
            Type type = expressionTypes.get(NodeRef.of(node.getExpression()));

            switch (node.getField()) {
                case YEAR:
                    return new FunctionCallBuilder(metadata)
                            .setName(QualifiedName.of("year"))
                            .addArgument(type, value)
                            .build();
                case QUARTER:
                    return new FunctionCallBuilder(metadata)
                            .setName(QualifiedName.of("quarter"))
                            .addArgument(type, value)
                            .build();
                case MONTH:
                    return new FunctionCallBuilder(metadata)
                            .setName(QualifiedName.of("month"))
                            .addArgument(type, value)
                            .build();
                case WEEK:
                    return new FunctionCallBuilder(metadata)
                            .setName(QualifiedName.of("week"))
                            .addArgument(type, value)
                            .build();
                case DAY:
                case DAY_OF_MONTH:
                    return new FunctionCallBuilder(metadata)
                            .setName(QualifiedName.of("day"))
                            .addArgument(type, value)
                            .build();
                case DAY_OF_WEEK:
                case DOW:
                    return new FunctionCallBuilder(metadata)
                            .setName(QualifiedName.of("day_of_week"))
                            .addArgument(type, value)
                            .build();
                case DAY_OF_YEAR:
                case DOY:
                    return new FunctionCallBuilder(metadata)
                            .setName(QualifiedName.of("day_of_year"))
                            .addArgument(type, value)
                            .build();
                case YEAR_OF_WEEK:
                case YOW:
                    return new FunctionCallBuilder(metadata)
                            .setName(QualifiedName.of("year_of_week"))
                            .addArgument(type, value)
                            .build();
                case HOUR:
                    return new FunctionCallBuilder(metadata)
                            .setName(QualifiedName.of("hour"))
                            .addArgument(type, value)
                            .build();
                case MINUTE:
                    return new FunctionCallBuilder(metadata)
                            .setName(QualifiedName.of("minute"))
                            .addArgument(type, value)
                            .build();
                case SECOND:
                    return new FunctionCallBuilder(metadata)
                            .setName(QualifiedName.of("second"))
                            .addArgument(type, value)
                            .build();
                case TIMEZONE_MINUTE:
                    return new FunctionCallBuilder(metadata)
                            .setName(QualifiedName.of("timezone_minute"))
                            .addArgument(type, value)
                            .build();
                case TIMEZONE_HOUR:
                    return new FunctionCallBuilder(metadata)
                            .setName(QualifiedName.of("timezone_hour"))
                            .addArgument(type, value)
                            .build();
            }

            throw new UnsupportedOperationException("not yet implemented: " + node.getField());
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

            return new FunctionCallBuilder(metadata)
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
