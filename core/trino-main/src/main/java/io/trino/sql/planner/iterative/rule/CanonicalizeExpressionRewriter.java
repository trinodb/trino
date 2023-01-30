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
import io.trino.spi.type.DateType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.ExpressionRewriter;
import io.trino.sql.tree.ExpressionTreeRewriter;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.IfExpression;
import io.trino.sql.tree.IsNotNullPredicate;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.SearchedCaseExpression;
import io.trino.sql.tree.SymbolReference;
import io.trino.sql.tree.WhenClause;

import java.util.Map;
import java.util.Optional;

import static io.trino.metadata.ResolvedFunction.extractFunctionName;
import static io.trino.sql.ExpressionUtils.isEffectivelyLiteral;
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
        private final Map<NodeRef<Expression>, Type> expressionTypes;

        public Visitor(Session session, PlannerContext plannerContext, Map<NodeRef<Expression>, Type> expressionTypes)
        {
            this.session = session;
            this.plannerContext = plannerContext;
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

            Optional<Expression> falseValue = node.getFalseValue().map(value -> treeRewriter.rewrite(value, context));

            return new SearchedCaseExpression(ImmutableList.of(new WhenClause(condition, trueValue)), falseValue);
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
                    // prefer `CAST(x as DATE)` to `date(x)`, see e.g. UnwrapCastInComparison
                    return new Cast(treeRewriter.rewrite(argument, context), toSqlType(DateType.DATE));
                }
            }

            return treeRewriter.defaultRewrite(node, context);
        }

        private boolean isConstant(Expression expression)
        {
            // Current IR has no way to represent typed constants. It encodes simple ones as Cast(Literal)
            // This is the simplest possible check that
            //   1) doesn't require ExpressionInterpreter.optimize(), which is not cheap
            //   2) doesn't try to duplicate all the logic in LiteralEncoder
            //   3) covers a sufficient portion of the use cases that occur in practice
            // TODO: this should eventually be removed when IR includes types
            return isEffectivelyLiteral(plannerContext, session, expression);
        }
    }
}
