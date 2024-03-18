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
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.ArithmeticBinaryExpression;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.ComparisonExpression;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.ExpressionRewriter;
import io.trino.sql.ir.ExpressionTreeRewriter;
import io.trino.sql.ir.FunctionCall;
import io.trino.sql.ir.SymbolReference;
import io.trino.sql.planner.IrTypeAnalyzer;
import io.trino.sql.planner.TypeProvider;

import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.sql.ir.ArithmeticBinaryExpression.Operator.ADD;
import static io.trino.sql.ir.ArithmeticBinaryExpression.Operator.MULTIPLY;
import static java.util.Objects.requireNonNull;

public final class CanonicalizeExpressionRewriter
{
    public static Expression canonicalizeExpression(Expression expression, IrTypeAnalyzer typeAnalyzer, PlannerContext plannerContext, TypeProvider types)
    {
        return ExpressionTreeRewriter.rewriteWith(new Visitor(plannerContext, typeAnalyzer, types), expression);
    }

    private CanonicalizeExpressionRewriter() {}

    public static Expression rewrite(Expression expression, PlannerContext plannerContext, IrTypeAnalyzer typeAnalyzer, TypeProvider types)
    {
        requireNonNull(typeAnalyzer, "typeAnalyzer is null");

        if (expression instanceof SymbolReference) {
            return expression;
        }

        return ExpressionTreeRewriter.rewriteWith(new Visitor(plannerContext, typeAnalyzer, types), expression);
    }

    private static class Visitor
            extends ExpressionRewriter<Void>
    {
        private final PlannerContext plannerContext;
        private final IrTypeAnalyzer typeAnalyzer;
        private final TypeProvider types;

        public Visitor(PlannerContext plannerContext, IrTypeAnalyzer typeAnalyzer, TypeProvider types)
        {
            this.plannerContext = plannerContext;
            this.typeAnalyzer = typeAnalyzer;
            this.types = types;
        }

        @SuppressWarnings("ArgumentSelectionDefectChecker")
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

        @SuppressWarnings("ArgumentSelectionDefectChecker")
        @Override
        public Expression rewriteArithmeticBinary(ArithmeticBinaryExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            if (node.getOperator() == MULTIPLY || node.getOperator() == ADD) {
                // if we have a operation of the form <constant> [+|*] <expr>, normalize it to
                // <expr> [+|*] <constant>
                if (isConstant(node.getLeft()) && !isConstant(node.getRight())) {
                    node = new ArithmeticBinaryExpression(
                            plannerContext.getMetadata().resolveOperator(
                                    switch (node.getOperator()) {
                                        case ADD -> OperatorType.ADD;
                                        case MULTIPLY -> OperatorType.MULTIPLY;
                                        default -> throw new IllegalStateException("Unexpected value: " + node.getOperator());
                                    },
                                    ImmutableList.of(
                                            node.getFunction().getSignature().getArgumentType(1),
                                            node.getFunction().getSignature().getArgumentType(0))),
                            node.getOperator(),
                            node.getRight(),
                            node.getLeft());
                }
            }

            return treeRewriter.defaultRewrite(node, context);
        }

        @Override
        public Expression rewriteFunctionCall(FunctionCall node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            CatalogSchemaFunctionName functionName = node.getFunction().getName();
            if (functionName.equals(builtinFunctionName("date")) && node.getArguments().size() == 1) {
                Expression argument = node.getArguments().get(0);
                Type argumentType = typeAnalyzer.getType(types, argument);
                if (argumentType instanceof TimestampType
                        || argumentType instanceof TimestampWithTimeZoneType
                        || argumentType instanceof VarcharType) {
                    // prefer `CAST(x as DATE)` to `date(x)`, see e.g. UnwrapCastInComparison
                    return new Cast(treeRewriter.rewrite(argument, context), DATE);
                }
            }

            return treeRewriter.defaultRewrite(node, context);
        }

        private boolean isConstant(Expression expression)
        {
            return expression instanceof Constant;
        }
    }
}
