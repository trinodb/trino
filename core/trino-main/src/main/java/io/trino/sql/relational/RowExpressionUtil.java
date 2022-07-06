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
package io.trino.sql.relational;

import com.google.common.collect.Iterables;
import io.airlift.slice.Slice;
import io.trino.Session;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.type.RowType;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.LiteralEncoder;
import io.trino.sql.planner.NoOpSymbolResolver;
import io.trino.sql.planner.RowExpressionInterpreter;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.Literal;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.StringLiteral;
import io.trino.type.UnknownType;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.metadata.LiteralFunction.LITERAL_FUNCTION_NAME;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static io.trino.sql.relational.OriginalExpressionUtils.castToExpression;
import static io.trino.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static io.trino.sql.relational.OriginalExpressionUtils.isExpression;
import static io.trino.sql.relational.StandardFunctionResolution.isCastFunction;

public class RowExpressionUtil
{
    private RowExpressionUtil() {}

    /**
     * Returns whether expression is effectively literal. An effectively literal expression is a simple constant value, or null,
     * in either {@link Literal} form, or other form returned by {@link LiteralEncoder}. In particular, other constant expressions
     * like a deterministic function call with constant arguments are not considered effectively literal.
     */
    public static boolean isEffectivelyLiteral(PlannerContext plannerContext, Session session, RowExpression expression)
    {
        if (expression instanceof ConstantExpression) {
            return true;
        }

        if (expression instanceof CallExpression) {
            CallExpression callExpression = (CallExpression) expression;
            ResolvedFunction resolvedFunction = callExpression.getResolvedFunction();

            // need testing on TRY_CAST
            if (isCastFunction(resolvedFunction)) {
                RowExpression argument = Iterables.getOnlyElement(callExpression.getArguments());
                return (argument instanceof ConstantExpression && constantExpressionEvaluatesSuccessfully(plannerContext, session, expression));
            }

            return LITERAL_FUNCTION_NAME.equals(resolvedFunction.getSignature().getName());
        }
        return false;
    }

    private static boolean constantExpressionEvaluatesSuccessfully(PlannerContext plannerContext, Session session, RowExpression constantExpression)
    {
        RowExpressionInterpreter interpreter = new RowExpressionInterpreter(constantExpression, plannerContext, session);
        Object literalValue = interpreter.optimize(NoOpSymbolResolver.INSTANCE);
        return !(literalValue instanceof RowExpression);
    }

    public static boolean isRow(RowExpression row)
    {
        if (isExpression(row)) {
            return castToExpression(row) instanceof Row;
        }
        else {
            return row instanceof SpecialForm && ((SpecialForm) row).getForm().equals(SpecialForm.Form.ROW_CONSTRUCTOR);
        }
    }

    public static RowExpression toRowConstructorExpression(RowExpression argument)
    {
        return toRowConstructorExpression(List.of(argument));
    }

    public static RowExpression toRowConstructorExpression(List<RowExpression> arguments)
    {
        if (arguments.stream().anyMatch(OriginalExpressionUtils::isExpression)) {
            return castToRowExpression(new Row(arguments.stream().map(OriginalExpressionUtils::castToExpression).collect(toImmutableList())));
        }
        else {
            if (arguments.size() == 0) {
                return new SpecialForm(SpecialForm.Form.ROW_CONSTRUCTOR, UnknownType.UNKNOWN, arguments);
            }
            else {
                return new SpecialForm(SpecialForm.Form.ROW_CONSTRUCTOR, RowType.anonymous(arguments.stream().map(RowExpression::getType).collect(toImmutableList())), arguments);
            }
        }
    }

    // Hacky method to convert a RowExpression in ValuesNode to Expression, the RowExpression expression could be a constant, a cast, or a
    // random function, we should not use this method to convert  RowExpression to Expression when all the PlanNodes refer to RowExpression
    public static Expression castRowRowExpressionToExpression(RowExpression rowExpression)
    {
        if (isExpression(rowExpression)) {
            return castToExpression(rowExpression);
        }
        if (rowExpression instanceof CallExpression) {
            CallExpression callExpression = (CallExpression) rowExpression;
            if (isCastFunction(callExpression.getResolvedFunction())) {
                return new Cast(Iterables.getOnlyElement(((CallExpression) rowExpression).getArguments().stream().map(RowExpressionUtil::castRowRowExpressionToExpression).collect(toImmutableList())), toSqlType(callExpression.getType()));
            }
            else {
                // should be a random function here
                checkArgument(callExpression.getArguments().size() == 0);
                return new FunctionCall(callExpression.getResolvedFunction().toQualifiedName(), ((CallExpression) rowExpression).getArguments().stream().map(RowExpressionUtil::castRowRowExpressionToExpression).collect(toImmutableList()));
            }
//            return new Cast(callExpression.getResolvedFunction().toQualifiedName(), ((CallExpression) rowExpression).getArguments().stream().map(RowExpressionUtil::castRowRowExpressionToExpression).collect(toImmutableList()));
        }
        if (rowExpression instanceof SpecialForm) {
            return new Row(((SpecialForm) rowExpression).getArguments().stream().map(RowExpressionUtil::castRowRowExpressionToExpression).collect(toImmutableList()));
        }
        ConstantExpression expression = (ConstantExpression) rowExpression;
        if (expression.getType().getJavaType() == boolean.class) {
            if (expression.getValue() == null) {
                return new NullLiteral();
            }
            return new BooleanLiteral(String.valueOf(expression.getValue()));
        }
        if (expression.getType().getJavaType() == long.class) {
            return new LongLiteral(String.valueOf(expression.getValue()));
        }
        if (expression.getType().getJavaType() == double.class) {
            return new DoubleLiteral(String.valueOf(expression.getValue()));
        }
        if (expression.getType().getJavaType() == Slice.class) {
            return new StringLiteral(new String(((Slice) expression.getValue()).getBytes(), StandardCharsets.UTF_8));
        }
        return new GenericLiteral(expression.getType().toString(), String.valueOf(expression.getValue()));
    }
}
