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
package io.trino.sql.planner;

import io.trino.metadata.Metadata;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.type.RowType;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Let;
import io.trino.sql.ir.Reference;
import io.trino.type.CharVarcharCoercion;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.operator.scalar.RowNullnessFunction.IS_NOT_NULL_NAME;
import static io.trino.operator.scalar.RowNullnessFunction.IS_NULL_NAME;
import static io.trino.sql.ir.IrExpressions.ifExpression;
import static io.trino.sql.ir.IrExpressions.not;
import static io.trino.sql.planner.LogicalPlanner.failFunction;
import static java.util.Objects.requireNonNull;

final class ExpressionBindings
{
    private final Metadata metadata;
    private final CharVarcharCoercion charVarcharCoercion;
    private final SymbolAllocator symbolAllocator;

    ExpressionBindings(Metadata metadata, CharVarcharCoercion charVarcharCoercion, SymbolAllocator symbolAllocator)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.charVarcharCoercion = requireNonNull(charVarcharCoercion, "charVarcharCoercion is null");
        this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
    }

    Expression bindIfNonTrivial(String name, Expression expression, List<Symbol> symbols, List<Expression> bindings)
    {
        if (expression instanceof Reference || expression instanceof Constant) {
            return expression;
        }
        Symbol bound = symbolAllocator.newSymbol(name, expression.type());
        symbols.add(bound);
        bindings.add(expression);
        return new Reference(expression.type(), bound.name());
    }

    /// SQL:2023 §6.12: COALESCE(V1, V2) is CASE WHEN NOT V1 IS NULL THEN V1 ELSE V2 END.
    /// For rows that is NOT ($row_is_null), not IS NOT NULL. IR Coalesce stays value-nullness.
    Expression rowCoalesce(List<Expression> operands)
    {
        checkArgument(!operands.isEmpty(), "operands is empty");
        if (operands.stream().noneMatch(operand -> operand.type() instanceof RowType)) {
            return new Coalesce(operands);
        }
        checkState(
                operands.stream().allMatch(operand -> operand.type() instanceof RowType),
                "COALESCE of a row must have row-typed operands: %s",
                operands.stream().map(Expression::type).toList());
        Expression result = operands.getLast();
        for (int i = operands.size() - 2; i >= 0; i--) {
            result = rowCoalesceOperand(operands.get(i), result);
        }
        return result;
    }

    Expression nullNotAllowedColumn(Expression expression, String columnName, ErrorCodeSupplier errorCode)
    {
        Expression fail = new Cast(
                failFunction(metadata, charVarcharCoercion, errorCode, "NULL value not allowed for NOT NULL column: " + columnName),
                expression.type());
        if (!(expression.type() instanceof RowType)) {
            return new Coalesce(expression, fail);
        }
        List<Symbol> symbols = new ArrayList<>();
        List<Expression> bindings = new ArrayList<>();
        Expression bound = bindIfNonTrivial("row_not_null", expression, symbols, bindings);
        Expression result = ifExpression(rowIsNotNull(bound), bound, fail);
        for (int i = symbols.size() - 1; i >= 0; i--) {
            result = new Let(symbols.get(i), bindings.get(i), result);
        }
        return result;
    }

    private Expression rowCoalesceOperand(Expression operand, Expression rest)
    {
        List<Symbol> symbols = new ArrayList<>();
        List<Expression> bindings = new ArrayList<>();
        Expression bound = bindIfNonTrivial("row_coalesce", operand, symbols, bindings);
        Expression result = ifExpression(not(metadata, charVarcharCoercion, rowIsNull(bound)), bound, rest);
        for (int i = symbols.size() - 1; i >= 0; i--) {
            result = new Let(symbols.get(i), bindings.get(i), result);
        }
        return result;
    }

    private Call rowIsNull(Expression row)
    {
        return BuiltinFunctionCallBuilder.resolve(metadata, charVarcharCoercion)
                .setName(IS_NULL_NAME)
                .addArgument(row.type(), row)
                .build();
    }

    private Call rowIsNotNull(Expression row)
    {
        return BuiltinFunctionCallBuilder.resolve(metadata, charVarcharCoercion)
                .setName(IS_NOT_NULL_NAME)
                .addArgument(row.type(), row)
                .build();
    }
}
