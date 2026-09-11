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
package io.trino.sql.ir.optimizer.rule;

import io.trino.Session;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.NumberType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.TrinoNumber;
import io.trino.spi.type.TrinoNumber.BigDecimalValue;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.type.IntervalDayTimeType;
import io.trino.type.IntervalYearMonthType;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.spi.function.OperatorType.ADD;
import static io.trino.spi.function.OperatorType.DIVIDE;
import static io.trino.spi.function.OperatorType.MULTIPLY;
import static io.trino.spi.function.OperatorType.SUBTRACT;
import static io.trino.spi.type.Decimals.longTenToNth;
import static io.trino.spi.type.Int128Math.powerOfTen;
import static io.trino.sql.analyzer.ExpressionAnalyzer.isNumericType;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;

/**
 * Removes arithmetic with a constant identity operand:
 * <ul>
 *     <li>{@code x + 0}, {@code 0 + x}, {@code x - 0 -> x}
 *     <li>{@code x + INTERVAL '0' DAY}, {@code x - INTERVAL '0' DAY -> x}
 *     <li>{@code x * 1}, {@code 1 * x}, {@code x / 1 -> x}
 * </ul>
 * <p>
 * The remaining operand must already have the operation's result type. Decimal arithmetic widens it:
 * {@code decimal(10,2) + decimal(10,0)} is a {@code decimal(13,2)}, so dropping the addition there
 * would change the expression's type. Interval arithmetic widens the temporal precision to at least 3:
 * {@code timestamp(0) + INTERVAL '0' DAY} is a {@code timestamp(3)}.
 * <p>
 * The additive identities do not cover {@code real} and {@code double}, which have two zeros:
 * {@code -0.0 + 0.0} and {@code -0.0 - -0.0} are both {@code 0.0}. The multiplicative identities do
 * cover them, since multiplying or dividing by one keeps the sign of a zero. They are in turn limited
 * to numbers, because scaling an interval goes through {@code double}, which is lossy:
 * {@code INTERVAL '999999999 00:00:00.001' DAY TO SECOND * 1e0} drops the millisecond.
 */
public class RemoveRedundantArithmetic
        implements IrOptimizerRule
{
    @Override
    public Optional<Expression> apply(Expression expression, Session session, SymbolAllocator symbolAllocator, Map<Symbol, Expression> bindings)
    {
        if (!(expression instanceof Call(ResolvedFunction function, List<Expression> arguments)) || arguments.size() != 2) {
            return Optional.empty();
        }

        Expression left = arguments.get(0);
        Expression right = arguments.get(1);
        Type type = expression.type();
        CatalogSchemaFunctionName name = function.name();

        if (name.equals(builtinFunctionName(ADD))) {
            if (isZero(right) && left.type().equals(type)) {
                return Optional.of(left);
            }
            if (isZero(left) && right.type().equals(type)) {
                return Optional.of(right);
            }
        }
        else if (name.equals(builtinFunctionName(SUBTRACT))) {
            if (isZero(right) && left.type().equals(type)) {
                return Optional.of(left);
            }
        }
        else if (name.equals(builtinFunctionName(MULTIPLY)) && isNumber(type)) {
            if (isOne(right) && left.type().equals(type)) {
                return Optional.of(left);
            }
            if (isOne(left) && right.type().equals(type)) {
                return Optional.of(right);
            }
        }
        else if (name.equals(builtinFunctionName(DIVIDE)) && isNumber(type)) {
            if (isOne(right) && left.type().equals(type)) {
                return Optional.of(left);
            }
        }

        return Optional.empty();
    }

    /**
     * {@link io.trino.sql.analyzer.ExpressionAnalyzer#isNumericType} does not cover {@code number}.
     */
    private static boolean isNumber(Type type)
    {
        return isNumericType(type) || type instanceof NumberType;
    }

    private static boolean isZero(Expression expression)
    {
        if (!(expression instanceof Constant(Type type, Object value)) || value == null) {
            return false;
        }

        return switch (type) {
            case TinyintType _, SmallintType _, IntegerType _, BigintType _ -> (long) value == 0;
            case IntervalDayTimeType _, IntervalYearMonthType _ -> (long) value == 0;
            case DecimalType decimal -> decimal.isShort() ? (long) value == 0 : ((Int128) value).isZero();
            case NumberType _ -> ((TrinoNumber) value).toBigDecimal() instanceof BigDecimalValue(BigDecimal number) && number.signum() == 0;
            default -> false;
        };
    }

    private static boolean isOne(Expression expression)
    {
        if (!(expression instanceof Constant(Type type, Object value)) || value == null) {
            return false;
        }

        return switch (type) {
            case TinyintType _, SmallintType _, IntegerType _, BigintType _ -> (long) value == 1;
            case RealType _ -> intBitsToFloat(toIntExact((long) value)) == 1;
            case DoubleType _ -> (double) value == 1;
            case DecimalType decimal -> decimal.isShort() ?
                    (long) value == longTenToNth(decimal.getScale()) :
                    value.equals(powerOfTen(decimal.getScale()));
            case NumberType _ -> ((TrinoNumber) value).toBigDecimal() instanceof BigDecimalValue(BigDecimal number) && number.compareTo(BigDecimal.ONE) == 0;
            default -> false;
        };
    }
}
