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
package io.trino.type;

import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TrinoNumber;

import java.math.BigDecimal;
import java.math.RoundingMode;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.function.OperatorType.ADD;
import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.spi.function.OperatorType.DIVIDE;
import static io.trino.spi.function.OperatorType.MODULUS;
import static io.trino.spi.function.OperatorType.MULTIPLY;
import static io.trino.spi.function.OperatorType.NEGATION;
import static io.trino.spi.function.OperatorType.SUBTRACT;
import static java.lang.String.format;

public final class NumberOperators
{
    private NumberOperators() {}

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.NUMBER)
    public static TrinoNumber add(@SqlType(StandardTypes.NUMBER) TrinoNumber left, @SqlType(StandardTypes.NUMBER) TrinoNumber right)
    {
        return TrinoNumber.from(left.toBigDecimal().add(right.toBigDecimal()));
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.NUMBER)
    public static TrinoNumber subtract(@SqlType(StandardTypes.NUMBER) TrinoNumber left, @SqlType(StandardTypes.NUMBER) TrinoNumber right)
    {
        return TrinoNumber.from(left.toBigDecimal().subtract(right.toBigDecimal()));
    }

    @ScalarOperator(MULTIPLY)
    @SqlType(StandardTypes.NUMBER)
    public static TrinoNumber multiply(@SqlType(StandardTypes.NUMBER) TrinoNumber left, @SqlType(StandardTypes.NUMBER) TrinoNumber right)
    {
        return TrinoNumber.from(left.toBigDecimal().multiply(right.toBigDecimal()));
    }

    @ScalarOperator(DIVIDE)
    @SqlType(StandardTypes.NUMBER)
    public static TrinoNumber divide(@SqlType(StandardTypes.NUMBER) TrinoNumber dividend, @SqlType(StandardTypes.NUMBER) TrinoNumber divisor)
    {
        BigDecimal dividendBigDecimal = dividend.toBigDecimal();
        BigDecimal divisorBigDecimal = divisor.toBigDecimal();
        // Modeled after decimal division, see DecimalOperators.decimalDivideOperator
        int resultScale = Math.max(dividendBigDecimal.scale() + divisorBigDecimal.precision() + 1, 6);
        try {
            return TrinoNumber.from(dividendBigDecimal.divide(divisorBigDecimal, resultScale, RoundingMode.HALF_UP));
        }
        catch (ArithmeticException e) {
            throw new TrinoException(DIVISION_BY_ZERO, "Division by zero", e);
        }
    }

    @ScalarOperator(MODULUS)
    @SqlType(StandardTypes.NUMBER)
    public static TrinoNumber modulus(@SqlType(StandardTypes.NUMBER) TrinoNumber dividend, @SqlType(StandardTypes.NUMBER) TrinoNumber divisor)
    {
        try {
            return TrinoNumber.from(dividend.toBigDecimal().remainder(divisor.toBigDecimal()));
        }
        catch (ArithmeticException e) {
            throw new TrinoException(DIVISION_BY_ZERO, "Division by zero", e);
        }
    }

    @ScalarOperator(NEGATION)
    @SqlType(StandardTypes.NUMBER)
    public static TrinoNumber negate(@SqlType(StandardTypes.NUMBER) TrinoNumber value)
    {
        return TrinoNumber.from(value.toBigDecimal().negate());
    }

    @ScalarOperator(CAST)
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice castToVarchar(@LiteralParameter("x") long x, @SqlType(StandardTypes.NUMBER) TrinoNumber value)
    {
        String stringValue = value.toBigDecimal().toString();
        // String is all-ASCII, so String.length() here returns actual code points count
        if (stringValue.length() <= x) {
            return utf8Slice(stringValue);
        }
        throw new TrinoException(INVALID_CAST_ARGUMENT, format("Value %s cannot be represented as varchar(%s)", stringValue, x));
    }
}
