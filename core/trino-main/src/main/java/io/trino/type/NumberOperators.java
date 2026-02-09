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
import io.trino.spi.type.TrinoNumber.BigDecimalValue;
import io.trino.spi.type.TrinoNumber.Infinity;
import io.trino.spi.type.TrinoNumber.NotANumber;

import java.math.BigDecimal;
import java.math.RoundingMode;

import static io.airlift.slice.Slices.utf8Slice;
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
        return switch (left.toBigDecimal()) {
            case NotANumber() -> left;
            case Infinity(boolean leftNegative) -> switch (right.toBigDecimal()) {
                case NotANumber() -> right;
                case Infinity(boolean rightNegative) -> {
                    if (leftNegative == rightNegative) {
                        yield left;
                    }
                    yield TrinoNumber.from(new NotANumber());
                }
                case BigDecimalValue _ -> left;
            };
            case BigDecimalValue(BigDecimal leftValue) -> switch (right.toBigDecimal()) {
                case NotANumber(), Infinity(boolean _) -> right;
                case BigDecimalValue(BigDecimal rightValue) -> TrinoNumber.from(leftValue.add(rightValue));
            };
        };
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.NUMBER)
    public static TrinoNumber subtract(@SqlType(StandardTypes.NUMBER) TrinoNumber left, @SqlType(StandardTypes.NUMBER) TrinoNumber right)
    {
        return switch (left.toBigDecimal()) {
            case NotANumber() -> left;
            case Infinity(boolean leftNegative) -> switch (right.toBigDecimal()) {
                case NotANumber() -> right;
                case Infinity(boolean rightNegative) -> {
                    if (leftNegative != rightNegative) {
                        yield left;
                    }
                    yield TrinoNumber.from(new NotANumber());
                }
                case BigDecimalValue _ -> left;
            };
            case BigDecimalValue(BigDecimal leftValue) -> switch (right.toBigDecimal()) {
                case NotANumber() -> right;
                case Infinity(boolean rightNegative) -> TrinoNumber.from(new Infinity(!rightNegative));
                case BigDecimalValue(BigDecimal rightValue) -> TrinoNumber.from(leftValue.subtract(rightValue));
            };
        };
    }

    @ScalarOperator(MULTIPLY)
    @SqlType(StandardTypes.NUMBER)
    public static TrinoNumber multiply(@SqlType(StandardTypes.NUMBER) TrinoNumber left, @SqlType(StandardTypes.NUMBER) TrinoNumber right)
    {
        return switch (left.toBigDecimal()) {
            case NotANumber() -> left;
            case Infinity(boolean leftNegative) -> switch (right.toBigDecimal()) {
                case NotANumber() -> right;
                case Infinity(boolean rightNegative) -> {
                    boolean resultNegative = leftNegative != rightNegative;
                    yield TrinoNumber.from(new Infinity(resultNegative));
                }
                case BigDecimalValue(BigDecimal rightValue) -> multiplyInfinityAndFinite(leftNegative, rightValue);
            };
            case BigDecimalValue(BigDecimal leftValue) -> switch (right.toBigDecimal()) {
                case NotANumber() -> right;
                case Infinity(boolean rightNegative) -> multiplyInfinityAndFinite(rightNegative, leftValue);
                case BigDecimalValue(BigDecimal rightValue) -> TrinoNumber.from(leftValue.multiply(rightValue));
            };
        };
    }

    private static TrinoNumber multiplyInfinityAndFinite(boolean infinityNegative, BigDecimal finiteValue)
    {
        int compareTo0 = finiteValue.compareTo(BigDecimal.ZERO);
        if (compareTo0 == 0) {
            return TrinoNumber.from(new NotANumber());
        }
        boolean resultNegative = infinityNegative != (compareTo0 < 0);
        return TrinoNumber.from(new Infinity(resultNegative));
    }

    @ScalarOperator(DIVIDE)
    @SqlType(StandardTypes.NUMBER)
    public static TrinoNumber divide(@SqlType(StandardTypes.NUMBER) TrinoNumber dividend, @SqlType(StandardTypes.NUMBER) TrinoNumber divisor)
    {
        return switch (dividend.toBigDecimal()) {
            case NotANumber() -> dividend;
            case Infinity(boolean dividendNegative) -> switch (divisor.toBigDecimal()) {
                case NotANumber() -> divisor;
                case Infinity(boolean _) -> TrinoNumber.from(new NotANumber());
                case BigDecimalValue(BigDecimal divisorValue) -> {
                    int compareTo0 = divisorValue.compareTo(BigDecimal.ZERO);
                    boolean resultNegative = dividendNegative != (compareTo0 < 0);
                    yield TrinoNumber.from(new Infinity(resultNegative));
                }
            };
            case BigDecimalValue(BigDecimal dividendValue) -> switch (divisor.toBigDecimal()) {
                case NotANumber() -> divisor;
                case Infinity(boolean _) -> TrinoNumber.from(BigDecimal.ZERO);
                case BigDecimalValue(BigDecimal divisorValue) -> {
                    if (divisorValue.compareTo(BigDecimal.ZERO) == 0) {
                        int dividendCompareTo0 = dividendValue.compareTo(BigDecimal.ZERO);
                        if (dividendCompareTo0 == 0) {
                            yield TrinoNumber.from(new NotANumber());
                        }
                        else {
                            yield TrinoNumber.from(new Infinity(dividendCompareTo0 < 0));
                        }
                    }
                    // Modeled after decimal division, see DecimalOperators.decimalDivideOperator
                    int resultScale = Math.max(dividendValue.scale() + divisorValue.precision() + 1, 6);
                    yield TrinoNumber.from(dividendValue.divide(divisorValue, resultScale, RoundingMode.HALF_UP));
                }
            };
        };
    }

    @ScalarOperator(MODULUS)
    @SqlType(StandardTypes.NUMBER)
    public static TrinoNumber modulus(@SqlType(StandardTypes.NUMBER) TrinoNumber dividend, @SqlType(StandardTypes.NUMBER) TrinoNumber divisor)
    {
        return switch (dividend.toBigDecimal()) {
            case NotANumber() -> dividend;
            case Infinity(boolean _) -> TrinoNumber.from(new NotANumber());
            case BigDecimalValue(BigDecimal dividendValue) -> switch (divisor.toBigDecimal()) {
                case NotANumber() -> divisor;
                case Infinity(boolean _) -> dividend;
                case BigDecimalValue(BigDecimal divisorValue) -> {
                    try {
                        yield TrinoNumber.from(dividendValue.remainder(divisorValue));
                    }
                    catch (ArithmeticException e) {
                        yield TrinoNumber.from(new NotANumber());
                    }
                }
            };
        };
    }

    @ScalarOperator(NEGATION)
    @SqlType(StandardTypes.NUMBER)
    public static TrinoNumber negate(@SqlType(StandardTypes.NUMBER) TrinoNumber value)
    {
        return switch (value.toBigDecimal()) {
            case NotANumber _ -> value;
            case Infinity(boolean negative) -> TrinoNumber.from(new Infinity(!negative));
            case BigDecimalValue(BigDecimal bigDecimal) -> TrinoNumber.from(bigDecimal.negate());
        };
    }

    @ScalarOperator(CAST)
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice castToVarchar(@LiteralParameter("x") long x, @SqlType(StandardTypes.NUMBER) TrinoNumber value)
    {
        String stringValue = switch (value.toBigDecimal()) {
            case NotANumber _ -> "NaN";
            case Infinity(boolean negative) -> negative ? "-Infinity" : "+Infinity";
            case BigDecimalValue(BigDecimal bigDecimal) -> bigDecimal.toString();
        };
        // String is all-ASCII, so String.length() here returns actual code points count
        if (stringValue.length() <= x) {
            return utf8Slice(stringValue);
        }
        throw new TrinoException(INVALID_CAST_ARGUMENT, format("Value %s cannot be represented as varchar(%s)", stringValue, x));
    }
}
