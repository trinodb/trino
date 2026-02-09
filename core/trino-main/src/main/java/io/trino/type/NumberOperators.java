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
import io.trino.spi.type.TrinoNumber.AsBigDecimal;
import io.trino.spi.type.TrinoNumber.BigDecimalValue;
import io.trino.spi.type.TrinoNumber.Infinity;
import io.trino.spi.type.TrinoNumber.NotANumber;

import java.math.BigDecimal;
import java.math.RoundingMode;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.function.OperatorType.ADD;
import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.spi.function.OperatorType.DIVIDE;
import static io.trino.spi.function.OperatorType.MODULUS;
import static io.trino.spi.function.OperatorType.MULTIPLY;
import static io.trino.spi.function.OperatorType.NEGATION;
import static io.trino.spi.function.OperatorType.SUBTRACT;
import static java.lang.Float.floatToRawIntBits;
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
                case Infinity(boolean rightNegative) -> TrinoNumber.from(new Infinity(leftNegative != rightNegative));
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
                    // We could return Infinity (or NaN) when dividing by zero, to match double semantics.
                    // However, we decided to match decimal division semantics instead.
                    // Modeled after decimal division, see DecimalOperators.decimalDivideOperator
                    int resultScale = Math.max(dividendValue.scale() + divisorValue.precision() + 1, 6);
                    try {
                        yield TrinoNumber.from(dividendValue.divide(divisorValue, resultScale, RoundingMode.HALF_UP));
                    }
                    catch (ArithmeticException e) {
                        throw new TrinoException(DIVISION_BY_ZERO, "Division by zero", e);
                    }
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
                        throw new TrinoException(DIVISION_BY_ZERO, "Division by zero", e);
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
    @SqlType(StandardTypes.TINYINT)
    public static long castToTinyint(@SqlType(StandardTypes.NUMBER) TrinoNumber value)
    {
        AsBigDecimal asBigDecimal = value.toBigDecimal();
        try {
            if (asBigDecimal instanceof BigDecimalValue(BigDecimal bigDecimal)) {
                long valueAsLong = bigDecimal.setScale(0, RoundingMode.HALF_UP).longValueExact();
                if (valueAsLong >= Byte.MIN_VALUE && valueAsLong <= Byte.MAX_VALUE) {
                    return valueAsLong;
                }
            }
        }
        catch (ArithmeticException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, format("Cannot cast NUMBER '%s' to TINYINT", asBigDecimal), e);
        }
        throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, format("Cannot cast NUMBER '%s' to TINYINT", asBigDecimal));
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.SMALLINT)
    public static long castToSmallint(@SqlType(StandardTypes.NUMBER) TrinoNumber value)
    {
        AsBigDecimal asBigDecimal = value.toBigDecimal();
        try {
            if (asBigDecimal instanceof BigDecimalValue(BigDecimal bigDecimal)) {
                long valueAsLong = bigDecimal.setScale(0, RoundingMode.HALF_UP).longValueExact();
                if (valueAsLong >= Short.MIN_VALUE && valueAsLong <= Short.MAX_VALUE) {
                    return valueAsLong;
                }
            }
        }
        catch (ArithmeticException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, format("Cannot cast NUMBER '%s' to SMALLINT", asBigDecimal), e);
        }
        throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, format("Cannot cast NUMBER '%s' to SMALLINT", asBigDecimal));
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.INTEGER)
    public static long castToInteger(@SqlType(StandardTypes.NUMBER) TrinoNumber value)
    {
        AsBigDecimal asBigDecimal = value.toBigDecimal();
        try {
            if (asBigDecimal instanceof BigDecimalValue(BigDecimal bigDecimal)) {
                long valueAsLong = bigDecimal.setScale(0, RoundingMode.HALF_UP).longValueExact();
                if (valueAsLong >= Integer.MIN_VALUE && valueAsLong <= Integer.MAX_VALUE) {
                    return valueAsLong;
                }
            }
        }
        catch (ArithmeticException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, format("Cannot cast NUMBER '%s' to INTEGER", asBigDecimal), e);
        }
        throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, format("Cannot cast NUMBER '%s' to INTEGER", asBigDecimal));
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.BIGINT)
    public static long castToBigint(@SqlType(StandardTypes.NUMBER) TrinoNumber value)
    {
        AsBigDecimal asBigDecimal = value.toBigDecimal();
        try {
            if (asBigDecimal instanceof BigDecimalValue(BigDecimal bigDecimal)) {
                return bigDecimal.setScale(0, RoundingMode.HALF_UP).longValueExact();
            }
        }
        catch (ArithmeticException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, format("Cannot cast NUMBER '%s' to BIGINT", asBigDecimal), e);
        }
        throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, format("Cannot cast NUMBER '%s' to BIGINT", asBigDecimal));
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.REAL)
    public static long castToReal(@SqlType(StandardTypes.NUMBER) TrinoNumber value)
    {
        return floatToRawIntBits(switch (value.toBigDecimal()) {
            case NotANumber() -> Float.NaN;
            case Infinity(boolean negative) -> negative ? Float.NEGATIVE_INFINITY : Float.POSITIVE_INFINITY;
            case BigDecimalValue(BigDecimal bigDecimal) -> bigDecimal.floatValue();
        });
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.DOUBLE)
    public static double castToDouble(@SqlType(StandardTypes.NUMBER) TrinoNumber value)
    {
        return switch (value.toBigDecimal()) {
            case NotANumber() -> Double.NaN;
            case Infinity(boolean negative) -> negative ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
            case BigDecimalValue(BigDecimal bigDecimal) -> bigDecimal.doubleValue();
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
