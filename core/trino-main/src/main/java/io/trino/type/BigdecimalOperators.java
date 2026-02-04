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
import io.trino.spi.type.Bigdecimal;
import io.trino.spi.type.StandardTypes;

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

public final class BigdecimalOperators
{
    private BigdecimalOperators() {}

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.BIGDECIMAL)
    public static Bigdecimal add(@SqlType(StandardTypes.BIGDECIMAL) Bigdecimal left, @SqlType(StandardTypes.BIGDECIMAL) Bigdecimal right)
    {
        return Bigdecimal.from(left.toBigDecimal().add(right.toBigDecimal()));
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.BIGDECIMAL)
    public static Bigdecimal subtract(@SqlType(StandardTypes.BIGDECIMAL) Bigdecimal left, @SqlType(StandardTypes.BIGDECIMAL) Bigdecimal right)
    {
        return Bigdecimal.from(left.toBigDecimal().subtract(right.toBigDecimal()));
    }

    @ScalarOperator(MULTIPLY)
    @SqlType(StandardTypes.BIGDECIMAL)
    public static Bigdecimal multiply(@SqlType(StandardTypes.BIGDECIMAL) Bigdecimal left, @SqlType(StandardTypes.BIGDECIMAL) Bigdecimal right)
    {
        return Bigdecimal.from(left.toBigDecimal().multiply(right.toBigDecimal()));
    }

    @ScalarOperator(DIVIDE)
    @SqlType(StandardTypes.BIGDECIMAL)
    public static Bigdecimal divide(@SqlType(StandardTypes.BIGDECIMAL) Bigdecimal left, @SqlType(StandardTypes.BIGDECIMAL) Bigdecimal right)
    {
        BigDecimal leftBigDecimal = left.toBigDecimal();
        BigDecimal rightBigDecimal = right.toBigDecimal();
        // Modeled after decimal division, see DecimalOperators.decimalDivideOperator
        int resultScale = Math.max(leftBigDecimal.scale() + rightBigDecimal.precision() + 1, 6);
        try {
            return Bigdecimal.from(leftBigDecimal.divide(rightBigDecimal, resultScale, RoundingMode.HALF_UP));
        }
        catch (ArithmeticException e) {
            throw new TrinoException(DIVISION_BY_ZERO, "Division by zero", e);
        }
    }

    @ScalarOperator(MODULUS)
    @SqlType(StandardTypes.BIGDECIMAL)
    public static Bigdecimal modulus(@SqlType(StandardTypes.BIGDECIMAL) Bigdecimal left, @SqlType(StandardTypes.BIGDECIMAL) Bigdecimal right)
    {
        try {
            return Bigdecimal.from(left.toBigDecimal().remainder(right.toBigDecimal()));
        }
        catch (ArithmeticException e) {
            throw new TrinoException(DIVISION_BY_ZERO, "Division by zero", e);
        }
    }

    @ScalarOperator(NEGATION)
    @SqlType(StandardTypes.BIGDECIMAL)
    public static Bigdecimal negate(@SqlType(StandardTypes.BIGDECIMAL) Bigdecimal value)
    {
        return Bigdecimal.from(value.toBigDecimal().negate());
    }

    @ScalarOperator(CAST)
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice castToVarchar(@LiteralParameter("x") long x, @SqlType(StandardTypes.BIGDECIMAL) Bigdecimal value)
    {
        String stringValue = value.toBigDecimal().toPlainString();
        // String is all-ASCII, so String.length() here returns actual code points count
        if (stringValue.length() <= x) {
            return utf8Slice(stringValue);
        }
        throw new TrinoException(INVALID_CAST_ARGUMENT, format("Value %s cannot be represented as varchar(%s)", stringValue, x));
    }

    // TODO all these, or prune
//    @ScalarOperator(CAST)
//    @SqlType(StandardTypes.BOOLEAN)
//    public static boolean castToBoolean(@SqlType(StandardTypes.BIGDECIMAL) long value)
//    {
//        return value != 0;
//    }
//
//    @ScalarOperator(CAST)
//    @SqlType(StandardTypes.INTEGER)
//    public static long castToInteger(@SqlType(StandardTypes.BIGDECIMAL) long value)
//    {
//        try {
//            return toIntExact(value);
//        }
//        catch (ArithmeticException e) {
//            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Out of range for integer: " + value, e);
//        }
//    }
//
//    @ScalarOperator(SATURATED_FLOOR_CAST)
//    @SqlType(StandardTypes.INTEGER)
//    public static long saturatedFloorCastToInteger(@SqlType(StandardTypes.BIGDECIMAL) long value)
//    {
//        return Ints.saturatedCast(value);
//    }
//
//    @ScalarOperator(SATURATED_FLOOR_CAST)
//    @SqlType(StandardTypes.SMALLINT)
//    public static long saturatedFloorCastToSmallint(@SqlType(StandardTypes.BIGDECIMAL) long value)
//    {
//        return Shorts.saturatedCast(value);
//    }
//
//    @ScalarOperator(SATURATED_FLOOR_CAST)
//    @SqlType(StandardTypes.TINYINT)
//    public static long saturatedFloorCastToTinyint(@SqlType(StandardTypes.BIGDECIMAL) long value)
//    {
//        return SignedBytes.saturatedCast(value);
//    }
//
//    @ScalarOperator(CAST)
//    @SqlType(StandardTypes.SMALLINT)
//    public static long castToSmallint(@SqlType(StandardTypes.BIGDECIMAL) long value)
//    {
//        try {
//            return Shorts.checkedCast(value);
//        }
//        catch (IllegalArgumentException e) {
//            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Out of range for smallint: " + value, e);
//        }
//    }
//
//    @ScalarOperator(CAST)
//    @SqlType(StandardTypes.TINYINT)
//    public static long castToTinyint(@SqlType(StandardTypes.BIGDECIMAL) long value)
//    {
//        try {
//            return SignedBytes.checkedCast(value);
//        }
//        catch (IllegalArgumentException e) {
//            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Out of range for tinyint: " + value, e);
//        }
//    }
//
//    @ScalarOperator(CAST)
//    @SqlType(StandardTypes.DOUBLE)
//    public static double castToDouble(@SqlType(StandardTypes.BIGDECIMAL) long value)
//    {
//        return value;
//    }
//
//    @ScalarOperator(CAST)
//    @SqlType(StandardTypes.REAL)
//    public static long castToReal(@SqlType(StandardTypes.BIGDECIMAL) long value)
//    {
//        return floatToRawIntBits((float) value);
//    }
//
//    @ScalarOperator(CAST)
//    @LiteralParameters("x")
//    @SqlType("varchar(x)")
//    public static Slice castToVarchar(@LiteralParameter("x") long x, @SqlType(StandardTypes.BIGDECIMAL) long value)
//    {
//        // todo optimize me
//        String stringValue = String.valueOf(value);
//        // String is all-ASCII, so String.length() here returns actual code points count
//        if (stringValue.length() <= x) {
//            return utf8Slice(stringValue);
//        }
//
//        throw new TrinoException(INVALID_CAST_ARGUMENT, format("Value %s cannot be represented as varchar(%s)", value, x));
//    }
}
