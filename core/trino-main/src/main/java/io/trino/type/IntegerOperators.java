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

import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

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
import static io.trino.spi.function.OperatorType.SATURATED_FLOOR_CAST;
import static io.trino.spi.function.OperatorType.SUBTRACT;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.String.format;

public final class IntegerOperators
{
    private IntegerOperators() {}

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.INTEGER)
    public static long add(@SqlType(StandardTypes.INTEGER) long left, @SqlType(StandardTypes.INTEGER) long right)
    {
        try {
            return Math.addExact((int) left, (int) right);
        }
        catch (ArithmeticException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, format("integer addition overflow: %s + %s", left, right), e);
        }
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.INTEGER)
    public static long subtract(@SqlType(StandardTypes.INTEGER) long left, @SqlType(StandardTypes.INTEGER) long right)
    {
        try {
            return Math.subtractExact((int) left, (int) right);
        }
        catch (ArithmeticException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, format("integer subtraction overflow: %s - %s", left, right), e);
        }
    }

    @ScalarOperator(MULTIPLY)
    @SqlType(StandardTypes.INTEGER)
    public static long multiply(@SqlType(StandardTypes.INTEGER) long left, @SqlType(StandardTypes.INTEGER) long right)
    {
        try {
            return Math.multiplyExact((int) left, (int) right);
        }
        catch (ArithmeticException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, format("integer multiplication overflow: %s * %s", left, right), e);
        }
    }

    @ScalarOperator(DIVIDE)
    @SqlType(StandardTypes.INTEGER)
    public static long divide(@SqlType(StandardTypes.INTEGER) long left, @SqlType(StandardTypes.INTEGER) long right)
    {
        try {
            return left / right;
        }
        catch (ArithmeticException e) {
            throw new TrinoException(DIVISION_BY_ZERO, "Division by zero", e);
        }
    }

    @ScalarOperator(MODULUS)
    @SqlType(StandardTypes.INTEGER)
    public static long modulus(@SqlType(StandardTypes.INTEGER) long left, @SqlType(StandardTypes.INTEGER) long right)
    {
        try {
            return left % right;
        }
        catch (ArithmeticException e) {
            throw new TrinoException(DIVISION_BY_ZERO, "Division by zero", e);
        }
    }

    @ScalarOperator(NEGATION)
    @SqlType(StandardTypes.INTEGER)
    public static long negate(@SqlType(StandardTypes.INTEGER) long value)
    {
        try {
            return Math.negateExact((int) value);
        }
        catch (ArithmeticException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "integer negation overflow: " + value, e);
        }
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.BIGINT)
    public static long castToBigint(@SqlType(StandardTypes.INTEGER) long value)
    {
        return value;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.SMALLINT)
    public static long castToSmallint(@SqlType(StandardTypes.INTEGER) long value)
    {
        try {
            return Shorts.checkedCast(value);
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Out of range for smallint: " + value, e);
        }
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TINYINT)
    public static long castToTinyint(@SqlType(StandardTypes.INTEGER) long value)
    {
        try {
            return SignedBytes.checkedCast(value);
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Out of range for tinyint: " + value, e);
        }
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean castToBoolean(@SqlType(StandardTypes.INTEGER) long value)
    {
        return value != 0;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.DOUBLE)
    public static double castToDouble(@SqlType(StandardTypes.INTEGER) long value)
    {
        return value;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.REAL)
    public static long castToReal(@SqlType(StandardTypes.INTEGER) long value)
    {
        return floatToRawIntBits((float) value);
    }

    @ScalarOperator(CAST)
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice castToVarchar(@LiteralParameter("x") long x, @SqlType(StandardTypes.INTEGER) long value)
    {
        // todo optimize me
        String stringValue = String.valueOf(value);
        // String is all-ASCII, so String.length() here returns actual code points count
        if (stringValue.length() <= x) {
            return utf8Slice(stringValue);
        }

        throw new TrinoException(INVALID_CAST_ARGUMENT, format("Value %s cannot be represented as varchar(%s)", value, x));
    }

    @ScalarOperator(SATURATED_FLOOR_CAST)
    @SqlType(StandardTypes.SMALLINT)
    public static long saturatedFloorCastToSmallint(@SqlType(StandardTypes.INTEGER) long value)
    {
        return Shorts.saturatedCast(value);
    }

    @ScalarOperator(SATURATED_FLOOR_CAST)
    @SqlType(StandardTypes.TINYINT)
    public static long saturatedFloorCastToTinyint(@SqlType(StandardTypes.INTEGER) long value)
    {
        return SignedBytes.saturatedCast(value);
    }
}
