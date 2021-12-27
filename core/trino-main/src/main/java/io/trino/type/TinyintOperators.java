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
import static io.trino.spi.function.OperatorType.SUBTRACT;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.String.format;

public final class TinyintOperators
{
    private TinyintOperators() {}

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.TINYINT)
    public static long add(@SqlType(StandardTypes.TINYINT) long left, @SqlType(StandardTypes.TINYINT) long right)
    {
        try {
            return SignedBytes.checkedCast(left + right);
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, format("tinyint addition overflow: %s + %s", left, right), e);
        }
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.TINYINT)
    public static long subtract(@SqlType(StandardTypes.TINYINT) long left, @SqlType(StandardTypes.TINYINT) long right)
    {
        try {
            return SignedBytes.checkedCast(left - right);
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, format("tinyint subtraction overflow: %s - %s", left, right), e);
        }
    }

    @ScalarOperator(MULTIPLY)
    @SqlType(StandardTypes.TINYINT)
    public static long multiply(@SqlType(StandardTypes.TINYINT) long left, @SqlType(StandardTypes.TINYINT) long right)
    {
        try {
            return SignedBytes.checkedCast(left * right);
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, format("tinyint multiplication overflow: %s * %s", left, right), e);
        }
    }

    @ScalarOperator(DIVIDE)
    @SqlType(StandardTypes.TINYINT)
    public static long divide(@SqlType(StandardTypes.TINYINT) long left, @SqlType(StandardTypes.TINYINT) long right)
    {
        try {
            return left / right;
        }
        catch (ArithmeticException e) {
            throw new TrinoException(DIVISION_BY_ZERO, "Division by zero", e);
        }
    }

    @ScalarOperator(MODULUS)
    @SqlType(StandardTypes.TINYINT)
    public static long modulus(@SqlType(StandardTypes.TINYINT) long left, @SqlType(StandardTypes.TINYINT) long right)
    {
        try {
            return left % right;
        }
        catch (ArithmeticException e) {
            throw new TrinoException(DIVISION_BY_ZERO, "Division by zero", e);
        }
    }

    @ScalarOperator(NEGATION)
    @SqlType(StandardTypes.TINYINT)
    public static long negate(@SqlType(StandardTypes.TINYINT) long value)
    {
        try {
            return SignedBytes.checkedCast(-value);
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "tinyint negation overflow: " + value, e);
        }
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.BIGINT)
    public static long castToBigint(@SqlType(StandardTypes.TINYINT) long value)
    {
        return value;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.INTEGER)
    public static long castToInteger(@SqlType(StandardTypes.TINYINT) long value)
    {
        return value;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.SMALLINT)
    public static long castToSmallint(@SqlType(StandardTypes.TINYINT) long value)
    {
        return value;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean castToBoolean(@SqlType(StandardTypes.TINYINT) long value)
    {
        return value != 0;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.DOUBLE)
    public static double castToDouble(@SqlType(StandardTypes.TINYINT) long value)
    {
        return value;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.REAL)
    public static long castToReal(@SqlType(StandardTypes.TINYINT) long value)
    {
        return floatToRawIntBits((float) value);
    }

    @ScalarOperator(CAST)
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice castToVarchar(@LiteralParameter("x") long x, @SqlType(StandardTypes.TINYINT) long value)
    {
        // todo optimize me
        String stringValue = String.valueOf(value);
        // String is all-ASCII, so String.length() here returns actual code points count
        if (stringValue.length() <= x) {
            return utf8Slice(stringValue);
        }

        throw new TrinoException(INVALID_CAST_ARGUMENT, format("Value %s cannot be represented as varchar(%s)", value, x));
    }
}
