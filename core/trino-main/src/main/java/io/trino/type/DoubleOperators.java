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

import com.google.common.math.DoubleMath;
import io.airlift.slice.Slice;
import io.trino.operator.scalar.MathFunctions;
import io.trino.operator.scalar.preimage.OrderPreservingCastPreimage;
import io.trino.spi.TrinoException;
import io.trino.spi.function.FunctionPreimage;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TrinoNumber;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.function.OperatorType.ADD;
import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.spi.function.OperatorType.DIVIDE;
import static io.trino.spi.function.OperatorType.MODULO;
import static io.trino.spi.function.OperatorType.MULTIPLY;
import static io.trino.spi.function.OperatorType.NEGATION;
import static io.trino.spi.function.OperatorType.SUBTRACT;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.String.format;
import static java.lang.runtime.ExactConversionsSupport.isLongToByteExact;
import static java.lang.runtime.ExactConversionsSupport.isLongToIntExact;
import static java.lang.runtime.ExactConversionsSupport.isLongToShortExact;
import static java.math.RoundingMode.HALF_UP;
import static java.util.Locale.ENGLISH;

public final class DoubleOperators
{
    private static final ThreadLocal<DecimalFormat> FORMAT = ThreadLocal.withInitial(() -> new DecimalFormat("0.0###################E0", new DecimalFormatSymbols(ENGLISH)));

    private DoubleOperators() {}

    @ScalarOperator(value = ADD, neverFails = true)
    @SqlType(StandardTypes.DOUBLE)
    public static double add(@SqlType(StandardTypes.DOUBLE) double left, @SqlType(StandardTypes.DOUBLE) double right)
    {
        return left + right;
    }

    @ScalarOperator(value = SUBTRACT, neverFails = true)
    @SqlType(StandardTypes.DOUBLE)
    public static double subtract(@SqlType(StandardTypes.DOUBLE) double left, @SqlType(StandardTypes.DOUBLE) double right)
    {
        return left - right;
    }

    @ScalarOperator(value = MULTIPLY, neverFails = true)
    @SqlType(StandardTypes.DOUBLE)
    public static double multiply(@SqlType(StandardTypes.DOUBLE) double left, @SqlType(StandardTypes.DOUBLE) double right)
    {
        return left * right;
    }

    @ScalarOperator(value = DIVIDE, neverFails = true)
    @SqlType(StandardTypes.DOUBLE)
    public static double divide(@SqlType(StandardTypes.DOUBLE) double left, @SqlType(StandardTypes.DOUBLE) double right)
    {
        return left / right;
    }

    @ScalarOperator(value = MODULO, neverFails = true)
    @SqlType(StandardTypes.DOUBLE)
    public static double modulo(@SqlType(StandardTypes.DOUBLE) double left, @SqlType(StandardTypes.DOUBLE) double right)
    {
        return left % right;
    }

    @ScalarOperator(value = NEGATION, neverFails = true)
    @SqlType(StandardTypes.DOUBLE)
    public static double negate(@SqlType(StandardTypes.DOUBLE) double value)
    {
        return -value;
    }

    @FunctionPreimage(OrderPreservingCastPreimage.class)
    @ScalarOperator(value = CAST, neverFails = true)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean castToBoolean(@SqlType(StandardTypes.DOUBLE) double value)
    {
        return value != 0;
    }

    // fallible
    @FunctionPreimage(OrderPreservingCastPreimage.class)
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.INTEGER)
    public static long castToInteger(@SqlType(StandardTypes.DOUBLE) double value)
    {
        if (Double.isNaN(value)) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "Cannot cast double NaN to integer");
        }
        long rounded = (long) MathFunctions.round(value);
        if (!isLongToIntExact(rounded)) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Out of range for integer: " + value);
        }
        return (int) rounded;
    }

    // fallible
    @FunctionPreimage(OrderPreservingCastPreimage.class)
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.SMALLINT)
    public static long castToSmallint(@SqlType(StandardTypes.DOUBLE) double value)
    {
        if (Double.isNaN(value)) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "Cannot cast double NaN to smallint");
        }
        long rounded = (long) MathFunctions.round(value);
        if (!isLongToShortExact(rounded)) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Out of range for smallint: " + value);
        }
        return (short) rounded;
    }

    // fallible
    @FunctionPreimage(OrderPreservingCastPreimage.class)
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TINYINT)
    public static long castToTinyint(@SqlType(StandardTypes.DOUBLE) double value)
    {
        if (Double.isNaN(value)) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "Cannot cast double NaN to tinyint");
        }
        long rounded = (long) MathFunctions.round(value);
        if (!isLongToByteExact(rounded)) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Out of range for tinyint: " + value);
        }
        return (byte) rounded;
    }

    // fallible
    @FunctionPreimage(OrderPreservingCastPreimage.class)
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.BIGINT)
    public static long castToBigint(@SqlType(StandardTypes.DOUBLE) double value)
    {
        if (Double.isNaN(value)) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "Cannot cast double NaN to bigint");
        }
        try {
            return DoubleMath.roundToLong(value, HALF_UP);
        }
        catch (ArithmeticException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Out of range for bigint: " + value, e);
        }
    }

    @FunctionPreimage(OrderPreservingCastPreimage.class)
    @ScalarOperator(value = CAST, neverFails = true)
    @SqlType(StandardTypes.REAL)
    public static long castToReal(@SqlType(StandardTypes.DOUBLE) double value)
    {
        return floatToRawIntBits((float) value);
    }

    @FunctionPreimage(OrderPreservingCastPreimage.class)
    @ScalarOperator(value = CAST, neverFails = true)
    @SqlType(StandardTypes.NUMBER)
    public static TrinoNumber castToNumber(@SqlType(StandardTypes.DOUBLE) double value)
    {
        if (Double.isNaN(value)) {
            return TrinoNumber.from(new TrinoNumber.NotANumber());
        }
        if (Double.isInfinite(value)) {
            return TrinoNumber.from(new TrinoNumber.Infinity(value < 0.0));
        }
        return TrinoNumber.from(BigDecimal.valueOf(value));
    }

    // fallible
    @FunctionPreimage(OrderPreservingCastPreimage.class)
    @ScalarOperator(CAST)
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice castToVarchar(@LiteralParameter("x") long x, @SqlType(StandardTypes.DOUBLE) double value)
    {
        String stringValue;

        // handle positive and negative 0
        if (value == 0e0) {
            if (1e0 / value > 0) {
                stringValue = "0E0";
            }
            else {
                stringValue = "-0E0";
            }
        }
        else if (Double.isInfinite(value)) {
            if (value > 0) {
                stringValue = "Infinity";
            }
            else {
                stringValue = "-Infinity";
            }
        }
        else {
            stringValue = FORMAT.get().format(value);
        }

        // String is all-ASCII, so String.length() here returns actual code points count
        if (stringValue.length() <= x) {
            return utf8Slice(stringValue);
        }

        throw new TrinoException(INVALID_CAST_ARGUMENT, format("Value %s (%s) cannot be represented as varchar(%s)", value, stringValue, x));
    }
}
