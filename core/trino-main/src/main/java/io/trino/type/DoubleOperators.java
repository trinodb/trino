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
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.airlift.slice.Slice;
import io.trino.operator.scalar.MathFunctions;
import io.trino.spi.TrinoException;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import java.text.DecimalFormat;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.utf8Slice;
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
import static java.lang.Float.floatToIntBits;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.math.RoundingMode.FLOOR;
import static java.math.RoundingMode.HALF_UP;

public final class DoubleOperators
{
    private static final double MIN_INTEGER_AS_DOUBLE = -0x1p31;
    private static final double MAX_INTEGER_PLUS_ONE_AS_DOUBLE = 0x1p31;
    private static final double MIN_SHORT_AS_DOUBLE = -0x1p15;
    private static final double MAX_SHORT_PLUS_ONE_AS_DOUBLE = 0x1p15;
    private static final double MIN_BYTE_AS_DOUBLE = -0x1p7;
    private static final double MAX_BYTE_PLUS_ONE_AS_DOUBLE = 0x1p7;

    private static final ThreadLocal<DecimalFormat> FORMAT = ThreadLocal.withInitial(() -> new DecimalFormat("0.0###################E0"));

    private DoubleOperators()
    {
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.DOUBLE)
    public static double add(@SqlType(StandardTypes.DOUBLE) double left, @SqlType(StandardTypes.DOUBLE) double right)
    {
        return left + right;
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.DOUBLE)
    public static double subtract(@SqlType(StandardTypes.DOUBLE) double left, @SqlType(StandardTypes.DOUBLE) double right)
    {
        return left - right;
    }

    @ScalarOperator(MULTIPLY)
    @SqlType(StandardTypes.DOUBLE)
    public static double multiply(@SqlType(StandardTypes.DOUBLE) double left, @SqlType(StandardTypes.DOUBLE) double right)
    {
        return left * right;
    }

    @ScalarOperator(DIVIDE)
    @SqlType(StandardTypes.DOUBLE)
    public static double divide(@SqlType(StandardTypes.DOUBLE) double left, @SqlType(StandardTypes.DOUBLE) double right)
    {
        return left / right;
    }

    @ScalarOperator(MODULUS)
    @SqlType(StandardTypes.DOUBLE)
    public static double modulus(@SqlType(StandardTypes.DOUBLE) double left, @SqlType(StandardTypes.DOUBLE) double right)
    {
        return left % right;
    }

    @ScalarOperator(NEGATION)
    @SqlType(StandardTypes.DOUBLE)
    public static double negate(@SqlType(StandardTypes.DOUBLE) double value)
    {
        return -value;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean castToBoolean(@SqlType(StandardTypes.DOUBLE) double value)
    {
        return value != 0;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.INTEGER)
    public static long castToInteger(@SqlType(StandardTypes.DOUBLE) double value)
    {
        if (Double.isNaN(value)) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "Cannot cast double NaN to integer");
        }
        try {
            return toIntExact((long) MathFunctions.round(value));
        }
        catch (ArithmeticException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Out of range for integer: " + value, e);
        }
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.SMALLINT)
    public static long castToSmallint(@SqlType(StandardTypes.DOUBLE) double value)
    {
        if (Double.isNaN(value)) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "Cannot cast double NaN to smallint");
        }
        try {
            return Shorts.checkedCast((long) MathFunctions.round(value));
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Out of range for smallint: " + value, e);
        }
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TINYINT)
    public static long castToTinyint(@SqlType(StandardTypes.DOUBLE) double value)
    {
        if (Double.isNaN(value)) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "Cannot cast double NaN to tinyint");
        }
        try {
            return SignedBytes.checkedCast((long) MathFunctions.round(value));
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Out of range for tinyint: " + value, e);
        }
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.BIGINT)
    public static long castToLong(@SqlType(StandardTypes.DOUBLE) double value)
    {
        try {
            return DoubleMath.roundToLong(value, HALF_UP);
        }
        catch (ArithmeticException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Unable to cast %s to bigint", value), e);
        }
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.REAL)
    public static long castToReal(@SqlType(StandardTypes.DOUBLE) double value)
    {
        return floatToRawIntBits((float) value);
    }

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

    @ScalarOperator(SATURATED_FLOOR_CAST)
    @SqlType(StandardTypes.REAL)
    public static long saturatedFloorCastToFloat(@SqlType(StandardTypes.DOUBLE) double value)
    {
        if (Double.isNaN(value)) {
            return floatToIntBits(Float.NaN);
        }
        float result;
        float minFloat = -1.0f * Float.MAX_VALUE;
        if (value <= minFloat) {
            result = minFloat;
        }
        else if (value >= Float.MAX_VALUE) {
            result = Float.MAX_VALUE;
        }
        else {
            result = (float) value;
            if (result > value) {
                result = Math.nextDown(result);
            }
            checkState(result <= value);
        }
        return floatToRawIntBits(result);
    }

    @ScalarOperator(SATURATED_FLOOR_CAST)
    @SqlType(StandardTypes.INTEGER)
    public static long saturatedFloorCastToInteger(@SqlType(StandardTypes.DOUBLE) double value)
    {
        return saturatedFloorCastToLong(value, Integer.MIN_VALUE, MIN_INTEGER_AS_DOUBLE, Integer.MAX_VALUE, MAX_INTEGER_PLUS_ONE_AS_DOUBLE, StandardTypes.INTEGER);
    }

    @ScalarOperator(SATURATED_FLOOR_CAST)
    @SqlType(StandardTypes.SMALLINT)
    public static long saturatedFloorCastToSmallint(@SqlType(StandardTypes.DOUBLE) double value)
    {
        return saturatedFloorCastToLong(value, Short.MIN_VALUE, MIN_SHORT_AS_DOUBLE, Short.MAX_VALUE, MAX_SHORT_PLUS_ONE_AS_DOUBLE, StandardTypes.SMALLINT);
    }

    @ScalarOperator(SATURATED_FLOOR_CAST)
    @SqlType(StandardTypes.TINYINT)
    public static long saturatedFloorCastToTinyint(@SqlType(StandardTypes.DOUBLE) double value)
    {
        return saturatedFloorCastToLong(value, Byte.MIN_VALUE, MIN_BYTE_AS_DOUBLE, Byte.MAX_VALUE, MAX_BYTE_PLUS_ONE_AS_DOUBLE, StandardTypes.TINYINT);
    }

    private static long saturatedFloorCastToLong(double value, long minValue, double minValueAsDouble, long maxValue, double maxValuePlusOneAsDouble, String targetType)
    {
        if (value <= minValueAsDouble) {
            return minValue;
        }
        if (value + 1 >= maxValuePlusOneAsDouble) {
            return maxValue;
        }
        try {
            return DoubleMath.roundToLong(value, FLOOR);
        }
        catch (ArithmeticException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Unable to cast double %s to %s", value, targetType), e);
        }
    }
}
