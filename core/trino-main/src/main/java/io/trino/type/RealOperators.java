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
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.math.RoundingMode.FLOOR;

public final class RealOperators
{
    private static final float MIN_SHORT_AS_FLOAT = -0x1p15f;
    private static final float MAX_SHORT_PLUS_ONE_AS_FLOAT = 0x1p15f;
    private static final float MIN_BYTE_AS_FLOAT = -0x1p7f;
    private static final float MAX_BYTE_PLUS_ONE_AS_FLOAT = 0x1p7f;

    private static final ThreadLocal<DecimalFormat> FORMAT = ThreadLocal.withInitial(() -> new DecimalFormat("0.0#####E0"));

    private RealOperators()
    {
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.REAL)
    public static long add(@SqlType(StandardTypes.REAL) long left, @SqlType(StandardTypes.REAL) long right)
    {
        return floatToRawIntBits(intBitsToFloat((int) left) + intBitsToFloat((int) right));
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.REAL)
    public static long subtract(@SqlType(StandardTypes.REAL) long left, @SqlType(StandardTypes.REAL) long right)
    {
        return floatToRawIntBits(intBitsToFloat((int) left) - intBitsToFloat((int) right));
    }

    @ScalarOperator(MULTIPLY)
    @SqlType(StandardTypes.REAL)
    public static long multiply(@SqlType(StandardTypes.REAL) long left, @SqlType(StandardTypes.REAL) long right)
    {
        return floatToRawIntBits(intBitsToFloat((int) left) * intBitsToFloat((int) right));
    }

    @ScalarOperator(DIVIDE)
    @SqlType(StandardTypes.REAL)
    public static long divide(@SqlType(StandardTypes.REAL) long left, @SqlType(StandardTypes.REAL) long right)
    {
        return floatToRawIntBits(intBitsToFloat((int) left) / intBitsToFloat((int) right));
    }

    @ScalarOperator(MODULUS)
    @SqlType(StandardTypes.REAL)
    public static long modulus(@SqlType(StandardTypes.REAL) long left, @SqlType(StandardTypes.REAL) long right)
    {
        return floatToRawIntBits(intBitsToFloat((int) left) % intBitsToFloat((int) right));
    }

    @ScalarOperator(NEGATION)
    @SqlType(StandardTypes.REAL)
    public static long negate(@SqlType(StandardTypes.REAL) long value)
    {
        return floatToRawIntBits(-intBitsToFloat((int) value));
    }

    @ScalarOperator(CAST)
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice castToVarchar(@LiteralParameter("x") long x, @SqlType(StandardTypes.REAL) long value)
    {
        float floatValue = intBitsToFloat((int) value);
        String stringValue;

        // handle positive and negative 0
        if (floatValue == 0.0f) {
            if (1.0f / floatValue > 0) {
                stringValue = "0E0";
            }
            else {
                stringValue = "-0E0";
            }
        }
        else if (Float.isInfinite(floatValue)) {
            if (floatValue > 0) {
                stringValue = "Infinity";
            }
            else {
                stringValue = "-Infinity";
            }
        }
        else {
            stringValue = FORMAT.get().format(Double.parseDouble(Float.toString(floatValue)));
        }

        // String is all-ASCII, so String.length() here returns actual code points count
        if (stringValue.length() <= x) {
            return utf8Slice(stringValue);
        }

        throw new TrinoException(INVALID_CAST_ARGUMENT, format("Value %s (%s) cannot be represented as varchar(%s)", floatValue, stringValue, x));
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.BIGINT)
    public static long castToLong(@SqlType(StandardTypes.REAL) long value)
    {
        float floatValue = intBitsToFloat((int) value);
        if (Float.isNaN(floatValue)) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "Cannot cast real NaN to bigint");
        }
        return (long) MathFunctions.round(floatValue);
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.INTEGER)
    public static long castToInteger(@SqlType(StandardTypes.REAL) long value)
    {
        float floatValue = intBitsToFloat((int) value);
        if (Float.isNaN(floatValue)) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "Cannot cast real NaN to integer");
        }
        try {
            return toIntExact((long) MathFunctions.round(floatValue));
        }
        catch (ArithmeticException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Out of range for integer: " + floatValue, e);
        }
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.SMALLINT)
    public static long castToSmallint(@SqlType(StandardTypes.REAL) long value)
    {
        float floatValue = intBitsToFloat((int) value);
        if (Float.isNaN(floatValue)) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "Cannot cast real NaN to smallint");
        }
        try {
            return Shorts.checkedCast((long) MathFunctions.round(floatValue));
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Out of range for smallint: " + floatValue, e);
        }
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TINYINT)
    public static long castToTinyint(@SqlType(StandardTypes.REAL) long value)
    {
        float floatValue = intBitsToFloat((int) value);
        if (Float.isNaN(floatValue)) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "Cannot cast real NaN to tinyint");
        }
        try {
            return SignedBytes.checkedCast((long) MathFunctions.round(floatValue));
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Out of range for tinyint: " + floatValue, e);
        }
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.DOUBLE)
    public static double castToDouble(@SqlType(StandardTypes.REAL) long value)
    {
        return intBitsToFloat((int) value);
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean castToBoolean(@SqlType(StandardTypes.REAL) long value)
    {
        return intBitsToFloat((int) value) != 0.0f;
    }

    @ScalarOperator(SATURATED_FLOOR_CAST)
    @SqlType(StandardTypes.SMALLINT)
    public static long saturatedFloorCastToSmallint(@SqlType(StandardTypes.REAL) long value)
    {
        return saturatedFloorCastToLong(value, Short.MIN_VALUE, MIN_SHORT_AS_FLOAT, Short.MAX_VALUE, MAX_SHORT_PLUS_ONE_AS_FLOAT, StandardTypes.SMALLINT);
    }

    @ScalarOperator(SATURATED_FLOOR_CAST)
    @SqlType(StandardTypes.TINYINT)
    public static long saturatedFloorCastToTinyint(@SqlType(StandardTypes.REAL) long value)
    {
        return saturatedFloorCastToLong(value, Byte.MIN_VALUE, MIN_BYTE_AS_FLOAT, Byte.MAX_VALUE, MAX_BYTE_PLUS_ONE_AS_FLOAT, StandardTypes.TINYINT);
    }

    private static long saturatedFloorCastToLong(long valueBits, long minValue, float minValueAsDouble, long maxValue, float maxValuePlusOneAsDouble, String targetType)
    {
        float value = intBitsToFloat((int) valueBits);
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
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Unable to cast real %s to %s", value, targetType), e);
        }
    }
}
