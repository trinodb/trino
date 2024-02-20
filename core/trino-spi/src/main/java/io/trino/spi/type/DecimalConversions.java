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
package io.trino.spi.type;

import io.trino.spi.TrinoException;

import java.math.BigDecimal;
import java.math.BigInteger;

import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.type.Decimals.overflows;
import static io.trino.spi.type.Int128Math.compareAbsolute;
import static io.trino.spi.type.Int128Math.rescale;
import static java.lang.Double.parseDouble;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Float.parseFloat;
import static java.lang.String.format;
import static java.math.RoundingMode.HALF_UP;

public final class DecimalConversions
{
    /**
     * Powers of 10 which can be represented exactly in double.
     */
    private static final double[] DOUBLE_10_POW = {
            1.0e0, 1.0e1, 1.0e2, 1.0e3, 1.0e4, 1.0e5,
            1.0e6, 1.0e7, 1.0e8, 1.0e9, 1.0e10, 1.0e11,
            1.0e12, 1.0e13, 1.0e14, 1.0e15, 1.0e16, 1.0e17,
            1.0e18, 1.0e19, 1.0e20, 1.0e21, 1.0e22
    };
    /**
     * Powers of 10 which can be represented exactly in float.
     */
    private static final float[] FLOAT_10_POW = {
            1.0e0f, 1.0e1f, 1.0e2f, 1.0e3f, 1.0e4f, 1.0e5f,
            1.0e6f, 1.0e7f, 1.0e8f, 1.0e9f, 1.0e10f
    };
    private static final Int128 MAX_EXACT_DOUBLE = Int128.valueOf((1L << 52) - 1);
    private static final Int128 MAX_EXACT_FLOAT = Int128.valueOf((1L << 22) - 1);

    private DecimalConversions() {}

    public static double shortDecimalToDouble(long decimal, long tenToScale)
    {
        return ((double) decimal) / tenToScale;
    }

    public static double longDecimalToDouble(Int128 decimal, long scale)
    {
        // If both decimal and scale can be represented exactly in double then compute rescaled and rounded result directly in double.
        if (scale < DOUBLE_10_POW.length && compareAbsolute(decimal, MAX_EXACT_DOUBLE) <= 0) {
            return (double) decimal.toLong() / DOUBLE_10_POW[intScale(scale)];
        }

        // TODO: optimize and convert directly to double in similar fashion as in double to decimal casts
        return parseDouble(Decimals.toString(decimal, intScale(scale)));
    }

    public static long shortDecimalToReal(long decimal, long tenToScale)
    {
        return floatToRawIntBits(((float) decimal) / tenToScale);
    }

    public static long longDecimalToReal(Int128 decimal, long scale)
    {
        // If both decimal and scale can be represented exactly in float then compute rescaled and rounded result directly in float.
        if (scale < FLOAT_10_POW.length && compareAbsolute(decimal, MAX_EXACT_FLOAT) <= 0) {
            return floatToRawIntBits((float) decimal.toLong() / FLOAT_10_POW[intScale(scale)]);
        }

        // TODO: optimize and convert directly to float in similar fashion as in double to decimal casts
        return floatToRawIntBits(parseFloat(Decimals.toString(decimal, intScale(scale))));
    }

    public static long doubleToShortDecimal(double value, long precision, long scale)
    {
        // TODO: implement specialized version for short decimals
        Int128 decimal = internalDoubleToLongDecimal(value, precision, scale);

        long low = decimal.getLow();
        long high = decimal.getHigh();

        checkState(high == (low >> 63), "Unexpected long decimal");

        return low;
    }

    public static Int128 doubleToLongDecimal(double value, long precision, long scale)
    {
        return internalDoubleToLongDecimal(value, precision, scale);
    }

    private static Int128 internalDoubleToLongDecimal(double value, long precision, long scale)
    {
        if (Double.isInfinite(value) || Double.isNaN(value)) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast DOUBLE '%s' to DECIMAL(%s, %s)", value, precision, scale));
        }

        try {
            // todo consider changing this implementation to more performant one which does not use intermediate String objects
            BigDecimal bigDecimal = BigDecimal.valueOf(value).setScale(intScale(scale), HALF_UP);
            Int128 decimal = Decimals.valueOf(bigDecimal);
            if (Decimals.overflows(decimal, intScale(precision))) {
                throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast DOUBLE '%s' to DECIMAL(%s, %s)", value, precision, scale));
            }
            return decimal;
        }
        catch (ArithmeticException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast DOUBLE '%s' to DECIMAL(%s, %s)", value, precision, scale));
        }
    }

    /**
     * @deprecated Use {@link #realToShortDecimal(float, long, long)} instead
     */
    @Deprecated(forRemoval = true)
    public static long realToShortDecimal(long value, long precision, long scale)
    {
        float floatValue = intBitsToFloat(intScale(value));
        return realToShortDecimal(floatValue, precision, scale);
    }

    public static long realToShortDecimal(float value, long precision, long scale)
    {
        // TODO: implement specialized version for short decimals
        Int128 decimal = realToLongDecimal(value, precision, scale);

        long low = decimal.getLow();
        long high = decimal.getHigh();

        checkState(high == (low >> 63), "Unexpected long decimal");

        return low;
    }

    /**
     * @deprecated Use {@link #realToLongDecimal(float, long, long)} instead
     */
    @Deprecated(forRemoval = true)
    public static Int128 realToLongDecimal(long value, long precision, long scale)
    {
        float floatValue = intBitsToFloat(intScale(value));
        return realToLongDecimal(floatValue, precision, scale);
    }

    public static Int128 realToLongDecimal(float floatValue, long precision, long scale)
    {
        if (Float.isInfinite(floatValue) || Float.isNaN(floatValue)) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast REAL '%s' to DECIMAL(%s, %s)", floatValue, precision, scale));
        }

        try {
            // todo consider changing this implementation to more performant one which does not use intermediate String objects
            BigDecimal bigDecimal = new BigDecimal(String.valueOf(floatValue)).setScale(intScale(scale), HALF_UP);
            Int128 decimal = Decimals.valueOf(bigDecimal);
            if (Decimals.overflows(decimal, intScale(precision))) {
                throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast REAL '%s' to DECIMAL(%s, %s)", floatValue, precision, scale));
            }
            return decimal;
        }
        catch (ArithmeticException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast REAL '%s' to DECIMAL(%s, %s)", floatValue, precision, scale));
        }
    }

    public static long shortToShortCast(
            long value,
            long sourcePrecision,
            long sourceScale,
            long resultPrecision,
            long resultScale,
            long scalingFactor,
            long halfOfScalingFactor)
    {
        long returnValue;
        if (resultScale >= sourceScale) {
            returnValue = value * scalingFactor;
        }
        else {
            returnValue = value / scalingFactor;
            if (value >= 0) {
                if (value % scalingFactor >= halfOfScalingFactor) {
                    returnValue++;
                }
            }
            else {
                if (value % scalingFactor <= -halfOfScalingFactor) {
                    returnValue--;
                }
            }
        }
        if (overflows(returnValue, (int) resultPrecision)) {
            throw throwCastException(value, sourcePrecision, sourceScale, resultPrecision, resultScale);
        }
        return returnValue;
    }

    public static Int128 shortToLongCast(
            long value,
            long sourcePrecision,
            long sourceScale,
            long resultPrecision,
            long resultScale)
    {
        return longToLongCast(Int128.valueOf(value), sourcePrecision, sourceScale, resultPrecision, resultScale);
    }

    public static long longToShortCast(
            Int128 value,
            long sourcePrecision,
            long sourceScale,
            long resultPrecision,
            long resultScale)
    {
        return longToLongCast(value, sourcePrecision, sourceScale, resultPrecision, resultScale).toLong();
    }

    public static Int128 longToLongCast(
            Int128 value,
            long sourcePrecision,
            long sourceScale,
            long resultPrecision,
            long resultScale)
    {
        if (sourcePrecision == resultPrecision && sourceScale == resultScale) {
            return value;
        }

        try {
            Int128 result = rescale(value, (int) (resultScale - sourceScale));
            if (Decimals.overflows(result, (int) resultPrecision)) {
                throw throwCastException(value.toBigInteger(), sourcePrecision, sourceScale, resultPrecision, resultScale);
            }
            return result;
        }
        catch (ArithmeticException e) {
            throw throwCastException(value.toBigInteger(), sourcePrecision, sourceScale, resultPrecision, resultScale);
        }
    }

    private static TrinoException throwCastException(long value, long sourcePrecision, long sourceScale, long resultPrecision, long resultScale)
    {
        return new TrinoException(INVALID_CAST_ARGUMENT,
                format("Cannot cast DECIMAL(%d, %d) '%s' to DECIMAL(%d, %d)",
                        sourcePrecision,
                        sourceScale,
                        Decimals.toString(value, (int) sourceScale),
                        resultPrecision,
                        resultScale));
    }

    private static TrinoException throwCastException(BigInteger value, long sourcePrecision, long sourceScale, long resultPrecision, long resultScale)
    {
        return new TrinoException(INVALID_CAST_ARGUMENT,
                format("Cannot cast DECIMAL(%d, %d) '%s' to DECIMAL(%d, %d)",
                        sourcePrecision,
                        sourceScale,
                        Decimals.toString(value, (int) sourceScale),
                        resultPrecision,
                        resultScale));
    }

    @SuppressWarnings("NumericCastThatLosesPrecision")
    public static int intScale(long scale)
    {
        return (int) scale;
    }

    private static void checkState(boolean condition, String message)
    {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }
}
