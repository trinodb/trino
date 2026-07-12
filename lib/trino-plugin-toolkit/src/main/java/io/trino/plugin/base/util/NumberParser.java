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
package io.trino.plugin.base.util;

import io.airlift.slice.Slice;

/**
 * Parses floating point values directly from their ASCII bytes.
 * <p>
 * {@link Double#parseDouble} and {@link Float#parseFloat} only accept a {@link String}, so calling
 * them requires decoding a {@code String} for every value in a file.
 * <p>
 * Plain decimal values are parsed here, and everything else (exponents, hex, {@code Infinity},
 * {@code NaN}, surrounding whitespace, trailing type suffixes) falls back to the JDK, so the result
 * is always identical to what the JDK would return, including for malformed input.
 */
public final class NumberParser
{
    /**
     * Integers up to 2^53 are exactly representable as a double.
     */
    private static final long MAX_EXACT_DOUBLE_SIGNIFICAND = 1L << 53;

    /**
     * 10^22 is the largest power of ten exactly representable as a double.
     */
    private static final int MAX_EXACT_DOUBLE_POWER_OF_TEN = 22;

    private static final double[] DOUBLE_POWERS_OF_TEN = new double[MAX_EXACT_DOUBLE_POWER_OF_TEN + 1];

    /**
     * Integers up to 2^24 are exactly representable as a float.
     */
    private static final long MAX_EXACT_FLOAT_SIGNIFICAND = 1L << 24;

    /**
     * 10^10 is the largest power of ten exactly representable as a float: 10^10 is 5^10 * 2^10, and
     * 5^10 still fits in the 24 bit significand, while 5^11 does not.
     */
    private static final int MAX_EXACT_FLOAT_POWER_OF_TEN = 10;

    private static final float[] FLOAT_POWERS_OF_TEN = new float[MAX_EXACT_FLOAT_POWER_OF_TEN + 1];

    static {
        double doublePower = 1;
        for (int i = 0; i <= MAX_EXACT_DOUBLE_POWER_OF_TEN; i++) {
            DOUBLE_POWERS_OF_TEN[i] = doublePower;
            doublePower *= 10;
        }
        float floatPower = 1;
        for (int i = 0; i <= MAX_EXACT_FLOAT_POWER_OF_TEN; i++) {
            FLOAT_POWERS_OF_TEN[i] = floatPower;
            floatPower *= 10;
        }
    }

    /**
     * Signals that the value is not a plain decimal, and must be parsed by the JDK.
     */
    private static final long NOT_PLAIN_DECIMAL = -1;

    /**
     * Packs the number of fractional digits into the low bits and the sign into the next bit, so
     * that {@link #parsePlainDecimal} can return both alongside the significand.
     */
    private static final int SCALE_BITS = 6;
    private static final int SCALE_MASK = (1 << SCALE_BITS) - 1;
    private static final int NEGATIVE_BIT = 1 << SCALE_BITS;

    /**
     * Largest accumulator value that can take another digit without overflowing.
     */
    private static final long MAX_UNSCALED_ACCUMULATOR = (Long.MAX_VALUE - 9) / 10;

    private static final long[] LONG_POWERS_OF_TEN = new long[19];

    static {
        long longPower = 1;
        for (int i = 0; i < LONG_POWERS_OF_TEN.length; i++) {
            LONG_POWERS_OF_TEN[i] = longPower;
            if (i + 1 < LONG_POWERS_OF_TEN.length) {
                longPower *= 10;
            }
        }
    }

    /**
     * Returned by {@link #parseShortDecimal} when the value is not a plain decimal, or does not fit
     * in a long. A short decimal has a precision of at most 18, so its unscaled value is bounded by
     * 10^18 and can never be this.
     */
    public static final long NOT_SHORT_DECIMAL = Long.MIN_VALUE;

    private NumberParser() {}

    /**
     * Parses a plain decimal into an unscaled long at {@code scale}, rounding half up, exactly as
     * {@code new BigDecimal(value).setScale(scale, HALF_UP)} would. Returns
     * {@link #NOT_SHORT_DECIMAL} for anything else, including exponents and malformed input.
     * <p>
     * The caller must still reject values that exceed the precision of the target type.
     */
    public static long parseShortDecimal(Slice slice, int offset, int length, int scale)
    {
        int index = offset;
        int end = offset + length;
        if (index == end) {
            return NOT_SHORT_DECIMAL;
        }

        boolean negative = false;
        byte first = slice.getByte(index);
        if (first == '-') {
            negative = true;
            index++;
        }
        else if (first == '+') {
            index++;
        }

        long unscaled = 0;
        int digits = 0;
        int inputScale = 0;
        boolean decimalPoint = false;
        while (index < end) {
            byte current = slice.getByte(index);
            if (current >= '0' && current <= '9') {
                if (unscaled > MAX_UNSCALED_ACCUMULATOR) {
                    return NOT_SHORT_DECIMAL;
                }
                unscaled = unscaled * 10 + (current - '0');
                digits++;
                if (decimalPoint) {
                    inputScale++;
                }
            }
            else if (current == '.' && !decimalPoint) {
                decimalPoint = true;
            }
            else {
                return NOT_SHORT_DECIMAL;
            }
            index++;
        }

        if (digits == 0) {
            return NOT_SHORT_DECIMAL;
        }

        if (inputScale > scale) {
            int dropped = inputScale - scale;
            if (dropped >= LONG_POWERS_OF_TEN.length) {
                return NOT_SHORT_DECIMAL;
            }
            long divisor = LONG_POWERS_OF_TEN[dropped];
            long quotient = unscaled / divisor;
            long remainder = unscaled - (quotient * divisor);
            // half up rounds away from zero when the discarded fraction is at least one half, and
            // the value is still unsigned here, so away from zero is up
            if (remainder * 2 >= divisor) {
                quotient++;
            }
            unscaled = quotient;
        }
        else if (inputScale < scale) {
            int added = scale - inputScale;
            if (added >= LONG_POWERS_OF_TEN.length) {
                return NOT_SHORT_DECIMAL;
            }
            long multiplier = LONG_POWERS_OF_TEN[added];
            if (unscaled > Long.MAX_VALUE / multiplier) {
                return NOT_SHORT_DECIMAL;
            }
            unscaled *= multiplier;
        }

        return negative ? -unscaled : unscaled;
    }

    /**
     * Returns 10^exponent, for an exponent of at most 18.
     */
    public static long longPowerOfTen(int exponent)
    {
        return LONG_POWERS_OF_TEN[exponent];
    }

    public static double parseDouble(Slice slice, int offset, int length)
    {
        long packed = parsePlainDecimal(slice, offset, length, MAX_EXACT_DOUBLE_SIGNIFICAND);
        if (packed == NOT_PLAIN_DECIMAL) {
            return Double.parseDouble(slice.toStringAscii(offset, length));
        }

        int scale = scale(packed);
        if (scale > MAX_EXACT_DOUBLE_POWER_OF_TEN) {
            return Double.parseDouble(slice.toStringAscii(offset, length));
        }

        // The significand and the power of ten are both exact, so this single division is correctly
        // rounded, and therefore identical to parsing the decimal value directly.
        double value = significand(packed);
        if (scale > 0) {
            value /= DOUBLE_POWERS_OF_TEN[scale];
        }
        return isNegative(packed) ? -value : value;
    }

    public static float parseFloat(Slice slice, int offset, int length)
    {
        long packed = parsePlainDecimal(slice, offset, length, MAX_EXACT_FLOAT_SIGNIFICAND);
        if (packed == NOT_PLAIN_DECIMAL) {
            return Float.parseFloat(slice.toStringAscii(offset, length));
        }

        int scale = scale(packed);
        if (scale > MAX_EXACT_FLOAT_POWER_OF_TEN) {
            return Float.parseFloat(slice.toStringAscii(offset, length));
        }

        // Computed in float rather than double, because rounding to double and then to float could
        // round twice and produce a different value than parsing the decimal directly.
        float value = significand(packed);
        if (scale > 0) {
            value /= FLOAT_POWERS_OF_TEN[scale];
        }
        return isNegative(packed) ? -value : value;
    }

    /**
     * Parses an optionally signed decimal with no exponent, whose significand does not exceed
     * {@code maxSignificand}, and returns the significand, the number of fractional digits, and the
     * sign packed into a long. Returns {@link #NOT_PLAIN_DECIMAL} for anything else.
     */
    private static long parsePlainDecimal(Slice slice, int offset, int length, long maxSignificand)
    {
        int index = offset;
        int end = offset + length;
        if (index == end) {
            return NOT_PLAIN_DECIMAL;
        }

        boolean negative = false;
        byte first = slice.getByte(index);
        if (first == '-') {
            negative = true;
            index++;
        }
        else if (first == '+') {
            index++;
        }

        long significand = 0;
        int digits = 0;
        int scale = 0;
        boolean decimalPoint = false;
        while (index < end) {
            byte current = slice.getByte(index);
            if (current >= '0' && current <= '9') {
                significand = significand * 10 + (current - '0');
                if (significand > maxSignificand) {
                    // too large to be exactly representable, and the check also keeps the
                    // accumulator from overflowing
                    return NOT_PLAIN_DECIMAL;
                }
                digits++;
                if (decimalPoint) {
                    scale++;
                    if (scale > SCALE_MASK) {
                        return NOT_PLAIN_DECIMAL;
                    }
                }
            }
            else if (current == '.' && !decimalPoint) {
                decimalPoint = true;
            }
            else {
                return NOT_PLAIN_DECIMAL;
            }
            index++;
        }

        if (digits == 0) {
            return NOT_PLAIN_DECIMAL;
        }
        return (significand << (SCALE_BITS + 1)) | (negative ? NEGATIVE_BIT : 0) | scale;
    }

    private static long significand(long packed)
    {
        return packed >>> (SCALE_BITS + 1);
    }

    private static int scale(long packed)
    {
        return (int) (packed & SCALE_MASK);
    }

    private static boolean isNegative(long packed)
    {
        return (packed & NEGATIVE_BIT) != 0;
    }
}
