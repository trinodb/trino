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
package io.trino.util;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

import static java.lang.Double.doubleToRawLongBits;
import static java.lang.Float.floatToRawIntBits;

/**
 * Formats {@code double} and {@code real} values the way {@code CAST(x AS varchar)} does:
 * the shortest decimal that round-trips, in scientific notation with at least one fraction
 * digit, for example {@code 1.0E0}, {@code 1.2345E-7} or {@code -3.4028235E38}.
 * Zero, infinities and NaN are formatted as {@code 0E0}, {@code -0E0}, {@code Infinity},
 * {@code -Infinity} and {@code NaN}.
 * <p>
 * A {@code real} value is limited to {@value #MAX_REAL_DIGITS} significant digits, as it was
 * when it was formatted with {@code DecimalFormat("0.0#####E0")}.
 * <p>
 * The digits come from a Schubfach-style algorithm that needs a single multiplication by a power
 * of ten, as in the double to string conversion of yyjson, with an exact fallback.
 * It produces the same digits as {@link Double#toString(double)} and {@link Float#toString(float)}.
 */
public final class ShortestDecimalFormatter
{
    private static final int MAX_REAL_DIGITS = 7;
    // The error of v and delta, in units of 2^-64
    private static final long TOLERANCE = 16;
    // Below this significand the interval is wide enough to hold several two digit decimals
    private static final long SMALL_SIGNIFICAND = 1L << 20;
    private static final long[] POWERS_OF_TEN = new long[19];

    static {
        long power = 1;
        for (int i = 0; i < POWERS_OF_TEN.length; i++) {
            POWERS_OF_TEN[i] = power;
            power *= 10;
        }
    }

    private ShortestDecimalFormatter() {}

    public static Slice formatDouble(double value)
    {
        return formatDouble(value, false);
    }

    // exactOnly skips the fast path, so that it can be tested against exact arithmetic
    static Slice formatDouble(double value, boolean exactOnly)
    {
        if (Double.isNaN(value)) {
            return Slices.utf8Slice("NaN");
        }
        long bits = doubleToRawLongBits(value);
        boolean negative = bits < 0;
        if (Double.isInfinite(value)) {
            return Slices.utf8Slice(negative ? "-Infinity" : "Infinity");
        }
        if (value == 0) {
            return Slices.utf8Slice(negative ? "-0E0" : "0E0");
        }
        int biased = (int) ((bits >>> 52) & 0x7FF);
        long fraction = bits & ((1L << 52) - 1);
        if (biased == 0) {
            return shortest(negative, fraction, -1074, false, Integer.MAX_VALUE, exactOnly);
        }
        return shortest(negative, fraction | (1L << 52), biased - 1075, fraction == 0 && biased > 1, Integer.MAX_VALUE, exactOnly);
    }

    public static Slice formatReal(float value)
    {
        return formatReal(value, MAX_REAL_DIGITS, false);
    }

    // maxDigits is the number of digits to round to, and exactOnly skips the fast path
    static Slice formatReal(float value, int maxDigits, boolean exactOnly)
    {
        if (Float.isNaN(value)) {
            return Slices.utf8Slice("NaN");
        }
        int bits = floatToRawIntBits(value);
        boolean negative = bits < 0;
        if (Float.isInfinite(value)) {
            return Slices.utf8Slice(negative ? "-Infinity" : "Infinity");
        }
        if (value == 0) {
            return Slices.utf8Slice(negative ? "-0E0" : "0E0");
        }
        int biased = (bits >>> 23) & 0xFF;
        int fraction = bits & ((1 << 23) - 1);
        if (biased == 0) {
            return shortest(negative, fraction, -149, false, maxDigits, exactOnly);
        }
        return shortest(negative, fraction | (1 << 23), biased - 150, fraction == 0 && biased > 1, maxDigits, exactOnly);
    }

    /**
     * Finds the shortest decimal that rounds to the value c * 2^e2, using a single multiplication
     * by a cached power of ten in the manner of Yaoyuan's algorithm from yyjson (Schubfach family).
     * <p>
     * With e10 = floor(e2 * log10(2)) the rounding interval of the value is at least 10^e10 wide
     * and less than 10^(e10 + 1) wide. Scaling the value by 10^-e10 gives v, and the half width
     * of the interval becomes delta. The shortest decimal is then one of four candidates:
     * the nearest multiples of 10 below and above v, which are one digit shorter, or the
     * nearest integers below and above v, one of which is always inside the interval.
     * <p>
     * v and delta are computed with 64 fraction bits and an error of less than two units in the
     * last place. A comparison that falls within sixteen units of a boundary is not decided by
     * the fast path, but by {@link #shortestExact}.
     */
    private static Slice shortest(boolean negative, long c, int e2, boolean irregular, int maxDigits, boolean exactOnly)
    {
        if (exactOnly) {
            return shortestExact(negative, c, e2, irregular, maxDigits);
        }

        // integral values below 2^53 print as their digits, there is nothing shorter within half an ulp
        if (e2 <= 0 && e2 > -64 && (c & ((1L << -e2) - 1)) == 0) {
            return emit(negative, c >> -e2, 0, maxDigits);
        }

        int e10 = irregular ? Tables.floorLog10ThreeQuartersPow2(e2) : Tables.floorLog10Pow2(e2);
        long gHigh = Tables.powerHigh(e10);
        long gLow = Tables.powerLow(e10);
        // 10^-e10 = g * 2^-q, and 2^127 <= g < 2^128
        int shift = Tables.powerExponent(e10) - e2 - 64;

        // v = c * g >> shift has 64 fraction bits
        long low0 = c * gLow;
        long high0 = Math.unsignedMultiplyHigh(c, gLow);
        long low1 = c * gHigh;
        long high1 = Math.unsignedMultiplyHigh(c, gHigh);
        long product1 = low1 + high0;
        long product2 = high1 + (Long.compareUnsigned(product1, low1) < 0 ? 1 : 0);
        long integer = (product2 << (64 - shift)) | (product1 >>> shift);
        long fraction = (product1 << (64 - shift)) | (low0 >>> shift);

        // delta = g >> (shift + 1) is half the width of the interval, with 64 fraction bits
        int deltaShift = shift + 1;
        long deltaHigh;
        long deltaLow;
        if (deltaShift == 64) {
            deltaHigh = 0;
            deltaLow = gHigh;
        }
        else {
            deltaHigh = gHigh >>> deltaShift;
            deltaLow = (gLow >>> deltaShift) | (gHigh << (64 - deltaShift));
        }
        // the interval is narrower below a power of two
        long belowHigh = deltaHigh;
        long belowLow = deltaLow;
        if (irregular) {
            belowLow = (deltaLow >>> 1) | (deltaHigh << 63);
            belowHigh = deltaHigh >>> 1;
        }

        long remainder = integer % 10;
        // v might be a bit larger than computed, and cross to the next multiple of 10
        if (remainder == 9 && Long.compareUnsigned(fraction, -TOLERANCE) >= 0) {
            return shortestExact(negative, c, e2, irregular, maxDigits);
        }

        // the multiple of 10 below v is inside the interval when v mod 10 <= delta
        int below = compare(belowHigh - remainder - borrow(belowLow, fraction), belowLow - fraction);
        if (below == 0) {
            return shortestExact(negative, c, e2, irregular, maxDigits);
        }
        long significand;
        if (below > 0) {
            significand = integer - remainder;
        }
        else {
            // the multiple of 10 above v is inside the interval when v mod 10 + delta >= 10
            long sumLow = fraction + deltaLow;
            long sumHigh = remainder + deltaHigh + carry(sumLow, fraction) - 10;
            int above = compare(sumHigh, sumLow);
            if (above == 0) {
                return shortestExact(negative, c, e2, irregular, maxDigits);
            }
            if (above > 0) {
                significand = integer - remainder + 10;
            }
            else {
                // the closest integer, which is inside the interval, unless it is the lower one
                // and the interval is narrower below
                // positive when the fraction of v is below one half
                int half = compare(Long.compareUnsigned(fraction, Long.MIN_VALUE) <= 0 ? 0 : -1, Long.MIN_VALUE - fraction);
                if (half == 0) {
                    return shortestExact(negative, c, e2, irregular, maxDigits);
                }
                if (half > 0) {
                    significand = integer;
                    if (irregular) {
                        int inside = compare(belowHigh - borrow(belowLow, fraction), belowLow - fraction);
                        if (inside == 0) {
                            return shortestExact(negative, c, e2, irregular, maxDigits);
                        }
                        if (inside < 0) {
                            significand = integer + 1;
                        }
                    }
                }
                else {
                    significand = integer + 1;
                }
            }
        }

        // a single digit in a narrow interval is shown as two digits, that is not decided here
        if (c < SMALL_SIGNIFICAND) {
            long trimmed = significand;
            while (trimmed % 10 == 0) {
                trimmed /= 10;
            }
            if (trimmed < 10) {
                return shortestExact(negative, c, e2, irregular, maxDigits);
            }
        }
        return emit(negative, significand, e10, maxDigits);
    }

    private static long borrow(long a, long b)
    {
        return Long.compareUnsigned(a, b) < 0 ? 1 : 0;
    }

    private static long carry(long sum, long addend)
    {
        return Long.compareUnsigned(sum, addend) < 0 ? 1 : 0;
    }

    /**
     * Sign of a 128 bit difference, as 1 for at least TOLERANCE, -1 for at most -TOLERANCE and
     * 0 when the sign is not known because of the error of the operands.
     */
    private static int compare(long high, long low)
    {
        if (high > 0) {
            return 1;
        }
        if (high == 0) {
            return Long.compareUnsigned(low, TOLERANCE) >= 0 ? 1 : 0;
        }
        if (high == -1 && Long.compareUnsigned(low, -TOLERANCE) > 0) {
            return 0;
        }
        return -1;
    }

    /**
     * Same as {@link #shortest}, with exact arithmetic. Used when the fast path cannot tell.
     */
    private static Slice shortestExact(boolean negative, long c, int e2, boolean irregular, int maxDigits)
    {
        BigDecimal ulp = e2 >= 0
                ? new BigDecimal(BigInteger.ONE.shiftLeft(e2))
                : BigDecimal.ONE.divide(new BigDecimal(BigInteger.ONE.shiftLeft(-e2)));
        BigDecimal value = ulp.multiply(BigDecimal.valueOf(c));
        BigDecimal upper = value.add(ulp.divide(BigDecimal.valueOf(2)));
        BigDecimal lower = value.subtract(ulp.divide(BigDecimal.valueOf(irregular ? 4 : 2)));
        boolean inclusive = (c & 1) == 0;

        // the coarsest scale that has a decimal in the interval is the shortest one
        int scale = upper.precision() - upper.scale();
        BigInteger digits = null;
        while (digits == null) {
            digits = closestMultiple(lower, upper, value, inclusive, scale);
            if (digits == null) {
                scale--;
            }
        }
        // a single digit is shown as the closest two digit decimal
        BigInteger trimmed = digits;
        while (trimmed.remainder(BigInteger.TEN).signum() == 0) {
            trimmed = trimmed.divide(BigInteger.TEN);
        }
        if (trimmed.compareTo(BigInteger.TEN) < 0) {
            scale = value.precision() - value.scale() - 2;
            digits = closestMultiple(lower, upper, value, inclusive, scale);
        }
        return emit(negative, digits.longValueExact(), scale, maxDigits);
    }

    // The multiple of 10^scale in the interval that is closest to the value, with ties to even
    private static BigInteger closestMultiple(BigDecimal lower, BigDecimal upper, BigDecimal value, boolean inclusive, int scale)
    {
        BigDecimal unit = BigDecimal.ONE.scaleByPowerOfTen(scale);
        BigInteger first = lower.divide(unit, 0, RoundingMode.CEILING).toBigIntegerExact();
        if (!inclusive && unit.multiply(new BigDecimal(first)).compareTo(lower) == 0) {
            first = first.add(BigInteger.ONE);
        }
        BigInteger last = upper.divide(unit, 0, RoundingMode.FLOOR).toBigIntegerExact();
        if (!inclusive && unit.multiply(new BigDecimal(last)).compareTo(upper) == 0) {
            last = last.subtract(BigInteger.ONE);
        }
        if (first.compareTo(last) > 0) {
            return null;
        }
        BigInteger closest = value.divide(unit, 0, RoundingMode.HALF_EVEN).toBigIntegerExact();
        return closest.max(first).min(last);
    }

    private static Slice emit(boolean negative, long significand, int exponent, int maxDigits)
    {
        if (maxDigits == Integer.MAX_VALUE) {
            return format(negative, significand, exponent);
        }
        return formatRounded(negative, significand, exponent, maxDigits);
    }

    /**
     * Powers of ten as 128 bit significands, 10^-e10 = g * 2^-q, 2^127 <= g < 2^128,
     * and the binary logarithms needed to pick e10.
     */
    static final class Tables
    {
        private static final int MIN_E10 = -330;
        private static final int MAX_E10 = 300;
        private static final int MIN_E2 = -1100;
        private static final int MAX_E2 = 1000;
        private static final double LOG10_2 = Math.log10(2);
        private static final double LOG10_THREE_QUARTERS = Math.log10(0.75);

        private static final long[] HIGH = new long[MAX_E10 - MIN_E10 + 1];
        private static final long[] LOW = new long[HIGH.length];
        private static final int[] EXPONENT = new int[HIGH.length];
        private static final short[] FLOOR_LOG10_POW2 = new short[MAX_E2 - MIN_E2 + 1];
        private static final short[] FLOOR_LOG10_THREE_QUARTERS_POW2 = new short[FLOOR_LOG10_POW2.length];

        static {
            for (int e10 = MIN_E10; e10 <= MAX_E10; e10++) {
                BigInteger power = BigInteger.TEN.pow(Math.abs(e10));
                int length = power.bitLength();
                BigInteger significand;
                int exponent;
                if (e10 <= 0) {
                    exponent = 128 - length;
                    significand = exponent >= 0 ? power.shiftLeft(exponent) : power.shiftRight(-exponent);
                }
                else {
                    exponent = 127 + length;
                    significand = BigInteger.ONE.shiftLeft(exponent).divide(power);
                }
                int index = e10 - MIN_E10;
                HIGH[index] = significand.shiftRight(64).longValue();
                LOW[index] = significand.longValue();
                EXPONENT[index] = exponent;
            }
            for (int e2 = MIN_E2; e2 <= MAX_E2; e2++) {
                FLOOR_LOG10_POW2[e2 - MIN_E2] = (short) Math.floor(e2 * LOG10_2);
                FLOOR_LOG10_THREE_QUARTERS_POW2[e2 - MIN_E2] = (short) Math.floor(LOG10_THREE_QUARTERS + e2 * LOG10_2);
            }
        }

        static int floorLog10Pow2(int e2)
        {
            return FLOOR_LOG10_POW2[e2 - MIN_E2];
        }

        static int floorLog10ThreeQuartersPow2(int e2)
        {
            return FLOOR_LOG10_THREE_QUARTERS_POW2[e2 - MIN_E2];
        }

        static long powerHigh(int e10)
        {
            return HIGH[e10 - MIN_E10];
        }

        static long powerLow(int e10)
        {
            return LOW[e10 - MIN_E10];
        }

        static int powerExponent(int e10)
        {
            return EXPONENT[e10 - MIN_E10];
        }
    }

    // Rounds to maxDigits like DecimalFormat did, when the value was parsed back to a double first
    private static Slice formatRounded(boolean negative, long significand, int exponent, int maxDigits)
    {
        while (significand % 10 == 0) {
            significand /= 10;
            exponent++;
        }
        int digits = digitCount(significand);
        if (digits <= maxDigits) {
            return format(negative, significand, exponent);
        }
        int dropped = digits - maxDigits;
        long divisor = POWERS_OF_TEN[dropped];
        long half = divisor / 2;
        long quotient = significand / divisor;
        long remainder = significand % divisor;
        boolean roundUp;
        if (remainder == half) {
            roundUp = roundsTieUp(significand, exponent, quotient);
        }
        else {
            roundUp = remainder > half;
        }
        if (roundUp) {
            quotient++;
        }
        return format(negative, quotient, exponent + dropped);
    }

    /**
     * Resolves a tie the way DecimalFormat did for the double parsed from the float's string:
     * an integral value below 2^63 is treated as if it had been rounded down, so it rounds up.
     * Otherwise the exact value of the double decides, and an exact tie rounds to even.
     */
    private static boolean roundsTieUp(long significand, int exponent, long quotient)
    {
        BigDecimal decimal = BigDecimal.valueOf(significand, -exponent);
        double parsed = decimal.doubleValue();
        if (parsed == Math.rint(parsed) && parsed < 0x1p63) {
            return true;
        }
        int comparison = new BigDecimal(parsed).compareTo(decimal);
        if (comparison == 0) {
            return (quotient & 1) != 0;
        }
        return comparison > 0;
    }

    /**
     * Writes significand * 10^exponent as d.dddE[-]x
     */
    private static Slice format(boolean negative, long significand, int exponent)
    {
        while (significand % 10 == 0) {
            significand /= 10;
            exponent++;
        }
        int digits = digitCount(significand);
        int scientificExponent = exponent + digits - 1;

        int exponentDigits = digitCount(Math.abs(scientificExponent));
        int length = digits + 1 + 1 + exponentDigits;
        if (digits == 1) {
            length++;
        }
        if (negative) {
            length++;
        }
        if (scientificExponent < 0) {
            length++;
        }

        byte[] bytes = new byte[length];
        int position = length - 1;
        int absoluteExponent = Math.abs(scientificExponent);
        for (int i = 0; i < exponentDigits; i++) {
            bytes[position--] = (byte) ('0' + absoluteExponent % 10);
            absoluteExponent /= 10;
        }
        if (scientificExponent < 0) {
            bytes[position--] = '-';
        }
        bytes[position--] = 'E';
        if (digits == 1) {
            bytes[position--] = '0';
        }
        else {
            for (int i = 0; i < digits - 1; i++) {
                bytes[position--] = (byte) ('0' + significand % 10);
                significand /= 10;
            }
        }
        bytes[position--] = '.';
        bytes[position--] = (byte) ('0' + significand);
        if (negative) {
            bytes[position] = '-';
        }
        return Slices.wrappedBuffer(bytes);
    }

    private static int digitCount(long value)
    {
        int digits = 1;
        while (digits < POWERS_OF_TEN.length && value >= POWERS_OF_TEN[digits]) {
            digits++;
        }
        return digits;
    }
}
