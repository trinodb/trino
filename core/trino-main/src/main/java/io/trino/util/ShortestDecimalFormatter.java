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
 */
public final class ShortestDecimalFormatter
{
    private static final int MAX_REAL_DIGITS = 7;
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
        if (Double.isNaN(value)) {
            return Slices.utf8Slice("NaN");
        }
        boolean negative = doubleToRawLongBits(value) < 0;
        if (Double.isInfinite(value)) {
            return Slices.utf8Slice(negative ? "-Infinity" : "Infinity");
        }
        if (value == 0) {
            return Slices.utf8Slice(negative ? "-0E0" : "0E0");
        }
        return fromJavaString(Double.toString(Math.abs(value)), negative, Integer.MAX_VALUE);
    }

    public static Slice formatReal(float value)
    {
        if (Float.isNaN(value)) {
            return Slices.utf8Slice("NaN");
        }
        boolean negative = floatToRawIntBits(value) < 0;
        if (Float.isInfinite(value)) {
            return Slices.utf8Slice(negative ? "-Infinity" : "Infinity");
        }
        if (value == 0) {
            return Slices.utf8Slice(negative ? "-0E0" : "0E0");
        }
        return fromJavaString(Float.toString(Math.abs(value)), negative, MAX_REAL_DIGITS);
    }

    // Splits the output of Double.toString or Float.toString into significand and exponent
    private static Slice fromJavaString(String string, boolean negative, int maxDigits)
    {
        long significand = 0;
        int exponent = 0;
        int fractionDigits = 0;
        boolean afterPoint = false;
        int length = string.length();
        int i = 0;
        for (; i < length; i++) {
            char c = string.charAt(i);
            if (c == '.') {
                afterPoint = true;
            }
            else if (c == 'E') {
                exponent = Integer.parseInt(string, i + 1, length, 10);
                break;
            }
            else {
                significand = significand * 10 + (c - '0');
                if (afterPoint) {
                    fractionDigits++;
                }
            }
        }
        exponent -= fractionDigits;
        return formatRounded(negative, significand, exponent, maxDigits);
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
