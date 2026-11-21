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

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Random;

import static io.trino.util.ShortestDecimalFormatter.formatDouble;
import static io.trino.util.ShortestDecimalFormatter.formatReal;
import static java.lang.Math.nextDown;
import static java.lang.Math.nextUp;
import static java.lang.Math.scalb;
import static org.assertj.core.api.Assertions.assertThat;

class TestShortestDecimalFormatter
{
    @Test
    void testDoubleSpecialValues()
    {
        assertThat(formatDouble(0.0).toStringUtf8()).isEqualTo("0E0");
        assertThat(formatDouble(-0.0).toStringUtf8()).isEqualTo("-0E0");
        assertThat(formatDouble(Double.NaN).toStringUtf8()).isEqualTo("NaN");
        assertThat(formatDouble(Double.POSITIVE_INFINITY).toStringUtf8()).isEqualTo("Infinity");
        assertThat(formatDouble(Double.NEGATIVE_INFINITY).toStringUtf8()).isEqualTo("-Infinity");
    }

    @Test
    void testDoubleValues()
    {
        assertDouble(1.0, "1.0E0");
        assertDouble(2.5, "2.5E0");
        assertDouble(0.1, "1.0E-1");
        assertDouble(0.001, "1.0E-3");
        assertDouble(100.0, "1.0E2");
        assertDouble(123456.789, "1.23456789E5");
        assertDouble(Math.PI, "3.141592653589793E0");
        assertDouble(6.02214076e23, "6.02214076E23");
        assertDouble(1e22, "1.0E22");
        assertDouble(1e23, "1.0E23");
        assertDouble(0x1p53, "9.007199254740992E15");
        assertDouble(4503599627370497.0, "4.503599627370497E15");
        assertDouble(1.2345678901234568e17, "1.2345678901234568E17");
        assertDouble(Double.MAX_VALUE, "1.7976931348623157E308");
        assertDouble(Double.MIN_NORMAL, "2.2250738585072014E-308");
        assertDouble(Double.MIN_VALUE, "4.9E-324");
    }

    @Test
    void testRealSpecialValues()
    {
        assertThat(formatReal(0.0f).toStringUtf8()).isEqualTo("0E0");
        assertThat(formatReal(-0.0f).toStringUtf8()).isEqualTo("-0E0");
        assertThat(formatReal(Float.NaN).toStringUtf8()).isEqualTo("NaN");
        assertThat(formatReal(Float.POSITIVE_INFINITY).toStringUtf8()).isEqualTo("Infinity");
        assertThat(formatReal(Float.NEGATIVE_INFINITY).toStringUtf8()).isEqualTo("-Infinity");
    }

    @Test
    void testRealValues()
    {
        assertReal(1.0f, "1.0E0");
        assertReal(2.5f, "2.5E0");
        assertReal(0.1f, "1.0E-1");
        assertReal(0.3f, "3.0E-1");
        assertReal(100.0f, "1.0E2");
        assertReal(1e10f, "1.0E10");
        assertReal(Float.MIN_VALUE, "1.4E-45");
        // at most 7 significant digits
        assertReal(Float.MAX_VALUE, "3.402823E38");
        assertReal(Float.MIN_NORMAL, "1.175494E-38");
        assertReal(123456789f, "1.234568E8");
        assertReal(1.0000001f, "1.0E0");
        // ties: an exact tie rounds to even, but an integral value is treated as rounded down
        assertReal(1234567.5f, "1.234568E6");
        assertReal(1234568.5f, "1.234568E6");
        assertReal(10000125f, "1.000013E7");
        assertReal(16777216f, "1.677722E7");
        // ties between a float and the double parsed from its shortest digits are decided by the double
        assertReal(Float.intBitsToFloat(7136356), "1.000017E-38");
        assertReal(Float.intBitsToFloat(7136392), "1.000021E-38");
    }

    @Test
    void testDoubleRoundTrip()
    {
        Random random = new Random(1);
        for (int i = 0; i < 200_000; i++) {
            double value = Double.longBitsToDouble(random.nextLong());
            if (Double.isNaN(value) || Double.isInfinite(value) || value == 0) {
                continue;
            }
            String formatted = formatDouble(value).toStringUtf8();
            assertThat(Double.parseDouble(formatted)).describedAs(formatted).isEqualTo(value);
            assertThat(new BigDecimal(formatted).stripTrailingZeros())
                    .describedAs(formatted)
                    .isEqualByComparingTo(new BigDecimal(Double.toString(value)).stripTrailingZeros());
        }
    }

    @Test
    void testRealPrecision()
    {
        Random random = new Random(2);
        for (int i = 0; i < 200_000; i++) {
            float value = Float.intBitsToFloat(random.nextInt());
            if (Float.isNaN(value) || Float.isInfinite(value) || value == 0) {
                continue;
            }
            String formatted = formatReal(value).toStringUtf8();
            BigDecimal shortest = new BigDecimal(Float.toString(value));
            BigDecimal rounded = new BigDecimal(formatted);
            // within half a unit of the 7th significant digit, which is at most 5e-7 of the value
            assertThat(rounded.subtract(shortest).abs())
                    .describedAs("%s -> %s", Float.toString(value), formatted)
                    .isLessThanOrEqualTo(shortest.abs().multiply(new BigDecimal("5.0000001E-7")));
            assertThat(new BigDecimal(formatted).stripTrailingZeros().precision()).isLessThanOrEqualTo(7);
        }
    }

    @Test
    void testDoubleMatchesJdk()
    {
        Random random = new Random(3);
        for (int i = 0; i < 300_000; i++) {
            assertDoubleDigits(Double.longBitsToDouble(random.nextLong()));
            // values with a short decimal representation
            assertDoubleDigits(Double.parseDouble(random.nextLong(1, 1_000_000_000_000_000_000L) + "e" + random.nextInt(-320, 300)));
            assertDoubleDigits(Double.parseDouble(random.nextInt(1, 100_000) + "e" + random.nextInt(-12, 12)));
            // integral values around the limit of exact integers
            assertDoubleDigits(random.nextLong(1L << random.nextInt(1, 63)));
            // subnormals
            assertDoubleDigits(Double.longBitsToDouble(random.nextLong(1L << random.nextInt(1, 53))));
        }
        // powers of two have a narrower interval below
        for (int exponent = -1074; exponent <= 1023; exponent++) {
            double value = scalb(1.0, exponent);
            assertDoubleDigits(value);
            assertDoubleDigits(nextUp(value));
            assertDoubleDigits(nextDown(value));
            assertDoubleDigits(scalb((double) ((1L << 53) - 1), exponent));
        }
    }

    @Test
    void testRealMatchesJdk()
    {
        Random random = new Random(4);
        for (int i = 0; i < 300_000; i++) {
            assertRealDigits(Float.intBitsToFloat(random.nextInt()));
            assertRealDigits(Float.parseFloat(random.nextInt(1, 1_000_000_000) + "e" + random.nextInt(-45, 38)));
            assertRealDigits(Float.intBitsToFloat(random.nextInt(1 << random.nextInt(1, 24))));
        }
        for (int exponent = -149; exponent <= 127; exponent++) {
            float value = scalb(1.0f, exponent);
            assertRealDigits(value);
            assertRealDigits(Math.nextUp(value));
            assertRealDigits(Math.nextDown(value));
        }
    }

    @Test
    void testPowerOfTenTable()
    {
        for (int e2 = -1074; e2 <= 971; e2++) {
            int regular = ShortestDecimalFormatter.Tables.floorLog10Pow2(e2);
            assertThat(regular).describedAs("e2 = %s", e2).isEqualTo(floorLog10(powerOfTwo(e2)));
            int narrower = ShortestDecimalFormatter.Tables.floorLog10ThreeQuartersPow2(e2);
            assertThat(narrower).describedAs("e2 = %s", e2).isEqualTo(floorLog10(powerOfTwo(e2 - 2).multiply(BigDecimal.valueOf(3))));
            for (int e10 : new int[] {regular, narrower}) {
                int shift = ShortestDecimalFormatter.Tables.powerExponent(e10) - e2 - 64;
                assertThat(shift).describedAs("e2 = %s, e10 = %s", e2, e10).isBetween(57, 63);
            }
        }
    }

    private static BigDecimal powerOfTwo(int exponent)
    {
        if (exponent >= 0) {
            return new BigDecimal(BigInteger.ONE.shiftLeft(exponent));
        }
        return BigDecimal.ONE.divide(new BigDecimal(BigInteger.ONE.shiftLeft(-exponent)));
    }

    private static int floorLog10(BigDecimal value)
    {
        return value.precision() - value.scale() - 1;
    }

    private static void assertDoubleDigits(double value)
    {
        if (Double.isNaN(value) || Double.isInfinite(value) || value == 0) {
            return;
        }
        BigDecimal expected = new BigDecimal(Double.toString(value)).stripTrailingZeros();
        assertThat(new BigDecimal(ShortestDecimalFormatter.formatDouble(value, false).toStringUtf8()).stripTrailingZeros())
                .describedAs("fast path for %s", value)
                .isEqualTo(expected);
        if (Double.doubleToLongBits(value) % 97 == 0) {
            assertThat(new BigDecimal(ShortestDecimalFormatter.formatDouble(value, true).toStringUtf8()).stripTrailingZeros())
                    .describedAs("exact path for %s", value)
                    .isEqualTo(expected);
        }
    }

    private static void assertRealDigits(float value)
    {
        if (Float.isNaN(value) || Float.isInfinite(value) || value == 0) {
            return;
        }
        BigDecimal expected = new BigDecimal(Float.toString(value)).stripTrailingZeros();
        assertThat(new BigDecimal(ShortestDecimalFormatter.formatReal(value, Integer.MAX_VALUE, false).toStringUtf8()).stripTrailingZeros())
                .describedAs("fast path for %s", value)
                .isEqualTo(expected);
        if (Float.floatToIntBits(value) % 97 == 0) {
            assertThat(new BigDecimal(ShortestDecimalFormatter.formatReal(value, Integer.MAX_VALUE, true).toStringUtf8()).stripTrailingZeros())
                    .describedAs("exact path for %s", value)
                    .isEqualTo(expected);
        }
    }

    private static void assertDouble(double value, String expected)
    {
        assertThat(formatDouble(value).toStringUtf8()).isEqualTo(expected);
        assertThat(formatDouble(-value).toStringUtf8()).isEqualTo("-" + expected);
    }

    private static void assertReal(float value, String expected)
    {
        assertThat(formatReal(value).toStringUtf8()).isEqualTo(expected);
        assertThat(formatReal(-value).toStringUtf8()).isEqualTo("-" + expected);
    }
}
