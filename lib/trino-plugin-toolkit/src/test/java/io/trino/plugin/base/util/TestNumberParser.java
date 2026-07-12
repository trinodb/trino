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
import io.airlift.slice.Slices;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestNumberParser
{
    @Test
    public void testPlainDecimals()
    {
        for (String value : new String[] {
                "0", "-0", "+0", "0.0", "-0.0", "1", "-1", "+1", "1.5", "-1.5", "0.5", ".5", "-.5",
                "1.", "-1.", "12.30", "000001.5", "0000000000000000000000001.5",
                "3.141592653589793", "2.718281828459045",
                "9007199254740992", "9007199254740993", "9007199254740991.5",
                "123456789012345678901234567890", "0.000000000000000000000001",
                "16777216", "16777217", "1e5", "1E5", "1.5e-3", "-1.5E+3",
                "Infinity", "-Infinity", "NaN", "0x1p3", "1.5f", "1.5d", " 1.5", "1.5 ", "\t1.5",
        }) {
            assertMatchesJdk(value);
        }
    }

    @Test
    public void testMalformed()
    {
        for (String value : new String[] {"", ".", "-", "+", "-.", "1.2.3", "1,5", "abc", "--1", "1-", "1 5"}) {
            Slice slice = Slices.utf8Slice(value);
            assertThatThrownBy(() -> NumberParser.parseDouble(slice, 0, slice.length()))
                    .describedAs("parseDouble(%s)", value)
                    .isInstanceOf(NumberFormatException.class);
            assertThatThrownBy(() -> NumberParser.parseFloat(slice, 0, slice.length()))
                    .describedAs("parseFloat(%s)", value)
                    .isInstanceOf(NumberFormatException.class);
        }
    }

    @Test
    public void testRandomValuesMatchJdk()
    {
        Random random = new Random(9876);
        for (int i = 0; i < 200_000; i++) {
            assertMatchesJdk(randomDecimal(random));
        }
    }

    @Test
    public void testEveryScaleMatchesJdk()
    {
        // the fast path is only valid up to a bounded number of fractional digits, so walk past it
        Random random = new Random(4242);
        StringBuilder value = new StringBuilder("1.");
        for (int scale = 1; scale <= 40; scale++) {
            value.append((char) ('0' + random.nextInt(10)));
            assertMatchesJdk(value.toString());
            assertMatchesJdk("-" + value);
        }
    }

    @Test
    public void testRespectsOffsetAndLength()
    {
        Slice slice = Slices.utf8Slice("xx1.5yy");
        assertThat(NumberParser.parseDouble(slice, 2, 3)).isEqualTo(1.5);
        assertThat(NumberParser.parseFloat(slice, 2, 3)).isEqualTo(1.5f);
    }

    @Test
    public void testShortDecimalMatchesBigDecimal()
    {
        for (int scale : new int[] {0, 1, 2, 6, 12, 18}) {
            for (String value : new String[] {
                    "0", "-0", "1", "-1", "1.5", "-1.5", "0.5", "-0.5", ".5", "1.", "12.345", "-12.345",
                    // half up must round away from zero on both signs, and only at the boundary
                    "0.4", "0.5", "0.6", "-0.4", "-0.5", "-0.6",
                    "1.05", "1.15", "1.25", "-1.05", "-1.15", "-1.25",
                    "0.049", "0.05", "0.051", "-0.049", "-0.05", "-0.051",
                    "9.999999999999999", "0.000000000000000001", "000001.5",
            }) {
                assertShortDecimalMatchesBigDecimal(value, scale);
            }
        }
    }

    @Test
    public void testRandomShortDecimalsMatchBigDecimal()
    {
        Random random = new Random(555);
        for (int i = 0; i < 200_000; i++) {
            assertShortDecimalMatchesBigDecimal(randomDecimal(random), random.nextInt(19));
        }
    }

    @Test
    public void testNotShortDecimal()
    {
        // exponents and malformed input are left to BigDecimal
        for (String value : new String[] {"1e5", "1.5E3", "", ".", "-", "abc", "1.2.3", "Infinity", "NaN", " 1.5"}) {
            Slice slice = Slices.utf8Slice(value);
            assertThat(NumberParser.parseShortDecimal(slice, 0, slice.length(), 2))
                    .describedAs("parseShortDecimal(%s)", value)
                    .isEqualTo(NumberParser.NOT_SHORT_DECIMAL);
        }
    }

    /**
     * The unscaled value must be identical to what the BigDecimal path produces, so that switching
     * a column to the byte level parser cannot change a query result.
     */
    private static void assertShortDecimalMatchesBigDecimal(String value, int scale)
    {
        Slice slice = Slices.utf8Slice(value);
        long actual = NumberParser.parseShortDecimal(slice, 0, slice.length(), scale);
        if (actual == NumberParser.NOT_SHORT_DECIMAL) {
            return;
        }

        BigDecimal expected = new BigDecimal(value).setScale(scale, RoundingMode.HALF_UP);
        assertThat(actual)
                .describedAs("parseShortDecimal(%s, scale=%s)", value, scale)
                .isEqualTo(expected.unscaledValue().longValueExact());
    }

    private static String randomDecimal(Random random)
    {
        StringBuilder builder = new StringBuilder();
        if (random.nextBoolean()) {
            builder.append('-');
        }
        int integerDigits = random.nextInt(20);
        for (int i = 0; i < integerDigits; i++) {
            builder.append((char) ('0' + random.nextInt(10)));
        }
        if (integerDigits == 0) {
            builder.append((char) ('0' + random.nextInt(10)));
        }
        if (random.nextBoolean()) {
            builder.append('.');
            int fractionDigits = random.nextInt(25);
            for (int i = 0; i < fractionDigits; i++) {
                builder.append((char) ('0' + random.nextInt(10)));
            }
        }
        return builder.toString();
    }

    /**
     * The parsed value must be bit for bit identical to the JDK, so that switching a column to the
     * byte level parser cannot change a query result.
     */
    private static void assertMatchesJdk(String value)
    {
        Slice slice = Slices.utf8Slice(value);

        double expectedDouble = Double.parseDouble(value);
        double actualDouble = NumberParser.parseDouble(slice, 0, slice.length());
        assertThat(Double.doubleToRawLongBits(actualDouble))
                .describedAs("parseDouble(%s): expected %s, got %s", value, expectedDouble, actualDouble)
                .isEqualTo(Double.doubleToRawLongBits(expectedDouble));

        float expectedFloat = Float.parseFloat(value);
        float actualFloat = NumberParser.parseFloat(slice, 0, slice.length());
        assertThat(Float.floatToRawIntBits(actualFloat))
                .describedAs("parseFloat(%s): expected %s, got %s", value, expectedFloat, actualFloat)
                .isEqualTo(Float.floatToRawIntBits(expectedFloat));
    }
}
