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

import org.junit.jupiter.api.Test;

import java.util.Random;

import static io.trino.plugin.base.util.NumberFormatter.decimalLength;
import static io.trino.plugin.base.util.NumberFormatter.formatLong;
import static org.assertj.core.api.Assertions.assertThat;

public class TestNumberFormatter
{
    @Test
    public void testMatchesJdk()
    {
        for (long value : new long[] {
                0, 1, -1, 9, -9, 10, -10, 99, -99, 100, -100,
                Long.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE - 1, Long.MIN_VALUE + 1,
                Integer.MAX_VALUE, Integer.MIN_VALUE, Short.MAX_VALUE, Short.MIN_VALUE, Byte.MAX_VALUE, Byte.MIN_VALUE,
                1_000_000_000_000_000_000L, -1_000_000_000_000_000_000L,
        }) {
            assertMatchesJdk(value);
        }

        // every power of ten and its neighbours, where the digit count changes
        for (long power = 1; power <= 1_000_000_000_000_000_000L; power *= 10) {
            assertMatchesJdk(power - 1);
            assertMatchesJdk(power);
            assertMatchesJdk(power + 1);
            assertMatchesJdk(-power + 1);
            assertMatchesJdk(-power);
            assertMatchesJdk(-power - 1);
        }
    }

    @Test
    public void testRandomValuesMatchJdk()
    {
        Random random = new Random(20260712);
        for (int i = 0; i < 500_000; i++) {
            assertMatchesJdk(random.nextLong());
            // small values are far more common than uniformly random longs
            assertMatchesJdk(random.nextLong(-1_000_000, 1_000_000));
        }
    }

    /**
     * The rendering must be identical to the JDK, so that switching a cast to the byte level
     * formatter cannot change a query result. The length must match too, because the cast rejects a
     * value that does not fit the target based on it.
     */
    private static void assertMatchesJdk(long value)
    {
        String expected = Long.toString(value);
        assertThat(decimalLength(value))
                .describedAs("decimalLength(%s)", value)
                .isEqualTo(expected.length());
        assertThat(formatLong(value).toStringUtf8())
                .describedAs("formatLong(%s)", value)
                .isEqualTo(expected);
    }
}
