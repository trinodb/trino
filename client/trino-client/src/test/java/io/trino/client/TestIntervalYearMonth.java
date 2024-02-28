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
package io.trino.client;

import org.junit.jupiter.api.Test;

import static io.trino.client.IntervalYearMonth.formatMonths;
import static io.trino.client.IntervalYearMonth.parseMonths;
import static io.trino.client.IntervalYearMonth.toMonths;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestIntervalYearMonth
{
    @Test
    public void testFormat()
    {
        assertMonths(0, "0-0");
        assertMonths(toMonths(0, 0), "0-0");

        assertMonths(3, "0-3");
        assertMonths(-3, "-0-3");
        assertMonths(toMonths(0, 3), "0-3");
        assertMonths(toMonths(0, -3), "-0-3");

        assertMonths(28, "2-4");
        assertMonths(-28, "-2-4");

        assertMonths(toMonths(2, 4), "2-4");
        assertMonths(toMonths(-2, -4), "-2-4");

        assertMonths(Integer.MAX_VALUE, "178956970-7");
        assertMonths(Integer.MIN_VALUE + 1, "-178956970-7");
        assertMonths(Integer.MIN_VALUE, "-178956970-8");
    }

    private static void assertMonths(int months, String formatted)
    {
        assertThat(formatMonths(months)).isEqualTo(formatted);
        assertThat(parseMonths(formatted)).isEqualTo(months);
    }

    @Test
    public void testMaxYears()
    {
        int years = Integer.MAX_VALUE / 12;
        assertThat(toMonths(years, 0)).isEqualTo(years * 12);
    }

    @Test
    public void testOverflow()
    {
        int days = (Integer.MAX_VALUE / 12) + 1;
        assertThatThrownBy(() -> toMonths(days, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("java.lang.ArithmeticException: integer overflow");
    }
}
