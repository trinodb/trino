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

import static io.trino.client.IntervalDayTime.formatInterval;
import static io.trino.client.IntervalDayTime.formatMicros;
import static io.trino.client.IntervalDayTime.parseMicros;
import static io.trino.client.IntervalDayTime.parseToPicos;
import static io.trino.client.IntervalDayTime.toMicros;
import static java.util.concurrent.TimeUnit.DAYS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestIntervalDayTime
{
    @Test
    public void testFormat()
    {
        assertMicros(0, "0 00:00:00.000000");
        assertMicros(1, "0 00:00:00.000001");
        assertMicros(-1, "-0 00:00:00.000001");

        assertMicros(toMicros(12, 13, 45, 56, 789000), "12 13:45:56.789000");
        assertMicros(toMicros(-12, -13, -45, -56, -789000), "-12 13:45:56.789000");

        assertMicros(Long.MAX_VALUE, "106751991 04:00:54.775807");
        assertMicros(Long.MIN_VALUE + 1, "-106751991 04:00:54.775807");
        assertMicros(Long.MIN_VALUE, "-106751991 04:00:54.775808");
    }

    private static void assertMicros(long micros, String formatted)
    {
        assertThat(formatMicros(micros)).isEqualTo(formatted);
        assertThat(parseMicros(formatted)).isEqualTo(micros);
    }

    @Test
    public void testPicosecondRoundTrip()
    {
        // a value with picoseconds round-trips through the twelve-digit rendering and parse
        assertPicos(1_123_456, 789_012, 12, "0 00:00:01.123456789012");
        assertPicos(1_123_456, 789_000, 9, "0 00:00:01.123456789");
        assertPicos(-1_123_457, 211_000, 9, "-0 00:00:01.123456789");

        // a precision of zero renders and parses with no decimal point
        assertPicos(2_000_000, 0, 0, "0 00:00:02");

        // the sub-microsecond fraction is dropped when it falls below the rendered precision
        assertThat(parseToPicos("0 00:00:01.123456")).containsExactly(1_123_456, 0);
    }

    private static void assertPicos(long micros, int picosOfMicro, int fractionalPrecision, String formatted)
    {
        assertThat(formatInterval(micros, picosOfMicro, fractionalPrecision)).isEqualTo(formatted);
        assertThat(parseToPicos(formatted)).containsExactly(micros, picosOfMicro);
    }

    @Test
    public void textMaxDays()
    {
        long days = Long.MAX_VALUE / DAYS.toMicros(1);
        assertThat(toMicros(days, 0, 0, 0, 0)).isEqualTo(DAYS.toMicros(days));
    }

    @Test
    public void testOverflow()
    {
        long days = (Long.MAX_VALUE / DAYS.toMicros(1)) + 1;
        assertThatThrownBy(() -> toMicros(days, 0, 0, 0, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("java.lang.ArithmeticException: long overflow");
    }
}
