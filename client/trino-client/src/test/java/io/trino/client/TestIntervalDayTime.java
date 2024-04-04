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

import static io.trino.client.IntervalDayTime.formatMillis;
import static io.trino.client.IntervalDayTime.parseMillis;
import static io.trino.client.IntervalDayTime.toMillis;
import static java.util.concurrent.TimeUnit.DAYS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestIntervalDayTime
{
    @Test
    public void testFormat()
    {
        assertMillis(0, "0 00:00:00.000");
        assertMillis(1, "0 00:00:00.001");
        assertMillis(-1, "-0 00:00:00.001");

        assertMillis(toMillis(12, 13, 45, 56, 789), "12 13:45:56.789");
        assertMillis(toMillis(-12, -13, -45, -56, -789), "-12 13:45:56.789");

        assertMillis(Long.MAX_VALUE, "106751991167 07:12:55.807");
        assertMillis(Long.MIN_VALUE + 1, "-106751991167 07:12:55.807");
        assertMillis(Long.MIN_VALUE, "-106751991167 07:12:55.808");
    }

    private static void assertMillis(long millis, String formatted)
    {
        assertThat(formatMillis(millis)).isEqualTo(formatted);
        assertThat(parseMillis(formatted)).isEqualTo(millis);
    }

    @Test
    public void textMaxDays()
    {
        long days = Long.MAX_VALUE / DAYS.toMillis(1);
        assertThat(toMillis(days, 0, 0, 0, 0)).isEqualTo(DAYS.toMillis(days));
    }

    @Test
    public void testOverflow()
    {
        long days = (Long.MAX_VALUE / DAYS.toMillis(1)) + 1;
        assertThatThrownBy(() -> toMillis(days, 0, 0, 0, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("java.lang.ArithmeticException: long overflow");
    }
}
