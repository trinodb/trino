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
package io.trino.operator.scalar;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneId;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.operator.scalar.TimestampField.DAY;
import static io.trino.operator.scalar.TimestampField.HOUR;
import static io.trino.operator.scalar.TimestampField.MONTH;
import static io.trino.operator.scalar.TimestampField.QUARTER;
import static io.trino.operator.scalar.TimestampField.YEAR;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTimestampField
{
    @Test
    public void testClippedMonthEnds()
    {
        long january = millis("2023-01-31T12:00:00Z");
        long february = millis("2023-02-28T12:00:00Z");
        assertThat(MONTH.add(january, 1, UTC)).isEqualTo(february);
        assertThat(MONTH.difference(february, january, UTC)).isEqualTo(1);
        assertThat(MONTH.difference(january, february, UTC)).isEqualTo(-1);
        assertThat(MONTH.difference(february - 1, january, UTC)).isEqualTo(0);
        assertThat(QUARTER.add(january, 1, UTC)).isEqualTo(millis("2023-04-30T12:00:00Z"));
        assertThat(QUARTER.difference(millis("2023-04-30T12:00:00Z"), january, UTC)).isEqualTo(1);
        assertThat(YEAR.difference(millis("2025-02-28T12:00:00Z"), millis("2024-02-29T12:00:00Z"), UTC)).isEqualTo(1);
    }

    @Test
    public void testDaylightSavingArithmetic()
    {
        ZoneId zoneId = ZoneId.of("America/New_York");
        long start = millis("2024-03-09T17:00:00Z");
        long nextDay = millis("2024-03-10T16:00:00Z");
        assertThat(DAY.add(start, 1, zoneId)).isEqualTo(nextDay);
        assertThat(HOUR.add(start, 24, zoneId)).isEqualTo(millis("2024-03-10T17:00:00Z"));
        assertThat(DAY.difference(nextDay, start, zoneId)).isEqualTo(1);
        assertThat(HOUR.difference(nextDay, start, zoneId)).isEqualTo(23);
    }

    @Test
    public void testOverlapTruncation()
    {
        ZoneId zoneId = ZoneId.of("America/New_York");
        long laterOccurrence = millis("2024-11-03T06:30:00Z");
        assertThat(HOUR.truncate(laterOccurrence, zoneId)).isEqualTo(millis("2024-11-03T06:00:00Z"));
        assertThat(DAY.truncate(laterOccurrence, zoneId)).isEqualTo(millis("2024-11-03T04:00:00Z"));
    }

    @Test
    public void testFields()
    {
        assertThat(TimestampField.forTimestamp(utf8Slice("MoNtH"))).isEqualTo(MONTH);
        assertThat(TimestampField.forDate(utf8Slice("YEAR"))).isEqualTo(YEAR);
        assertThatThrownBy(() -> TimestampField.forDate(utf8Slice("hour")))
                .hasMessage("'hour' is not a valid DATE field");
        assertThatThrownBy(() -> TimestampField.forTimestamp(utf8Slice("invalid")))
                .hasMessage("'invalid' is not a valid TIMESTAMP field");
    }

    @Test
    public void testZeroYearAdditionAtBounds()
    {
        assertThat(YEAR.add(Long.MIN_VALUE, 0, UTC)).isEqualTo(Long.MIN_VALUE);
        assertThat(YEAR.add(Long.MAX_VALUE, 0, UTC)).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    public void testOversizedCalendarAdditions()
    {
        long start = millis("0001-01-01T00:00:00Z");
        long value = 30L * 24 * 60 * 60 * 1000;
        assertThatThrownBy(() -> QUARTER.add(start, value, UTC))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("Magnitude of add amount is too large:");
        assertThatThrownBy(() -> YEAR.add(start, value, UTC))
                .isInstanceOf(ArithmeticException.class)
                .hasMessage("Value cannot fit in an int: " + value);
        assertThatThrownBy(() -> MONTH.add(start, Long.MAX_VALUE, UTC))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("Magnitude of add amount is too large:");
    }

    private static long millis(String value)
    {
        return Instant.parse(value).toEpochMilli();
    }
}
