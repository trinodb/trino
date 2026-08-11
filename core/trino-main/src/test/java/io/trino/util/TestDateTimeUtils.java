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

import io.trino.spi.TrinoException;
import io.trino.sql.tree.IntervalField;
import org.junit.jupiter.api.Test;

import java.time.DateTimeException;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.Callable;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.util.DateTimeUtils.parseDayTimeInterval;
import static io.trino.util.DateTimeUtils.parseIfIso8601DateFormat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDateTimeUtils
{
    private static final IntervalField DAY = new IntervalField.Day();
    private static final IntervalField HOUR = new IntervalField.Hour();
    private static final IntervalField MINUTE = new IntervalField.Minute();
    private static final IntervalField SECOND = new IntervalField.Second(OptionalInt.empty());

    @Test
    public void testParseDayTimeIntervalRejectsExcessFractionalPrecision()
    {
        // Millisecond precision is accepted.
        assertThat(parseDayTimeInterval("1.123", SECOND, Optional.empty()))
                .isEqualTo(millisIn(0, 0, 0, 1, 123));
        assertThat(parseDayTimeInterval(".123", SECOND, Optional.empty()))
                .isEqualTo(millisIn(0, 0, 0, 0, 123));
        assertThat(parseDayTimeInterval("5:10:1.123", HOUR, Optional.of(SECOND)))
                .isEqualTo(millisIn(0, 5, 10, 1, 123));
        assertThat(parseDayTimeInterval("2 5:10:1.123", DAY, Optional.of(SECOND)))
                .isEqualTo(millisIn(2, 5, 10, 1, 123));

        // More than 3 fractional digits must fail rather than silently truncate.
        // See https://github.com/trinodb/trino/issues/6754
        assertThrowsInvalidInterval(() -> parseDayTimeInterval(".0001", SECOND, Optional.empty()));
        assertThrowsInvalidInterval(() -> parseDayTimeInterval("1.1234", SECOND, Optional.empty()));
        assertThrowsInvalidInterval(() -> parseDayTimeInterval("1,1234", SECOND, Optional.empty()));
        assertThrowsInvalidInterval(() -> parseDayTimeInterval("-1.1234", SECOND, Optional.empty()));
        assertThrowsInvalidInterval(() -> parseDayTimeInterval("10:1.1234", MINUTE, Optional.of(SECOND)));
        assertThrowsInvalidInterval(() -> parseDayTimeInterval("5:10:1.1234", HOUR, Optional.of(SECOND)));
        assertThrowsInvalidInterval(() -> parseDayTimeInterval("2 5:10:1.1234", DAY, Optional.of(SECOND)));
    }

    private static long millisIn(long days, long hours, long minutes, long seconds, long millis)
    {
        return 86_400_000L * days
                + 3_600_000L * hours
                + 60_000L * minutes
                + 1_000L * seconds
                + millis;
    }

    private static void assertThrowsInvalidInterval(Callable<Long> callable)
    {
        assertThatThrownBy(callable::call)
                .isInstanceOf(TrinoException.class)
                .hasMessageStartingWith("Invalid INTERVAL");
    }

    @Test
    @SuppressWarnings("OptionalGetWithoutIsPresent")
    public void testParseIfIso8601DateFormat()
    {
        // valid dates
        assertThat(0)
                .describedAs("1970-01-01")
                .isEqualTo(parseIfIso8601DateFormat(utf8Slice("1970-01-01")).orElseThrow());
        assertThat(31)
                .describedAs("1970-02-01")
                .isEqualTo(parseIfIso8601DateFormat(utf8Slice("1970-02-01")).orElseThrow());
        assertThat(-31)
                .describedAs("1969-12-01")
                .isEqualTo(parseIfIso8601DateFormat(utf8Slice("1969-12-01")).orElseThrow());
        assertThat(19051)
                .describedAs("2022-02-28")
                .isEqualTo(parseIfIso8601DateFormat(utf8Slice("2022-02-28")).orElseThrow());
        assertThat(-719528)
                .describedAs("0000-01-01")
                .isEqualTo(parseIfIso8601DateFormat(utf8Slice("0000-01-01")).orElseThrow());
        assertThat(2932896)
                .describedAs("9999-12-31")
                .isEqualTo(parseIfIso8601DateFormat(utf8Slice("9999-12-31")).orElseThrow());

        // format invalid
        // invalid length
        assertThat(parseIfIso8601DateFormat(utf8Slice("1970-2-01"))).isEmpty();
        // invalid year0
        assertThat(parseIfIso8601DateFormat(utf8Slice("a970-02-10"))).isEmpty();
        // invalid year1
        assertThat(parseIfIso8601DateFormat(utf8Slice("1p70-02-10"))).isEmpty();
        // invalid year2
        assertThat(parseIfIso8601DateFormat(utf8Slice("19%0-02-10"))).isEmpty();
        // invalid year3
        assertThat(parseIfIso8601DateFormat(utf8Slice("197o-02-10"))).isEmpty();
        // invalid dash0
        assertThat(parseIfIso8601DateFormat(utf8Slice("1970_02-01"))).isEmpty();
        // invalid month0
        assertThat(parseIfIso8601DateFormat(utf8Slice("1970- 2-01"))).isEmpty();
        // invalid month1
        assertThat(parseIfIso8601DateFormat(utf8Slice("1970-3.-01"))).isEmpty();
        // invalid dash0
        assertThat(parseIfIso8601DateFormat(utf8Slice("1970-02/01"))).isEmpty();
        // invalid day0
        assertThat(parseIfIso8601DateFormat(utf8Slice("1970-02-/1"))).isEmpty();
        // invalid day1
        assertThat(parseIfIso8601DateFormat(utf8Slice("1970-12-0l"))).isEmpty();

        assertThat(parseIfIso8601DateFormat(utf8Slice("1970/02/01"))).isEmpty();
        assertThat(parseIfIso8601DateFormat(utf8Slice("Dec 24 2022"))).isEmpty();

        // format ok, but illegal value
        assertThatThrownBy(() -> parseIfIso8601DateFormat(utf8Slice("2022-02-29")))
                .isInstanceOf(DateTimeException.class)
                .hasMessage("Invalid date 'February 29' as '2022' is not a leap year");
        assertThatThrownBy(() -> parseIfIso8601DateFormat(utf8Slice("1970-32-01")))
                .isInstanceOf(DateTimeException.class)
                .hasMessage("Invalid value for MonthOfYear (valid values 1 - 12): 32");
        assertThatThrownBy(() -> parseIfIso8601DateFormat(utf8Slice("1970-02-41")))
                .isInstanceOf(DateTimeException.class)
                .hasMessage("Invalid value for DayOfMonth (valid values 1 - 28/31): 41");
    }
}
