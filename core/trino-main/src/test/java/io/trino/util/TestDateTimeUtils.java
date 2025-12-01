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

import io.trino.sql.tree.IntervalLiteral;
import io.trino.sql.tree.IntervalLiteral.IntervalField;
import org.junit.jupiter.api.Test;

import java.time.DateTimeException;
import java.time.Duration;
import java.util.Optional;

import static io.trino.sql.tree.IntervalLiteral.IntervalField.DAY;
import static io.trino.sql.tree.IntervalLiteral.IntervalField.HOUR;
import static io.trino.sql.tree.IntervalLiteral.IntervalField.MINUTE;
import static io.trino.sql.tree.IntervalLiteral.IntervalField.SECOND;
import static io.trino.sql.tree.IntervalLiteral.Sign.NEGATIVE;
import static io.trino.sql.tree.IntervalLiteral.Sign.POSITIVE;
import static io.trino.util.DateTimeUtils.formatDayTimeInterval;
import static io.trino.util.DateTimeUtils.parseDayTimeInterval;
import static io.trino.util.DateTimeUtils.parseIfIso8601DateFormat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDateTimeUtils
{
    @Test
    @SuppressWarnings("OptionalGetWithoutIsPresent")
    public void testParseIfIso8601DateFormat()
    {
        // valid dates
        assertThat(0)
                .describedAs("1970-01-01")
                .isEqualTo(parseIfIso8601DateFormat("1970-01-01").getAsInt());
        assertThat(31)
                .describedAs("1970-02-01")
                .isEqualTo(parseIfIso8601DateFormat("1970-02-01").getAsInt());
        assertThat(-31)
                .describedAs("1969-12-01")
                .isEqualTo(parseIfIso8601DateFormat("1969-12-01").getAsInt());
        assertThat(19051)
                .describedAs("2022-02-28")
                .isEqualTo(parseIfIso8601DateFormat("2022-02-28").getAsInt());
        assertThat(-719528)
                .describedAs("0000-01-01")
                .isEqualTo(parseIfIso8601DateFormat("0000-01-01").getAsInt());
        assertThat(2932896)
                .describedAs("9999-12-31")
                .isEqualTo(parseIfIso8601DateFormat("9999-12-31").getAsInt());

        // format invalid
        // invalid length
        assertThat(parseIfIso8601DateFormat("1970-2-01")).isEmpty();
        // invalid year0
        assertThat(parseIfIso8601DateFormat("a970-02-10")).isEmpty();
        // invalid year1
        assertThat(parseIfIso8601DateFormat("1p70-02-10")).isEmpty();
        // invalid year2
        assertThat(parseIfIso8601DateFormat("19%0-02-10")).isEmpty();
        // invalid year3
        assertThat(parseIfIso8601DateFormat("197o-02-10")).isEmpty();
        // invalid dash0
        assertThat(parseIfIso8601DateFormat("1970_02-01")).isEmpty();
        // invalid month0
        assertThat(parseIfIso8601DateFormat("1970- 2-01")).isEmpty();
        // invalid month1
        assertThat(parseIfIso8601DateFormat("1970-3.-01")).isEmpty();
        // invalid dash0
        assertThat(parseIfIso8601DateFormat("1970-02/01")).isEmpty();
        // invalid day0
        assertThat(parseIfIso8601DateFormat("1970-02-/1")).isEmpty();
        // invalid day1
        assertThat(parseIfIso8601DateFormat("1970-12-0l")).isEmpty();

        assertThat(parseIfIso8601DateFormat("1970/02/01")).isEmpty();
        assertThat(parseIfIso8601DateFormat("Dec 24 2022")).isEmpty();

        // format ok, but illegal value
        assertThatThrownBy(() -> parseIfIso8601DateFormat("2022-02-29"))
                .isInstanceOf(DateTimeException.class)
                .hasMessage("Invalid date 'February 29' as '2022' is not a leap year");
        assertThatThrownBy(() -> parseIfIso8601DateFormat("1970-32-01"))
                .isInstanceOf(DateTimeException.class)
                .hasMessage("Invalid value for MonthOfYear (valid values 1 - 12): 32");
        assertThatThrownBy(() -> parseIfIso8601DateFormat("1970-02-41"))
                .isInstanceOf(DateTimeException.class)
                .hasMessage("Invalid value for DayOfMonth (valid values 1 - 28/31): 41");
    }

    @Test
    void testDayTimeIntervalRoundTrip()
    {
        testDayTimeIntervalRoundTrip("0", SECOND, Optional.empty());
        testDayTimeIntervalRoundTrip("0.000", SECOND, Optional.empty(), "0", SECOND, Optional.empty());
        testDayTimeIntervalRoundTrip("45", SECOND, Optional.empty());
        testDayTimeIntervalRoundTrip("0.555", SECOND, Optional.empty());
        testDayTimeIntervalRoundTrip("59.999", SECOND, Optional.empty());
        testDayTimeIntervalRoundTrip("60", SECOND, Optional.empty(), "1", MINUTE, Optional.empty());
        testDayTimeIntervalRoundTrip("61", SECOND, Optional.empty(), "1:01", MINUTE, Optional.of(SECOND));
        testDayTimeIntervalRoundTrip("3661", SECOND, Optional.empty(), "1:01:01", HOUR, Optional.of(SECOND));
        testDayTimeIntervalRoundTrip("90061", SECOND, Optional.empty(), "1 1:01:01", DAY, Optional.of(SECOND));

        testDayTimeIntervalRoundTrip("0", MINUTE, Optional.empty(), "0", SECOND, Optional.empty());
        testDayTimeIntervalRoundTrip("25", MINUTE, Optional.empty());
        testDayTimeIntervalRoundTrip("15:30", MINUTE, Optional.of(SECOND));
        testDayTimeIntervalRoundTrip("59:00.999", MINUTE, Optional.of(SECOND));
        testDayTimeIntervalRoundTrip("60", MINUTE, Optional.empty(), "1", HOUR, Optional.empty());
        testDayTimeIntervalRoundTrip("61", MINUTE, Optional.empty(), "1:01", HOUR, Optional.of(MINUTE));
        testDayTimeIntervalRoundTrip("1500", MINUTE, Optional.empty(), "1 1", DAY, Optional.of(HOUR));
        testDayTimeIntervalRoundTrip("1501", MINUTE, Optional.empty(), "1 1:01", DAY, Optional.of(MINUTE));

        testDayTimeIntervalRoundTrip("0", HOUR, Optional.empty(), "0", SECOND, Optional.empty());
        testDayTimeIntervalRoundTrip("8", HOUR, Optional.empty());
        testDayTimeIntervalRoundTrip("2:45", HOUR, Optional.of(MINUTE));
        testDayTimeIntervalRoundTrip("2:00:45", HOUR, Optional.of(SECOND));
        testDayTimeIntervalRoundTrip("1:30:45", HOUR, Optional.of(SECOND));
        testDayTimeIntervalRoundTrip("1:00:00.999", HOUR, Optional.of(SECOND));
        testDayTimeIntervalRoundTrip("24", HOUR, Optional.empty(), "1", DAY, Optional.empty());
        testDayTimeIntervalRoundTrip("25", HOUR, Optional.empty(), "1 1", DAY, Optional.of(HOUR));
        testDayTimeIntervalRoundTrip("17520", HOUR, Optional.empty(), "730", DAY, Optional.empty());

        testDayTimeIntervalRoundTrip("0", DAY, Optional.empty(), "0", SECOND, Optional.empty());
        testDayTimeIntervalRoundTrip("340", DAY, Optional.empty());
        testDayTimeIntervalRoundTrip("2 6", DAY, Optional.of(HOUR));
        testDayTimeIntervalRoundTrip("3 0:30", DAY, Optional.of(MINUTE));
        testDayTimeIntervalRoundTrip("3 12:30", DAY, Optional.of(MINUTE));
        testDayTimeIntervalRoundTrip("1 0:00:15", DAY, Optional.of(SECOND));
        testDayTimeIntervalRoundTrip("1 4:20:15", DAY, Optional.of(SECOND));
        testDayTimeIntervalRoundTrip("1 0:00:00.999", DAY, Optional.of(SECOND));
        testDayTimeIntervalRoundTrip("1 23:59:59.999", DAY, Optional.of(SECOND));
    }

    private static void testDayTimeIntervalRoundTrip(String value, IntervalField start, Optional<IntervalField> end)
    {
        testDayTimeIntervalRoundTrip(value, start, end, value, start, end);
    }

    private static void testDayTimeIntervalRoundTrip(
            String inputValue,
            IntervalField inputStart,
            Optional<IntervalField> inputEnd,
            String expectedValue,
            IntervalField expectedStart,
            Optional<IntervalField> expectedEnd)
    {
        long millis = parseDayTimeInterval(inputValue, inputStart, inputEnd);
        assertThat(millis).isGreaterThanOrEqualTo(0);

        IntervalLiteral positiveInterval = formatDayTimeInterval(Duration.ofMillis(millis));
        assertThat(positiveInterval.getSign()).isEqualTo(POSITIVE);
        assertThat(positiveInterval.getStartField()).isEqualTo(expectedStart);
        assertThat(positiveInterval.getEndField()).isEqualTo(expectedEnd);
        assertThat(positiveInterval.getValue()).isEqualTo(expectedValue);

        if (millis != 0) {
            long millisNegative = parseDayTimeInterval("-" + inputValue, inputStart, inputEnd);
            assertThat(millisNegative).isLessThan(0);

            IntervalLiteral negativeInterval = formatDayTimeInterval(Duration.ofMillis(millisNegative));
            assertThat(negativeInterval.getSign()).isEqualTo(NEGATIVE);
            assertThat(negativeInterval.getStartField()).isEqualTo(expectedStart);
            assertThat(negativeInterval.getEndField()).isEqualTo(expectedEnd);
            assertThat(negativeInterval.getValue()).isEqualTo(expectedValue);
        }
    }
}
