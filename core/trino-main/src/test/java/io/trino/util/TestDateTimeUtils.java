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

import java.time.DateTimeException;
import java.time.Duration;
import java.time.LocalDate;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.StandardErrorCode.INVALID_LITERAL;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.type.IntervalField.DAY;
import static io.trino.spi.type.IntervalField.HOUR;
import static io.trino.spi.type.IntervalField.MINUTE;
import static io.trino.spi.type.IntervalField.MONTH;
import static io.trino.spi.type.IntervalField.SECOND;
import static io.trino.spi.type.IntervalField.YEAR;
import static io.trino.sql.tree.IntervalLiteral.Sign.NEGATIVE;
import static io.trino.sql.tree.IntervalLiteral.Sign.POSITIVE;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.util.DateTimeUtils.formatDayTimeInterval;
import static io.trino.util.DateTimeUtils.parseDate;
import static io.trino.util.DateTimeUtils.parseDayTimeIntervalToPicos;
import static io.trino.util.DateTimeUtils.parseIfIso8601DateFormat;
import static io.trino.util.DateTimeUtils.parseYearMonthInterval;
import static io.trino.util.DateTimeUtils.printDate;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDateTimeUtils
{
    @Test
    public void testParseDateSlice()
    {
        assertThat(parseIfIso8601DateFormat(utf8Slice("prefix2000-02-29suffix").slice(6, 10)))
                .hasValue(11016);
        assertThat(parseIfIso8601DateFormat(utf8Slice("0000-02-29")))
                .hasValue(-719469);
        for (String value : new String[] {"1900-02-29", "2000-00-01", "2000-01-00", "2000-04-31"}) {
            assertThatThrownBy(() -> parseIfIso8601DateFormat(utf8Slice("prefix" + value).slice(6, 10)))
                    .isInstanceOf(DateTimeException.class)
                    .hasMessage("Invalid date '%s'", value);
        }
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
                .hasMessage("Invalid date '2022-02-29'");
        assertThatThrownBy(() -> parseIfIso8601DateFormat(utf8Slice("1970-32-01")))
                .isInstanceOf(DateTimeException.class)
                .hasMessage("Invalid date '1970-32-01'");
        assertThatThrownBy(() -> parseIfIso8601DateFormat(utf8Slice("1970-02-41")))
                .isInstanceOf(DateTimeException.class)
                .hasMessage("Invalid date '1970-02-41'");
    }

    @Test
    public void testLegacyDateSyntax()
    {
        assertThat(parseDate(utf8Slice("1-1-1"))).isEqualTo(LocalDate.of(1, 1, 1).toEpochDay());
        assertThat(parseDate(utf8Slice("+2013-2-02"))).isEqualTo(LocalDate.of(2013, 2, 2).toEpochDay());
        assertThat(parseDate(utf8Slice("02013-02-2"))).isEqualTo(LocalDate.of(2013, 2, 2).toEpochDay());
        assertThat(parseDate(utf8Slice("-2013-2-2"))).isEqualTo(LocalDate.of(-2013, 2, 2).toEpochDay());
        assertThat(parseDate(utf8Slice("0000-2-29"))).isEqualTo(LocalDate.of(0, 2, 29).toEpochDay());
        for (String value : new String[] {"2013-002-02", "2013-02-002", "2013-+2-2", "1000000000-1-1", "2013-2-2 "}) {
            assertThatThrownBy(() -> parseDate(utf8Slice(value)))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> parseDate(utf8Slice("1900-2-29")))
                .isInstanceOf(DateTimeException.class);
    }

    @Test
    public void testExtendedDatePrinting()
    {
        assertThat(printDate((int) LocalDate.of(-1, 1, 1).toEpochDay()).toStringUtf8()).isEqualTo("-0001-01-01");
        assertThat(printDate((int) LocalDate.of(0, 1, 1).toEpochDay()).toStringUtf8()).isEqualTo("0000-01-01");
        assertThat(printDate((int) LocalDate.of(10_000, 1, 1).toEpochDay()).toStringUtf8()).isEqualTo("10000-01-01");
        for (int days : new int[] {Integer.MIN_VALUE, Integer.MAX_VALUE, -719_529, 2_932_897}) {
            assertThat(parseDate(printDate(days))).isEqualTo(days);
        }
    }

    @Test
    public void testDayTimeIntervalFields()
    {
        assertThat(parseDayTimeIntervalToPicos("1 2:3:4.123456789012", DAY, SECOND, 12))
                .containsExactly(93_784_123_456L, 789_012);
        assertThat(parseDayTimeIntervalToPicos("-1 2:3:4.123456789012", DAY, SECOND, 12))
                .containsExactly(-93_784_123_457L, 210_988);
        assertThat(parseDayTimeIntervalToPicos("1 2", DAY, SECOND, 12))
                .containsExactly(93_600_000_000L, 0);
        assertThat(parseDayTimeIntervalToPicos("1 2:3:", DAY, SECOND, 12))
                .containsExactly(93_780_000_000L, 0);
        assertThat(parseDayTimeIntervalToPicos("1 ", DAY, HOUR, 12))
                .containsExactly(86_400_000_000L, 0);
        assertThat(parseDayTimeIntervalToPicos("1:+2", HOUR, MINUTE, 12))
                .containsExactly(3_720_000_000L, 0);
        assertThat(parseDayTimeIntervalToPicos("1:-0", HOUR, MINUTE, 12))
                .containsExactly(3_600_000_000L, 0);
        // The legacy seconds parser accepted comma fractions without adding them to the result.
        assertThat(parseDayTimeIntervalToPicos("1 2:3:4,5", DAY, SECOND, 12))
                .containsExactly(93_784_000_000L, 0);
        assertThat(parseDayTimeIntervalToPicos("1:+00000000001", MINUTE, SECOND, 12))
                .containsExactly(61_000_000L, 0);
        for (String value : new String[] {"1  2", "1 2:-3", "1 2:3:4:5", "1 2:3:4.1x", "1 2.3", "1 2:3:+-4", "1 2:2147483648"}) {
            assertTrinoExceptionThrownBy(() -> parseDayTimeIntervalToPicos(value, DAY, SECOND, 12))
                    .hasErrorCode(INVALID_LITERAL);
        }
    }

    @Test
    public void testIntervalRoundingAndBounds()
    {
        assertThat(parseDayTimeIntervalToPicos("0.0000005", SECOND, SECOND, 6)).containsExactly(1, 0);
        assertThat(parseDayTimeIntervalToPicos("-0.0000005", SECOND, SECOND, 6)).containsExactly(0, 0);
        assertThat(parseDayTimeIntervalToPicos("1.000000000001", SECOND, SECOND, 12, -1))
                .containsExactly(-1_000_001, 999_999);
        assertThat(parseDayTimeIntervalToPicos("9223372036854.775807", SECOND, SECOND, 6))
                .containsExactly(Long.MAX_VALUE, 0);
        assertThat(parseDayTimeIntervalToPicos("-9223372036854.775808", SECOND, SECOND, 6))
                .containsExactly(Long.MIN_VALUE, 0);
        assertTrinoExceptionThrownBy(() -> parseDayTimeIntervalToPicos("9223372036854775808", SECOND, SECOND, 12))
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
        assertTrinoExceptionThrownBy(() -> parseDayTimeIntervalToPicos("1:2147483648", MINUTE, SECOND, 12))
                .hasErrorCode(INVALID_LITERAL);
        assertTrinoExceptionThrownBy(() -> parseDayTimeIntervalToPicos("9223372036854775808:bad", MINUTE, SECOND, 12))
                .hasErrorCode(INVALID_LITERAL);
    }

    @Test
    public void testYearMonthIntervalFields()
    {
        assertThat(parseYearMonthInterval("124-30", YEAR, MONTH)).isEqualTo(1_518);
        assertThat(parseYearMonthInterval("124", YEAR, MONTH)).isEqualTo(1_488);
        assertThat(parseYearMonthInterval("1-", YEAR, MONTH)).isEqualTo(12);
        assertThat(parseYearMonthInterval("1-+00000000001", YEAR, MONTH)).isEqualTo(13);
        assertThat(parseYearMonthInterval("", YEAR, MONTH)).isZero();
        assertThat(parseYearMonthInterval("-178956970-8", YEAR, MONTH)).isEqualTo(Integer.MIN_VALUE);
        assertThat(parseYearMonthInterval("-2147483648", MONTH, MONTH)).isEqualTo(Integer.MIN_VALUE);
        assertTrinoExceptionThrownBy(() -> parseYearMonthInterval("178956970-8", YEAR, MONTH))
                .hasErrorCode(INVALID_LITERAL);
        assertTrinoExceptionThrownBy(() -> parseYearMonthInterval("124--30", YEAR, MONTH))
                .hasErrorCode(INVALID_LITERAL);
    }

    @Test
    public void testFormatDayTimeInterval()
    {
        assertThat(formatDayTimeInterval(Duration.ZERO).getValue()).isEqualTo("0 0:00:00");
        assertThat(formatDayTimeInterval(Duration.ZERO).getSign()).isEqualTo(POSITIVE);
        assertThat(formatDayTimeInterval(Duration.ofMillis(1)).getValue()).isEqualTo("0 0:00:00.001");
        assertThat(formatDayTimeInterval(Duration.ofMillis(-90_061_001)).getValue()).isEqualTo("1 1:01:01.001");
        assertThat(formatDayTimeInterval(Duration.ofMillis(-90_061_001)).getSign()).isEqualTo(NEGATIVE);
    }
}
