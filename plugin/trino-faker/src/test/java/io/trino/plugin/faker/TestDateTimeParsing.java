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
package io.trino.plugin.faker;

import io.trino.spi.type.LongTimeWithTimeZone;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import org.junit.jupiter.api.Test;

import java.time.DateTimeException;
import java.time.format.DateTimeParseException;

import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackOffsetMinutes;
import static io.trino.spi.type.DateTimeEncoding.unpackTimeNanos;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestDateTimeParsing
{
    @Test
    void testParseDate()
    {
        // valid dates
        assertThat(DateTimeParsing.parseDate("1970-01-01")).isEqualTo(0);
        assertThat(DateTimeParsing.parseDate("1970-02-01")).isEqualTo(31);
        assertThat(DateTimeParsing.parseDate("1969-12-01")).isEqualTo(-31);
        assertThat(DateTimeParsing.parseDate("2022-02-28")).isEqualTo(19051);
        assertThat(DateTimeParsing.parseDate("0000-01-01")).isEqualTo(-719528);
        assertThat(DateTimeParsing.parseDate("9999-12-31")).isEqualTo(2932896);
        // extended year format
        assertThat(DateTimeParsing.parseDate("5881580-07-11")).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    void testParseDateInvalidFormat()
    {
        // invalid length
        assertThatThrownBy(() -> DateTimeParsing.parseDate("1970-2-01"))
                .isInstanceOf(DateTimeParseException.class);
        // invalid characters
        assertThatThrownBy(() -> DateTimeParsing.parseDate("a970-02-10"))
                .isInstanceOf(DateTimeParseException.class);
        assertThatThrownBy(() -> DateTimeParsing.parseDate("1p70-02-10"))
                .isInstanceOf(DateTimeParseException.class);
        assertThatThrownBy(() -> DateTimeParsing.parseDate("19%0-02-10"))
                .isInstanceOf(DateTimeParseException.class);
        // wrong separators
        assertThatThrownBy(() -> DateTimeParsing.parseDate("1970_02-01"))
                .isInstanceOf(DateTimeParseException.class);
        assertThatThrownBy(() -> DateTimeParsing.parseDate("1970/02/01"))
                .isInstanceOf(DateTimeParseException.class);
        assertThatThrownBy(() -> DateTimeParsing.parseDate("1970-02/01"))
                .isInstanceOf(DateTimeParseException.class);
        // completely wrong format
        assertThatThrownBy(() -> DateTimeParsing.parseDate("Dec 24 2022"))
                .isInstanceOf(DateTimeParseException.class);
    }

    @Test
    void testParseDateInvalidValue()
    {
        assertThatThrownBy(() -> DateTimeParsing.parseDate("2022-02-29"))
                .isInstanceOf(DateTimeException.class);
        assertThatThrownBy(() -> DateTimeParsing.parseDate("1970-32-01"))
                .isInstanceOf(DateTimeException.class);
        assertThatThrownBy(() -> DateTimeParsing.parseDate("1970-02-41"))
                .isInstanceOf(DateTimeException.class);
    }

    @Test
    void testParseDayTimeInterval()
    {
        // whole seconds
        assertThat(DateTimeParsing.parseDayTimeInterval("0")).isEqualTo(0);
        assertThat(DateTimeParsing.parseDayTimeInterval("1")).isEqualTo(1000);
        assertThat(DateTimeParsing.parseDayTimeInterval("60")).isEqualTo(60_000);

        // fractional seconds
        assertThat(DateTimeParsing.parseDayTimeInterval("1.5")).isEqualTo(1500);
        assertThat(DateTimeParsing.parseDayTimeInterval("123.456")).isEqualTo(123456);
        assertThat(DateTimeParsing.parseDayTimeInterval("0.001")).isEqualTo(1);
        assertThat(DateTimeParsing.parseDayTimeInterval("0.1")).isEqualTo(100);
        assertThat(DateTimeParsing.parseDayTimeInterval("0.12")).isEqualTo(120);

        // fraction truncated to 3 digits (millis)
        assertThat(DateTimeParsing.parseDayTimeInterval("1.1234")).isEqualTo(1123);
        assertThat(DateTimeParsing.parseDayTimeInterval("1.9999")).isEqualTo(1999);

        // negative values
        assertThat(DateTimeParsing.parseDayTimeInterval("-1")).isEqualTo(-1000);
        assertThat(DateTimeParsing.parseDayTimeInterval("-1.5")).isEqualTo(-1500);
        assertThat(DateTimeParsing.parseDayTimeInterval("-0")).isEqualTo(0);
    }

    @Test
    void testParseYearMonthInterval()
    {
        assertThat(DateTimeParsing.parseYearMonthInterval("0")).isEqualTo(0);
        assertThat(DateTimeParsing.parseYearMonthInterval("1")).isEqualTo(1);
        assertThat(DateTimeParsing.parseYearMonthInterval("12")).isEqualTo(12);
        assertThat(DateTimeParsing.parseYearMonthInterval("-3")).isEqualTo(-3);
        assertThat(DateTimeParsing.parseYearMonthInterval("-12")).isEqualTo(-12);

        assertThatThrownBy(() -> DateTimeParsing.parseYearMonthInterval("abc"))
                .isInstanceOf(NumberFormatException.class);
    }

    @Test
    void testParseTime()
    {
        // midnight
        assertThat(DateTimeParsing.parseTime("00:00:00")).isEqualTo(0);
        // hour and minute only
        assertThat(DateTimeParsing.parseTime("12:34")).isEqualTo(12 * 3600 * 1_000_000_000_000L + 34 * 60 * 1_000_000_000_000L);
        // full time
        assertThat(DateTimeParsing.parseTime("12:34:56")).isEqualTo(
                (12 * 3600L + 34 * 60 + 56) * 1_000_000_000_000L);
        // end of day
        assertThat(DateTimeParsing.parseTime("23:59:59")).isEqualTo(
                (23 * 3600L + 59 * 60 + 59) * 1_000_000_000_000L);
        // with fractional seconds
        assertThat(DateTimeParsing.parseTime("12:34:56.1")).isEqualTo(
                (12 * 3600L + 34 * 60 + 56) * 1_000_000_000_000L + 100_000_000_000L);
        assertThat(DateTimeParsing.parseTime("12:34:56.123")).isEqualTo(
                (12 * 3600L + 34 * 60 + 56) * 1_000_000_000_000L + 123_000_000_000L);
        assertThat(DateTimeParsing.parseTime("12:34:56.123456789012")).isEqualTo(
                (12 * 3600L + 34 * 60 + 56) * 1_000_000_000_000L + 123456789012L);
    }

    @Test
    void testParseTimeInvalid()
    {
        // hour out of range
        assertThatThrownBy(() -> DateTimeParsing.parseTime("25:00:00"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid TIME");
        // minute out of range
        assertThatThrownBy(() -> DateTimeParsing.parseTime("12:60:00"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid TIME");
        // second out of range
        assertThatThrownBy(() -> DateTimeParsing.parseTime("12:00:60"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid TIME");
        // precision too high (max is 12)
        assertThatThrownBy(() -> DateTimeParsing.parseTime("12:00:00.1234567890123"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid TIME");
        // time with timezone offset should be rejected
        assertThatThrownBy(() -> DateTimeParsing.parseTime("12:34:56+05:30"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid TIME");
        // not a time
        assertThatThrownBy(() -> DateTimeParsing.parseTime("not-a-time"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid TIME");
    }

    @Test
    void testParseTimeWithTimeZoneShort()
    {
        // short precision (0-3) returns packed long
        Object result = DateTimeParsing.parseTimeWithTimeZone(3, "12:34:56+05:30");
        assertThat(result).isInstanceOf(Long.class);
        long packed = (long) result;
        assertThat(unpackTimeNanos(packed)).isEqualTo((12 * 3600L + 34 * 60 + 56) * 1_000_000_000L);
        assertThat(unpackOffsetMinutes(packed)).isEqualTo(5 * 60 + 30);

        // negative offset
        Object negativeOffset = DateTimeParsing.parseTimeWithTimeZone(0, "08:00:00-03:00");
        assertThat(negativeOffset).isInstanceOf(Long.class);
        assertThat(unpackOffsetMinutes((long) negativeOffset)).isEqualTo(-180);

        // UTC offset
        Object utcOffset = DateTimeParsing.parseTimeWithTimeZone(0, "00:00:00+00:00");
        assertThat(utcOffset).isInstanceOf(Long.class);
        assertThat(unpackTimeNanos((long) utcOffset)).isEqualTo(0);
        assertThat(unpackOffsetMinutes((long) utcOffset)).isEqualTo(0);
    }

    @Test
    void testParseTimeWithTimeZoneLong()
    {
        // long precision (>9) returns LongTimeWithTimeZone
        Object result = DateTimeParsing.parseTimeWithTimeZone(10, "12:34:56.1234567890+05:30");
        assertThat(result).isInstanceOf(LongTimeWithTimeZone.class);
        LongTimeWithTimeZone longTime = (LongTimeWithTimeZone) result;
        assertThat(longTime.getOffsetMinutes()).isEqualTo(5 * 60 + 30);
    }

    @Test
    void testParseTimeWithTimeZoneInvalid()
    {
        // missing timezone offset
        assertThatThrownBy(() -> DateTimeParsing.parseTimeWithTimeZone(3, "12:34:56"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid TIME WITH TIME ZONE");
        // invalid offset (>14:00)
        assertThatThrownBy(() -> DateTimeParsing.parseTimeWithTimeZone(3, "12:34:56+15:00"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid TIME WITH TIME ZONE");
        // max valid offset is +14:00
        assertThat(DateTimeParsing.parseTimeWithTimeZone(3, "12:34:56+14:00"))
                .isInstanceOf(Long.class);
    }

    @Test
    void testParseTimestampShort()
    {
        // precision 0 - no fractional seconds
        Object result = DateTimeParsing.parseTimestamp(0, "2020-01-01 12:34:56");
        assertThat(result).isInstanceOf(Long.class);
        // epoch micros for 2020-01-01 12:34:56 UTC
        assertThat((long) result).isEqualTo(1577882096000000L);

        // precision 3
        result = DateTimeParsing.parseTimestamp(3, "2020-01-01 12:34:56.789");
        assertThat(result).isInstanceOf(Long.class);
        assertThat((long) result).isEqualTo(1577882096789000L);

        // precision 6
        result = DateTimeParsing.parseTimestamp(6, "2020-01-01 12:34:56.123456");
        assertThat(result).isInstanceOf(Long.class);
        assertThat((long) result).isEqualTo(1577882096123456L);

        // date only (no time component)
        result = DateTimeParsing.parseTimestamp(0, "2020-01-01");
        assertThat(result).isInstanceOf(Long.class);
        assertThat((long) result).isEqualTo(1577836800000000L);
    }

    @Test
    void testParseTimestampLong()
    {
        // precision 7+
        Object result = DateTimeParsing.parseTimestamp(9, "2020-01-01 12:34:56.123456789");
        assertThat(result).isInstanceOf(LongTimestamp.class);
        LongTimestamp longTs = (LongTimestamp) result;
        assertThat(longTs.getEpochMicros()).isEqualTo(1577882096123456L);
        assertThat(longTs.getPicosOfMicro()).isEqualTo(789000);

        // precision 12
        result = DateTimeParsing.parseTimestamp(12, "2020-01-01 12:34:56.123456789012");
        assertThat(result).isInstanceOf(LongTimestamp.class);
        longTs = (LongTimestamp) result;
        assertThat(longTs.getEpochMicros()).isEqualTo(1577882096123456L);
        assertThat(longTs.getPicosOfMicro()).isEqualTo(789012);
    }

    @Test
    void testParseTimestampInvalid()
    {
        // timestamp with timezone should be rejected
        assertThatThrownBy(() -> DateTimeParsing.parseTimestamp(3, "2020-01-01 12:34:56.789 UTC"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid TIMESTAMP");
        // short precision with too many fractional digits
        assertThatThrownBy(() -> DateTimeParsing.parseTimestamp(3, "2020-01-01 12:34:56.1234567"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("short timestamp");
        // long precision with too few fractional digits
        assertThatThrownBy(() -> DateTimeParsing.parseTimestamp(9, "2020-01-01 12:34:56.123"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("long timestamp");
        // completely invalid
        assertThatThrownBy(() -> DateTimeParsing.parseTimestamp(3, "not-a-timestamp"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid TIMESTAMP");
    }

    @Test
    void testParseTimestampDstTransition()
    {
        // DST gap - this timestamp doesn't exist in Europe/Warsaw but parseTimestamp uses UTC
        // so it should succeed
        assertThat(DateTimeParsing.parseTimestamp(0, "2020-03-29 02:30:00"))
                .isInstanceOf(Long.class);
    }

    @Test
    void testParseTimestampWithTimeZoneShort()
    {
        Object result = DateTimeParsing.parseTimestampWithTimeZone(3, "2020-01-01 12:34:56.789 UTC");
        assertThat(result).isInstanceOf(Long.class);
        long packed = (long) result;
        assertThat(unpackMillisUtc(packed)).isEqualTo(1577882096789L);
        assertThat(unpackZoneKey(packed).getId()).isEqualTo("UTC");

        // named timezone
        result = DateTimeParsing.parseTimestampWithTimeZone(3, "2020-01-01 12:34:56.789 America/New_York");
        assertThat(result).isInstanceOf(Long.class);
        packed = (long) result;
        assertThat(unpackZoneKey(packed).getId()).isEqualTo("America/New_York");
        // New York is UTC-5 in January, so epoch millis should be 5 hours later than UTC
        assertThat(unpackMillisUtc(packed)).isEqualTo(1577900096789L);
    }

    @Test
    void testParseTimestampWithTimeZoneLong()
    {
        Object result = DateTimeParsing.parseTimestampWithTimeZone(9, "2020-01-01 12:34:56.123456789 America/New_York");
        assertThat(result).isInstanceOf(LongTimestampWithTimeZone.class);
        LongTimestampWithTimeZone longTstz = (LongTimestampWithTimeZone) result;
        assertThat(longTstz.getTimeZoneKey()).isEqualTo(getTimeZoneKey("America/New_York").getKey());
    }

    @Test
    void testParseTimestampWithTimeZoneInvalid()
    {
        // missing timezone
        assertThatThrownBy(() -> DateTimeParsing.parseTimestampWithTimeZone(3, "2020-01-01 12:34:56.789"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid TIMESTAMP WITH TIME ZONE");
        // long precision with too few fractional digits
        assertThatThrownBy(() -> DateTimeParsing.parseTimestampWithTimeZone(9, "2020-01-01 12:34:56.789 UTC"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("long timestamp");
        // completely invalid
        assertThatThrownBy(() -> DateTimeParsing.parseTimestampWithTimeZone(3, "not-a-timestamp"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid TIMESTAMP WITH TIME ZONE");
    }

    @Test
    void testParseTimestampWithTimeZoneDstTransition()
    {
        // DST gap in Europe/Warsaw: 2020-03-29 02:00 doesn't exist (clocks jump from 02:00 to 03:00)
        assertThatThrownBy(() -> DateTimeParsing.parseTimestampWithTimeZone(3, "2020-03-29 02:30:00.000 Europe/Warsaw"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("daylight savings");
    }
}
