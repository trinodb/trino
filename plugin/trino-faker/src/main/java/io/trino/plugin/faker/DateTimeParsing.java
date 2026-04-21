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

import io.trino.spi.type.DateTimeEncoding;
import io.trino.spi.type.LongTimeWithTimeZone;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.trino.spi.type.DateTimeEncoding.packTimeWithTimeZone;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimestampType.MAX_SHORT_PRECISION;
import static java.lang.Math.multiplyExact;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;

final class DateTimeParsing
{
    static final Pattern DATETIME_PATTERN = Pattern.compile("" +
            "(?<year>[-+]?\\d{4,})-(?<month>\\d{1,2})-(?<day>\\d{1,2})" +
            "( (?:(?<hour>\\d{1,2}):(?<minute>\\d{1,2})(?::(?<second>\\d{1,2})(?:\\.(?<fraction>\\d+))?)?)?" +
            "\\s*(?<timezone>.+)?)?");

    static final Pattern TIME_PATTERN = Pattern.compile("" +
            "(?<hour>\\d{1,2}):(?<minute>\\d{1,2})(?::(?<second>\\d{1,2})(?:\\.(?<fraction>\\d+))?)?" +
            "\\s*((?<sign>[+-])(?<offsetHour>\\d\\d):(?<offsetMinute>\\d\\d))?");

    private static final long[] POWERS_OF_TEN = {
            1L,
            10L,
            100L,
            1000L,
            10_000L,
            100_000L,
            1_000_000L,
            10_000_000L,
            100_000_000L,
            1_000_000_000L,
            10_000_000_000L,
            100_000_000_000L,
            1000_000_000_000L,
    };

    private static final int MICROSECONDS_PER_SECOND = 1_000_000;
    private static final long NANOSECONDS_PER_SECOND = 1_000_000_000;
    private static final long PICOSECONDS_PER_SECOND = 1_000_000_000_000L;
    private static final int PICOSECONDS_PER_MICROSECOND = 1_000_000;

    private static final DateTimeFormatter DATE_FORMATTER = new DateTimeFormatterBuilder()
            .appendValue(ChronoField.YEAR, 4, 10, SignStyle.NORMAL)
            .appendLiteral('-')
            .appendValue(ChronoField.MONTH_OF_YEAR, 2)
            .appendLiteral('-')
            .appendValue(ChronoField.DAY_OF_MONTH, 2)
            .toFormatter()
            .withResolverStyle(ResolverStyle.STRICT);

    private DateTimeParsing() {}

    static int parseDate(String value)
    {
        return toIntExact(LocalDate.parse(value, DATE_FORMATTER).toEpochDay());
    }

    static long parseDayTimeInterval(String value)
    {
        boolean negative = value.startsWith("-");
        if (negative) {
            value = value.substring(1);
        }

        int dotIndex = value.indexOf('.');
        long seconds;
        long millis;
        if (dotIndex >= 0) {
            seconds = Long.parseLong(value.substring(0, dotIndex));
            String fraction = value.substring(dotIndex + 1);
            if (fraction.length() > 3) {
                fraction = fraction.substring(0, 3);
            }
            while (fraction.length() < 3) {
                fraction = fraction + "0";
            }
            millis = Long.parseLong(fraction);
        }
        else {
            seconds = Long.parseLong(value);
            millis = 0;
        }

        long result = seconds * 1000 + millis;
        return negative ? -result : result;
    }

    static long parseYearMonthInterval(String value)
    {
        return Long.parseLong(value);
    }

    static Object parseTimestamp(int precision, String value)
    {
        if (precision <= MAX_SHORT_PRECISION) {
            return parseShortTimestamp(value);
        }
        return parseLongTimestamp(value);
    }

    static Object parseTimestampWithTimeZone(int precision, String value)
    {
        if (precision <= TimestampWithTimeZoneType.MAX_SHORT_PRECISION) {
            return parseShortTimestampWithTimeZone(value);
        }
        return parseLongTimestampWithTimeZone(value);
    }

    static long parseTime(String value)
    {
        Matcher matcher = TIME_PATTERN.matcher(value);
        if (!matcher.matches() || matcher.group("offsetHour") != null || matcher.group("offsetMinute") != null) {
            throw new IllegalArgumentException("Invalid TIME: " + value);
        }

        int hour = Integer.parseInt(matcher.group("hour"));
        int minute = Integer.parseInt(matcher.group("minute"));
        int second = matcher.group("second") == null ? 0 : Integer.parseInt(matcher.group("second"));

        if (hour > 23 || minute > 59 || second > 59) {
            throw new IllegalArgumentException("Invalid TIME: " + value);
        }

        int precision = 0;
        String fraction = matcher.group("fraction");
        long fractionValue = 0;
        if (fraction != null) {
            precision = fraction.length();
            fractionValue = Long.parseLong(fraction);
        }

        if (precision > TimeType.MAX_PRECISION) {
            throw new IllegalArgumentException("Invalid TIME: " + value);
        }

        return (((hour * 60L) + minute) * 60 + second) * PICOSECONDS_PER_SECOND + rescale(fractionValue, precision, 12);
    }

    static Object parseTimeWithTimeZone(int precision, String value)
    {
        if (precision <= TimeWithTimeZoneType.MAX_SHORT_PRECISION) {
            return parseShortTimeWithTimeZone(value);
        }
        return parseLongTimeWithTimeZone(value);
    }

    private static long parseShortTimestamp(String value)
    {
        Matcher matcher = DATETIME_PATTERN.matcher(value);
        if (!matcher.matches() || matcher.group("timezone") != null) {
            throw new IllegalArgumentException("Invalid TIMESTAMP: " + value);
        }

        String year = matcher.group("year");
        String month = matcher.group("month");
        String day = matcher.group("day");
        String hour = matcher.group("hour");
        String minute = matcher.group("minute");
        String second = matcher.group("second");
        String fraction = matcher.group("fraction");

        long epochSecond = toEpochSecond(year, month, day, hour, minute, second, UTC);

        int precision = 0;
        long fractionValue = 0;
        if (fraction != null) {
            precision = fraction.length();
            fractionValue = Long.parseLong(fraction);
        }

        if (precision > MAX_SHORT_PRECISION) {
            throw new IllegalArgumentException(format("Cannot parse '%s' as short timestamp. Max allowed precision = %s", value, MAX_SHORT_PRECISION));
        }

        return multiplyExact(epochSecond, MICROSECONDS_PER_SECOND) + rescale(fractionValue, precision, 6);
    }

    private static LongTimestamp parseLongTimestamp(String value)
    {
        Matcher matcher = DATETIME_PATTERN.matcher(value);
        if (!matcher.matches() || matcher.group("timezone") != null) {
            throw new IllegalArgumentException("Invalid TIMESTAMP: " + value);
        }

        String year = matcher.group("year");
        String month = matcher.group("month");
        String day = matcher.group("day");
        String hour = matcher.group("hour");
        String minute = matcher.group("minute");
        String second = matcher.group("second");
        String fraction = matcher.group("fraction");

        if (fraction == null || fraction.length() <= MAX_SHORT_PRECISION) {
            throw new IllegalArgumentException(format("Cannot parse '%s' as long timestamp. Precision must be in the range [%s, %s]", value, MAX_SHORT_PRECISION + 1, TimestampType.MAX_PRECISION));
        }

        int precision = fraction.length();
        long epochSecond = toEpochSecond(year, month, day, hour, minute, second, UTC);
        long picoFraction = rescale(Long.parseLong(fraction), precision, 12);

        return longTimestamp(epochSecond, picoFraction);
    }

    private static long parseShortTimestampWithTimeZone(String value)
    {
        Matcher matcher = DATETIME_PATTERN.matcher(value);
        if (!matcher.matches() || matcher.group("timezone") == null) {
            throw new IllegalArgumentException("Invalid TIMESTAMP WITH TIME ZONE: " + value);
        }

        String year = matcher.group("year");
        String month = matcher.group("month");
        String day = matcher.group("day");
        String hour = matcher.group("hour");
        String minute = matcher.group("minute");
        String second = matcher.group("second");
        String fraction = matcher.group("fraction");
        String timezone = matcher.group("timezone");

        ZoneId zoneId = ZoneId.of(timezone);
        long epochSecond = toEpochSecond(year, month, day, hour, minute, second, zoneId);

        int precision = 0;
        long fractionValue = 0;
        if (fraction != null) {
            precision = fraction.length();
            fractionValue = Long.parseLong(fraction);
        }

        if (precision > MAX_SHORT_PRECISION) {
            throw new IllegalArgumentException(format("Cannot parse '%s' as short timestamp. Max allowed precision = %s", value, MAX_SHORT_PRECISION));
        }

        long epochMillis = epochSecond * 1000 + rescale(fractionValue, precision, 3);
        return DateTimeEncoding.packDateTimeWithZone(epochMillis, timezone);
    }

    private static LongTimestampWithTimeZone parseLongTimestampWithTimeZone(String value)
    {
        Matcher matcher = DATETIME_PATTERN.matcher(value);
        if (!matcher.matches() || matcher.group("timezone") == null) {
            throw new IllegalArgumentException("Invalid TIMESTAMP: " + value);
        }

        String year = matcher.group("year");
        String month = matcher.group("month");
        String day = matcher.group("day");
        String hour = matcher.group("hour");
        String minute = matcher.group("minute");
        String second = matcher.group("second");
        String fraction = matcher.group("fraction");
        String timezone = matcher.group("timezone");

        if (fraction == null || fraction.length() <= TimestampWithTimeZoneType.MAX_SHORT_PRECISION) {
            throw new IllegalArgumentException(format("Cannot parse '%s' as long timestamp. Precision must be in the range [%s, %s]", value, TimestampWithTimeZoneType.MAX_SHORT_PRECISION + 1, TimestampWithTimeZoneType.MAX_PRECISION));
        }

        ZoneId zoneId = ZoneId.of(timezone);
        long epochSecond = toEpochSecond(year, month, day, hour, minute, second, zoneId);

        return LongTimestampWithTimeZone.fromEpochSecondsAndFraction(epochSecond, rescale(Long.parseLong(fraction), fraction.length(), 12), getTimeZoneKey(timezone));
    }

    private static long parseShortTimeWithTimeZone(String value)
    {
        Matcher matcher = TIME_PATTERN.matcher(value);
        if (!matcher.matches() || matcher.group("offsetHour") == null || matcher.group("offsetMinute") == null) {
            throw new IllegalArgumentException("Invalid TIME WITH TIME ZONE: " + value);
        }

        int hour = Integer.parseInt(matcher.group("hour"));
        int minute = Integer.parseInt(matcher.group("minute"));
        int second = matcher.group("second") == null ? 0 : Integer.parseInt(matcher.group("second"));
        int offsetSign = matcher.group("sign").equals("+") ? 1 : -1;
        int offsetHour = Integer.parseInt(matcher.group("offsetHour"));
        int offsetMinute = Integer.parseInt(matcher.group("offsetMinute"));

        if (hour > 23 || minute > 59 || second > 59 || !isValidOffset(offsetHour, offsetMinute)) {
            throw new IllegalArgumentException("Invalid TIME WITH TIME ZONE: " + value);
        }

        int precision = 0;
        String fraction = matcher.group("fraction");
        long fractionValue = 0;
        if (fraction != null) {
            precision = fraction.length();
            fractionValue = Long.parseLong(fraction);
        }

        long nanos = (((hour * 60L) + minute) * 60 + second) * NANOSECONDS_PER_SECOND + rescale(fractionValue, precision, 9);
        return packTimeWithTimeZone(nanos, (offsetSign * (offsetHour * 60 + offsetMinute)));
    }

    private static LongTimeWithTimeZone parseLongTimeWithTimeZone(String value)
    {
        Matcher matcher = TIME_PATTERN.matcher(value);
        if (!matcher.matches() || matcher.group("offsetHour") == null || matcher.group("offsetMinute") == null) {
            throw new IllegalArgumentException("Invalid TIME WITH TIME ZONE: " + value);
        }

        int hour = Integer.parseInt(matcher.group("hour"));
        int minute = Integer.parseInt(matcher.group("minute"));
        int second = matcher.group("second") == null ? 0 : Integer.parseInt(matcher.group("second"));
        int offsetSign = matcher.group("sign").equals("+") ? 1 : -1;
        int offsetHour = Integer.parseInt(matcher.group("offsetHour"));
        int offsetMinute = Integer.parseInt(matcher.group("offsetMinute"));

        if (hour > 23 || minute > 59 || second > 59 || !isValidOffset(offsetHour, offsetMinute)) {
            throw new IllegalArgumentException("Invalid TIME WITH TIME ZONE: " + value);
        }

        int precision = 0;
        String fraction = matcher.group("fraction");
        long fractionValue = 0;
        if (fraction != null) {
            precision = fraction.length();
            fractionValue = Long.parseLong(fraction);
        }

        long picos = (((hour * 60) + minute) * 60 + second) * PICOSECONDS_PER_SECOND + rescale(fractionValue, precision, 12);
        return new LongTimeWithTimeZone(picos, (offsetSign * (offsetHour * 60 + offsetMinute)));
    }

    private static long toEpochSecond(String year, String month, String day, String hour, String minute, String second, ZoneId zoneId)
    {
        LocalDateTime timestamp = LocalDateTime.of(
                Integer.parseInt(year),
                Integer.parseInt(month),
                Integer.parseInt(day),
                hour == null ? 0 : Integer.parseInt(hour),
                minute == null ? 0 : Integer.parseInt(minute),
                second == null ? 0 : Integer.parseInt(second),
                0);

        List<ZoneOffset> offsets = zoneId.getRules().getValidOffsets(timestamp);
        if (offsets.isEmpty()) {
            throw new IllegalArgumentException("Invalid TIMESTAMP due to daylight savings transition");
        }

        return timestamp.toEpochSecond(offsets.get(0));
    }

    private static LongTimestamp longTimestamp(long epochSecond, long fractionInPicos)
    {
        return new LongTimestamp(
                multiplyExact(epochSecond, MICROSECONDS_PER_SECOND) + fractionInPicos / PICOSECONDS_PER_MICROSECOND,
                (int) (fractionInPicos % PICOSECONDS_PER_MICROSECOND));
    }

    private static long rescale(long value, int fromPrecision, int toPrecision)
    {
        if (fromPrecision <= toPrecision) {
            value *= scaleFactor(fromPrecision, toPrecision);
        }
        else {
            value /= scaleFactor(toPrecision, fromPrecision);
        }
        return value;
    }

    private static long scaleFactor(int fromPrecision, int toPrecision)
    {
        if (fromPrecision > toPrecision) {
            throw new IllegalArgumentException("fromPrecision must be <= toPrecision");
        }
        return POWERS_OF_TEN[toPrecision - fromPrecision];
    }

    private static boolean isValidOffset(int hour, int minute)
    {
        return (hour == 14 && minute == 0) ||
                (hour >= 0 && hour < 14 && minute >= 0 && minute <= 59);
    }
}
