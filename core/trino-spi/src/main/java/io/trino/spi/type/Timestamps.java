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
package io.trino.spi.type;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static io.trino.spi.type.TimestampType.MAX_PRECISION;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.multiplyExact;
import static java.time.ZoneOffset.UTC;

public class Timestamps
{
    static final long[] POWERS_OF_TEN = {
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

    public static final int MILLISECONDS_PER_SECOND = 1_000;
    public static final int MILLISECONDS_PER_MINUTE = MILLISECONDS_PER_SECOND * 60;
    public static final int MILLISECONDS_PER_HOUR = MILLISECONDS_PER_MINUTE * 60;
    public static final int MILLISECONDS_PER_DAY = MILLISECONDS_PER_HOUR * 24;
    public static final int MICROSECONDS_PER_MILLISECOND = 1_000;
    public static final int MICROSECONDS_PER_SECOND = 1_000_000;
    public static final long MICROSECONDS_PER_DAY = 24 * 60 * 60 * 1_000_000L;
    public static final int NANOSECONDS_PER_MICROSECOND = 1_000;
    public static final int NANOSECONDS_PER_MILLISECOND = 1_000_000;
    public static final long NANOSECONDS_PER_SECOND = 1_000_000_000;
    public static final long NANOSECONDS_PER_MINUTE = NANOSECONDS_PER_SECOND * 60;
    public static final long NANOSECONDS_PER_DAY = NANOSECONDS_PER_MINUTE * 60 * 24;
    public static final int PICOSECONDS_PER_NANOSECOND = 1_000;
    public static final int PICOSECONDS_PER_MICROSECOND = 1_000_000;
    public static final int PICOSECONDS_PER_MILLISECOND = 1_000_000_000;
    public static final long PICOSECONDS_PER_SECOND = 1_000_000_000_000L;
    public static final long PICOSECONDS_PER_MINUTE = PICOSECONDS_PER_SECOND * 60;
    public static final long PICOSECONDS_PER_HOUR = PICOSECONDS_PER_MINUTE * 60;
    public static final long PICOSECONDS_PER_DAY = PICOSECONDS_PER_HOUR * 24;
    public static final long SECONDS_PER_MINUTE = 60;
    public static final long MINUTES_PER_HOUR = 60;
    public static final long SECONDS_PER_DAY = SECONDS_PER_MINUTE * MINUTES_PER_HOUR * 24;

    private Timestamps() {}

    public static long round(long value, int magnitude)
    {
        return multiplyExact(roundDiv(value, POWERS_OF_TEN[magnitude]), POWERS_OF_TEN[magnitude]);
    }

    /**
     * Rescales a value of the given precision to another precision by adding 0s or truncating.
     */
    static long rescale(long value, int fromPrecision, int toPrecision)
    {
        if (value < 0) {
            throw new IllegalArgumentException("value must be >= 0");
        }

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

    @SuppressWarnings("NumericCastThatLosesPrecision")
    public static int roundDiv(int value, long factor)
    {
        return (int) roundDiv((long) value, factor);
    }

    public static long roundDiv(long value, long factor)
    {
        if (factor <= 0) {
            throw new IllegalArgumentException("Factor must be > 0");
        }

        if (factor == 1) {
            return value;
        }

        // round half up, without computing value + factor / 2, which can overflow
        long quotient = floorDiv(value, factor);
        if (value - quotient * factor >= factor - factor / 2) {
            return quotient + 1;
        }
        return quotient;
    }

    public static long epochMicrosToMillisWithRounding(long epochMicros)
    {
        return roundDiv(epochMicros, MICROSECONDS_PER_MILLISECOND);
    }

    /**
     * Formats a timestamp of the given precision. Digits beyond the precision are truncated, not rounded.
     */
    public static String formatTimestamp(int precision, long epochMicros, int picosOfMicro)
    {
        return formatTimestampInZone(precision, epochMicros, picosOfMicro, UTC);
    }

    public static String formatTimestampInZone(int precision, long epochMicros, int picosOfMicro, ZoneId zoneId)
    {
        LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(floorDiv(epochMicros, MICROSECONDS_PER_SECOND)), zoneId);
        long picoFraction = ((long) floorMod(epochMicros, MICROSECONDS_PER_SECOND)) * PICOSECONDS_PER_MICROSECOND + picosOfMicro;
        return formatDateTime(precision, dateTime, picoFraction, null);
    }

    public static String formatTimestampWithTimeZone(int precision, long epochMillis, int picosOfMilli, ZoneId zoneId)
    {
        LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), zoneId);
        long picoFraction = (long) floorMod(epochMillis, MILLISECONDS_PER_SECOND) * PICOSECONDS_PER_MILLISECOND + picosOfMilli;
        return formatDateTime(precision, dateTime, picoFraction, zoneId.getId());
    }

    /**
     * Formats a time of day (HH:mm:ss[.fraction]). Digits beyond the precision are truncated, not rounded.
     */
    public static String formatTime(int precision, long picos)
    {
        char[] buffer = new char[8 + (precision == 0 ? 0 : 1 + precision)];
        int length = writeTime(buffer, 0, precision, picos);
        return new String(buffer, 0, length);
    }

    public static String formatTimeWithTimeZone(int precision, long picos, int offsetMinutes)
    {
        char[] buffer = new char[8 + (precision == 0 ? 0 : 1 + precision) + 6];
        int length = writeTime(buffer, 0, precision, picos);
        buffer[length++] = offsetMinutes >= 0 ? '+' : '-';
        length = writeDigits(buffer, length, Math.abs(offsetMinutes / 60), 2);
        buffer[length++] = ':';
        length = writeDigits(buffer, length, Math.abs(offsetMinutes % 60), 2);
        return new String(buffer, 0, length);
    }

    private static String formatDateTime(int precision, LocalDateTime dateTime, long picoFraction, String zone)
    {
        // Sign, up to nine year digits, -MM-dd, space, HH:mm:ss, fraction, and optional zone.
        char[] buffer = new char[1 + 9 + 3 + 3 + 1 + 8 + (precision == 0 ? 0 : 1 + precision) + (zone == null ? 0 : zone.length() + 1)];
        int length = writeDate(buffer, dateTime.toLocalDate());
        buffer[length++] = ' ';
        long picos = dateTime.toLocalTime().toSecondOfDay() * PICOSECONDS_PER_SECOND + picoFraction;
        length = writeTime(buffer, length, precision, picos);
        if (zone != null) {
            buffer[length++] = ' ';
            zone.getChars(0, zone.length(), buffer, length);
            length += zone.length();
        }
        return new String(buffer, 0, length);
    }

    // Matches the "uuuu-MM-dd" pattern: at least four year digits, with a sign when the year is negative or has more than four digits
    private static int writeDate(char[] buffer, LocalDate date)
    {
        int year = date.getYear();
        int offset = 0;
        if (year < 0) {
            buffer[offset++] = '-';
        }
        else if (year > 9999) {
            buffer[offset++] = '+';
        }
        int yearDigits = 4;
        for (int remaining = Math.abs(year) / 10_000; remaining != 0; remaining /= 10) {
            yearDigits++;
        }
        offset = writeDigits(buffer, offset, Math.abs(year), yearDigits);
        buffer[offset++] = '-';
        offset = writeDigits(buffer, offset, date.getMonthValue(), 2);
        buffer[offset++] = '-';
        return writeDigits(buffer, offset, date.getDayOfMonth(), 2);
    }

    private static int writeTime(char[] buffer, int offset, int precision, long picos)
    {
        offset = writeDigits(buffer, offset, picos / PICOSECONDS_PER_HOUR, 2);
        buffer[offset++] = ':';
        offset = writeDigits(buffer, offset, (picos / PICOSECONDS_PER_MINUTE) % MINUTES_PER_HOUR, 2);
        buffer[offset++] = ':';
        offset = writeDigits(buffer, offset, (picos / PICOSECONDS_PER_SECOND) % SECONDS_PER_MINUTE, 2);
        if (precision > 0) {
            buffer[offset++] = '.';
            offset = writeDigits(buffer, offset, (picos % PICOSECONDS_PER_SECOND) / POWERS_OF_TEN[MAX_PRECISION - precision], precision);
        }
        return offset;
    }

    private static int writeDigits(char[] buffer, int offset, long value, int digits)
    {
        int end = offset + digits;
        for (int index = end - 1; index >= offset; index--) {
            buffer[index] = (char) ('0' + value % 10);
            value /= 10;
        }
        return end;
    }
}
