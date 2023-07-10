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
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import static io.trino.spi.type.TimestampType.MAX_PRECISION;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.time.ZoneOffset.UTC;

public class Timestamps
{
    private static final String TIMESTAMP_FORMAT_PATTERN = "uuuu-MM-dd HH:mm:ss";
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern(TIMESTAMP_FORMAT_PATTERN);

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
            1000_000_000_000L
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
        return roundDiv(value, POWERS_OF_TEN[magnitude]) * POWERS_OF_TEN[magnitude];
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

        if (value >= 0) {
            return (value + (factor / 2)) / factor;
        }

        return (value + 1 - (factor / 2)) / factor;
    }

    public static long truncateEpochMicrosToMillis(long epochMicros)
    {
        return floorDiv(epochMicros, MICROSECONDS_PER_MILLISECOND) * MICROSECONDS_PER_MILLISECOND;
    }

    public static long epochMicrosToMillisWithRounding(long epochMicros)
    {
        return roundDiv(epochMicros, MICROSECONDS_PER_MILLISECOND);
    }

    static String formatTimestamp(int precision, long epochMicros, int picosOfMicro)
    {
        Instant instant = Instant.ofEpochSecond(floorDiv(epochMicros, MICROSECONDS_PER_SECOND));
        long picoFraction = ((long) floorMod(epochMicros, MICROSECONDS_PER_SECOND)) * PICOSECONDS_PER_MICROSECOND + picosOfMicro;
        LocalDateTime dateTime = LocalDateTime.ofInstant(instant, UTC);

        StringBuilder builder = new StringBuilder(timestampFormatLength(precision));
        formatTimestamp(precision, dateTime, picoFraction, builder);
        return builder.toString();
    }

    private static int timestampFormatLength(int precision)
    {
        return TIMESTAMP_FORMAT_PATTERN.length() + (precision == 0 ? 0 : precision + 1);
    }

    static String formatTimestampWithTimeZone(int precision, long epochMillis, int picosOfMilli, ZoneId zoneId)
    {
        Instant instant = Instant.ofEpochMilli(epochMillis);
        long picoFraction = (long) floorMod(epochMillis, MILLISECONDS_PER_SECOND) * PICOSECONDS_PER_MILLISECOND + picosOfMilli;
        LocalDateTime dateTime = LocalDateTime.ofInstant(instant, zoneId);

        String zoneIdString = zoneId.getId();
        StringBuilder builder = new StringBuilder(timestampFormatLength(precision) + zoneIdString.length() + 1);
        formatTimestamp(precision, dateTime, picoFraction, builder);
        return builder.append(' ').append(zoneIdString).toString();
    }

    private static void formatTimestamp(int precision, LocalDateTime dateTime, long picoFraction, StringBuilder builder)
    {
        TIMESTAMP_FORMATTER.formatTo(dateTime, builder);
        if (precision > 0) {
            long scaledFraction = picoFraction / POWERS_OF_TEN[MAX_PRECISION - precision];
            builder.append('.');
            builder.setLength(builder.length() + precision);
            int index = builder.length() - 1;

            // Append the fractional the decimal digits in reverse order
            // comparable to format("%0" + precision + "d", scaledFraction);
            for (int i = 0; i < precision; i++) {
                long temp = scaledFraction / 10;
                int digit = (int) (scaledFraction - (temp * 10));
                scaledFraction = temp;
                builder.setCharAt(index - i, (char) ('0' + digit));
            }
        }
    }
}
