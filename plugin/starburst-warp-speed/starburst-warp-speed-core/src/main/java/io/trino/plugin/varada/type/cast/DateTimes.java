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
package io.trino.plugin.varada.type.cast;

import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimestampType;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.type.TimestampType.MAX_SHORT_PRECISION;
import static java.lang.Math.floorMod;
import static java.lang.Math.multiplyExact;
import static java.lang.String.format;
import static java.time.temporal.ChronoField.MICRO_OF_SECOND;

public final class DateTimes
{
    public static final Pattern DATETIME_PATTERN = Pattern.compile(
            "(?<year>[-+]?\\d{4,})-(?<month>\\d{1,2})-(?<day>\\d{1,2})" +
            "(?: (?<hour>\\d{1,2}):(?<minute>\\d{1,2})(?::(?<second>\\d{1,2})(?:\\.(?<fraction>\\d+))?)?)?" +
            "\\s*(?<timezone>.+)?");
    public static final int MICROSECONDS_PER_SECOND = 1_000_000;
    public static final int PICOSECONDS_PER_MICROSECOND = 1_000_000;
    public static final int PICOSECONDS_PER_NANOSECOND = 1000;
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
            1000_000_000_000L
    };

    private DateTimes() {}

    private static long roundDiv(long value, long factor)
    {
        checkArgument(factor > 0, "factor must be positive");

        if (factor == 1) {
            return value;
        }

        if (value >= 0) {
            return (value + (factor / 2)) / factor;
        }

        return (value + 1 - (factor / 2)) / factor;
    }

    public static long scaleEpochMicrosToSeconds(long epochMicros)
    {
        return Math.floorDiv(epochMicros, MICROSECONDS_PER_SECOND);
    }

    public static int getMicrosOfSecond(long epochMicros)
    {
        return floorMod(epochMicros, MICROSECONDS_PER_SECOND);
    }

    public static long round(long value, int magnitude)
    {
        return roundToNearest(value, POWERS_OF_TEN[magnitude]);
    }

    public static long roundToNearest(long value, long bound)
    {
        return roundDiv(value, bound) * bound;
    }

    public static long scaleFactor(int fromPrecision, int toPrecision)
    {
        if (fromPrecision > toPrecision) {
            throw new IllegalArgumentException("fromPrecision must be <= toPrecision");
        }

        return POWERS_OF_TEN[toPrecision - fromPrecision];
    }

    /**
     * Rescales a value of the given precision to another precision by adding 0s or truncating.
     */
    public static long rescale(long value, int fromPrecision, int toPrecision)
    {
        if (fromPrecision <= toPrecision) {
            value *= scaleFactor(fromPrecision, toPrecision);
        }
        else {
            value /= scaleFactor(toPrecision, fromPrecision);
        }

        return value;
    }

    /**
     * Formats a timestamp of the given precision. This method doesn't do any rounding, so it's expected that the
     * combination of [epochMicros, picosSecond] is already rounded to the provided precision if necessary
     */
    public static String formatTimestamp(int precision, long epochMicros, int picosOfMicro, ZoneId zoneId, DateTimeFormatter yearToSecondFormatter)
    {
        checkArgument(picosOfMicro >= 0 && picosOfMicro < PICOSECONDS_PER_MICROSECOND, "picosOfMicro is out of range [0, 1_000_000]");

        Instant instant = Instant.ofEpochSecond(scaleEpochMicrosToSeconds(epochMicros));
        LocalDateTime dateTime = LocalDateTime.ofInstant(instant, zoneId);
        long picoFraction = ((long) getMicrosOfSecond(epochMicros)) * PICOSECONDS_PER_MICROSECOND + picosOfMicro;

        return formatTimestamp(precision, dateTime, picoFraction, yearToSecondFormatter, builder -> {});
    }

    public static String formatTimestamp(int precision, LocalDateTime dateTime, long picoFraction, DateTimeFormatter yearToSecondFormatter, Consumer<StringBuilder> zoneIdFormatter)
    {
        StringBuilder builder = new StringBuilder();
        builder.append(yearToSecondFormatter.format(dateTime));
        if (precision > 0) {
            builder.append(".");
            builder.append(format("%0" + precision + "d", rescale(picoFraction, 12, precision)));
        }

        zoneIdFormatter.accept(builder);

        return builder.toString();
    }

    public static LongTimestamp longTimestamp(long precision, Instant start)
    {
        checkArgument(precision > MAX_SHORT_PRECISION && precision <= TimestampType.MAX_PRECISION, "Precision is out of range");
        return new LongTimestamp(
                start.getEpochSecond() * MICROSECONDS_PER_SECOND + start.getLong(MICRO_OF_SECOND),
                (int) round((start.getNano() % PICOSECONDS_PER_NANOSECOND) * ((long) PICOSECONDS_PER_NANOSECOND), (int) (TimestampType.MAX_PRECISION - precision)));
    }

    public static LongTimestamp longTimestamp(long epochSecond, long fractionInPicos)
    {
        return new LongTimestamp(
                multiplyExact(epochSecond, MICROSECONDS_PER_SECOND) + fractionInPicos / PICOSECONDS_PER_MICROSECOND,
                (int) (fractionInPicos % PICOSECONDS_PER_MICROSECOND));
    }
}
