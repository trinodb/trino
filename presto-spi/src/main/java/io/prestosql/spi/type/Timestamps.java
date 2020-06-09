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
package io.prestosql.spi.type;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import static io.prestosql.spi.type.TimestampType.MAX_PRECISION;
import static java.lang.Math.floorMod;
import static java.lang.String.format;

class Timestamps
{
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss");
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

    public static final int MILLISECONDS_PER_SECOND = 1_000;
    public static final int MICROSECONDS_PER_SECOND = 1_000_000;
    public static final int PICOSECONDS_PER_MICROSECOND = 1_000_000;
    public static final int PICOSECONDS_PER_MILLISECOND = 1_000_000_000;

    private Timestamps() {}

    public static long round(long value, int magnitude)
    {
        return roundDiv(value, POWERS_OF_TEN[magnitude]) * POWERS_OF_TEN[magnitude];
    }

    public static long roundDiv(long value, long factor)
    {
        if (value >= 0) {
            return (value + (factor / 2)) / factor;
        }

        return (value + 1 - (factor / 2)) / factor;
    }

    public static String formatTimestamp(int precision, long epochMicros, int picosOfMicro, ZoneId zoneId)
    {
        Instant instant = Instant.ofEpochSecond(Math.floorDiv(epochMicros, MICROSECONDS_PER_SECOND));
        long picoFraction = ((long) floorMod(epochMicros, MICROSECONDS_PER_SECOND)) * PICOSECONDS_PER_MICROSECOND + picosOfMicro;
        LocalDateTime dateTime = LocalDateTime.ofInstant(instant, zoneId);

        return formatTimestamp(precision, dateTime, picoFraction).toString();
    }

    public static String formatTimestampWithTimeZone(int precision, long epochMillis, int picosOfMilli, ZoneId zoneId)
    {
        Instant instant = Instant.ofEpochMilli(epochMillis);
        long picoFraction = (long) floorMod(epochMillis, MILLISECONDS_PER_SECOND) * PICOSECONDS_PER_MILLISECOND + picosOfMilli;
        LocalDateTime dateTime = LocalDateTime.ofInstant(instant, zoneId);

        return formatTimestamp(precision, dateTime, picoFraction)
                .append(" ")
                .append(zoneId.getId()).toString();
    }

    private static StringBuilder formatTimestamp(int precision, LocalDateTime dateTime, long picoFraction)
    {
        StringBuilder builder = new StringBuilder();
        builder.append(TIMESTAMP_FORMATTER.format(dateTime));
        if (precision > 0) {
            long scaledFraction = picoFraction / POWERS_OF_TEN[MAX_PRECISION - precision];
            builder.append(".");
            builder.append(format("%0" + precision + "d", scaledFraction));
        }
        return builder;
    }
}
