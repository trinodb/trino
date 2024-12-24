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
package io.trino.plugin.neo4j.encoding;

import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampType;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.packTimeWithTimeZone;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_NANOS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.Timestamps.round;
import static java.time.ZoneOffset.UTC;

public class Encodings
{
    private Encodings() {}

    public static long toLongEncodedTime(LocalTime localTime, int precision)
    {
        checkArgument(precision <= 9 && precision >= 0, "Precision is out of range: %s", precision);

        return round(localTime.toNanoOfDay(), 9 - precision) * PICOSECONDS_PER_NANOSECOND;
    }

    public static long toLongEncodedTime(OffsetTime offsetTime, int precision)
    {
        checkArgument(precision <= 9 && precision >= 0, "Precision is out of range: %s", precision);

        long nanos = round(offsetTime.getLong(ChronoField.NANO_OF_DAY), 9 - precision);
        int offsetMinutes = offsetTime.getOffset().getTotalSeconds() / 60;

        return packTimeWithTimeZone(nanos, offsetMinutes);
    }

    public static long toLongEncodedTimestamp(LocalDateTime localDateTime, int precision)
    {
        checkArgument(precision >= 0, "Precision is out of range: %s", precision);
        checkArgument(precision <= TimestampType.MAX_SHORT_PRECISION, "Precision is out of range: %s", precision);

        long epochMicros = localDateTime.toEpochSecond(UTC) * MICROSECONDS_PER_SECOND
                + localDateTime.getNano() / NANOSECONDS_PER_MICROSECOND;

        return round(epochMicros, TimestampType.MAX_SHORT_PRECISION - precision);
    }

    public static LongTimestamp toLongTimestamp(LocalDateTime localDateTime, int precision)
    {
        // Adapted from trino-jbc StandardColumnMappings.
        checkArgument(precision > TimestampType.MAX_SHORT_PRECISION, "Precision is out of range: %s", precision);
        checkArgument(precision <= TimestampType.TIMESTAMP_NANOS.getPrecision(), "Precision is out of range: %s", precision);

        long epochMicros = localDateTime.toEpochSecond(UTC) * MICROSECONDS_PER_SECOND
                + localDateTime.getNano() / NANOSECONDS_PER_MICROSECOND;
        int picosOfMicro = (localDateTime.getNano() % NANOSECONDS_PER_MICROSECOND) * PICOSECONDS_PER_NANOSECOND;

        verify(picosOfMicro == round(picosOfMicro, TimestampType.MAX_PRECISION - precision),
                "Invalid value of picosOfMicro for precision %s: %s",
                precision,
                picosOfMicro);

        return new LongTimestamp(epochMicros, picosOfMicro);
    }

    public static long toLongEncodedTimestamp(ZonedDateTime zonedDateTime)
    {
        long millis = zonedDateTime.toInstant().toEpochMilli();
        TimeZoneKey timeZoneKey = TimeZoneKey.getTimeZoneKey(zonedDateTime.getZone().getId());
        return packDateTimeWithZone(millis, timeZoneKey);
    }

    public static LongTimestampWithTimeZone toLongTimestampWithTimeZone(OffsetDateTime offsetDateTime, int precision)
    {
        checkArgument(precision <= TIMESTAMP_TZ_NANOS.getPrecision(), "Precision is out of range: %s", precision);

        long nanos = round(offsetDateTime.getNano(), TIMESTAMP_TZ_NANOS.getPrecision() - precision);

        return LongTimestampWithTimeZone.fromEpochSecondsAndFraction(
                offsetDateTime.toEpochSecond(),
                nanos * PICOSECONDS_PER_NANOSECOND,
                UTC_KEY);
    }
}
