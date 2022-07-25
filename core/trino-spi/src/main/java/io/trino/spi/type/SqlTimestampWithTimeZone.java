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

import com.fasterxml.jackson.annotation.JsonValue;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Objects;

import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.formatTimestampWithTimeZone;
import static io.trino.spi.type.Timestamps.round;
import static io.trino.spi.type.Timestamps.roundDiv;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class SqlTimestampWithTimeZone
{
    private static final int NANOSECONDS_PER_MILLISECOND = 1_000_000;
    private static final int PICOSECONDS_PER_NANOSECOND = 1_000;

    private final int precision;
    private final long epochMillis;
    private final int picosOfMilli;
    private final TimeZoneKey timeZoneKey;

    public static SqlTimestampWithTimeZone fromInstant(int precision, Instant instant, ZoneId zoneId)
    {
        return newInstanceWithRounding(precision, instant.toEpochMilli(), (instant.getNano() % NANOSECONDS_PER_MILLISECOND) * PICOSECONDS_PER_NANOSECOND, TimeZoneKey.getTimeZoneKey(zoneId.getId()));
    }

    public static SqlTimestampWithTimeZone newInstance(int precision, long epochMillis, int picosOfMilli, TimeZoneKey timeZoneKey)
    {
        if (precision < 0 || precision > 12) {
            throw new IllegalArgumentException("Invalid precision: " + precision);
        }
        if (precision <= 3) {
            if (picosOfMilli != 0) {
                throw new IllegalArgumentException(format("Expected picosOfMilli to be 0 for precision %s: %s", precision, picosOfMilli));
            }
            if (round(epochMillis, 3 - precision) != epochMillis) {
                throw new IllegalArgumentException(format("Expected 0s for digits beyond precision %s: epochMicros = %s", precision, epochMillis));
            }
        }
        else {
            if (round(picosOfMilli, 12 - precision) != picosOfMilli) {
                throw new IllegalArgumentException(format("Expected 0s for digits beyond precision %s: picosOfMilli = %s", precision, picosOfMilli));
            }
        }

        if (picosOfMilli < 0 || picosOfMilli > PICOSECONDS_PER_MILLISECOND) {
            throw new IllegalArgumentException("picosOfMilli is out of range: " + picosOfMilli);
        }

        return new SqlTimestampWithTimeZone(precision, epochMillis, picosOfMilli, timeZoneKey);
    }

    private static SqlTimestampWithTimeZone newInstanceWithRounding(int precision, long epochMillis, int picosOfMilli, TimeZoneKey sessionTimeZoneKey)
    {
        if (precision < 0 || precision > 12) {
            throw new IllegalArgumentException("Invalid precision: " + precision);
        }
        if (precision < 3) {
            epochMillis = round(epochMillis, 3 - precision);
            picosOfMilli = 0;
        }
        else if (precision == 3) {
            if (round(picosOfMilli, 12 - precision) == PICOSECONDS_PER_MILLISECOND) {
                epochMillis++;
            }
            picosOfMilli = 0;
        }
        else {
            picosOfMilli = (int) round(picosOfMilli, 12 - precision);
        }

        return new SqlTimestampWithTimeZone(precision, epochMillis, picosOfMilli, sessionTimeZoneKey);
    }

    // visible for testing
    SqlTimestampWithTimeZone(int precision, long epochMillis, int picosOfMilli, TimeZoneKey timeZoneKey)
    {
        this.precision = precision;
        this.epochMillis = epochMillis;
        this.picosOfMilli = picosOfMilli;
        this.timeZoneKey = requireNonNull(timeZoneKey, "timeZoneKey is null");
    }

    public int getPrecision()
    {
        return precision;
    }

    public long getEpochMillis()
    {
        return epochMillis;
    }

    public int getPicosOfMilli()
    {
        return picosOfMilli;
    }

    public long getMillisUtc()
    {
        return epochMillis;
    }

    public TimeZoneKey getTimeZoneKey()
    {
        return timeZoneKey;
    }

    public SqlTimestampWithTimeZone roundTo(int precision)
    {
        return newInstanceWithRounding(precision, epochMillis, picosOfMilli, timeZoneKey);
    }

    @JsonValue
    @Override
    public String toString()
    {
        return formatTimestampWithTimeZone(precision, epochMillis, picosOfMilli, timeZoneKey.getZoneId());
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SqlTimestampWithTimeZone that = (SqlTimestampWithTimeZone) o;
        return precision == that.precision &&
                epochMillis == that.epochMillis &&
                picosOfMilli == that.picosOfMilli &&
                timeZoneKey.equals(that.timeZoneKey);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(precision, epochMillis, picosOfMilli, timeZoneKey);
    }

    /**
     * @return timestamp with time zone rounded to nanosecond precision
     */
    public ZonedDateTime toZonedDateTime()
    {
        long epochSecond = floorDiv(epochMillis, MILLISECONDS_PER_SECOND);
        int nanosOfSecond = floorMod(epochMillis, MILLISECONDS_PER_SECOND) * NANOSECONDS_PER_MILLISECOND +
                roundDiv(picosOfMilli, PICOSECONDS_PER_NANOSECOND);

        // nanosOfSecond overflowing NANOSECONDS_PER_SECOND case is handled by Instant.ofEpochSecond
        return Instant.ofEpochSecond(epochSecond, nanosOfSecond)
                .atZone(timeZoneKey.getZoneId());
    }
}
