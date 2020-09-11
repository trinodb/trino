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

import com.fasterxml.jackson.annotation.JsonValue;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Objects;

import static io.prestosql.spi.type.Timestamps.PICOSECONDS_PER_MILLISECOND;
import static io.prestosql.spi.type.Timestamps.formatTimestampWithTimeZone;
import static io.prestosql.spi.type.Timestamps.round;
import static java.util.Objects.requireNonNull;

public final class SqlTimestampWithTimeZone
{
    private static final int NANOSECONDS_PER_MILLISECOND = 1_000_000;
    private static final int PICOSECONDS_PER_NANOSECOND = 1_000;

    private final int precision;
    private final long epochMillis;
    private final int picosOfMilli;
    private final TimeZoneKey timeZoneKey;

    public static SqlTimestampWithTimeZone newInstance(int precision, Instant instant, ZoneId zoneId)
    {
        return newInstanceWithRounding(precision, instant.toEpochMilli(), (instant.getNano() % NANOSECONDS_PER_MILLISECOND) * PICOSECONDS_PER_NANOSECOND, TimeZoneKey.getTimeZoneKey(zoneId.getId()));
    }

    public static SqlTimestampWithTimeZone newInstance(int precision, long epochMillis, int picosOfMilli, TimeZoneKey timeZoneKey)
    {
        return newInstanceWithRounding(precision, epochMillis, picosOfMilli, timeZoneKey);
    }

    private static SqlTimestampWithTimeZone newInstanceWithRounding(int precision, long epochMillis, int picosOfMilli, TimeZoneKey sessionTimeZoneKey)
    {
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

    private SqlTimestampWithTimeZone(int precision, long epochMillis, int picosOfMilli, TimeZoneKey timeZoneKey)
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
}
