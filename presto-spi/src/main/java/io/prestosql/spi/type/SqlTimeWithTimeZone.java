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

import java.util.Objects;

import static io.prestosql.spi.type.TimeType.MAX_PRECISION;
import static io.prestosql.spi.type.TimeWithTimezoneTypes.normalizePicos;
import static io.prestosql.spi.type.Timestamps.MINUTES_PER_HOUR;
import static io.prestosql.spi.type.Timestamps.PICOSECONDS_PER_HOUR;
import static io.prestosql.spi.type.Timestamps.PICOSECONDS_PER_MINUTE;
import static io.prestosql.spi.type.Timestamps.PICOSECONDS_PER_SECOND;
import static io.prestosql.spi.type.Timestamps.POWERS_OF_TEN;
import static io.prestosql.spi.type.Timestamps.SECONDS_PER_MINUTE;
import static java.lang.String.format;

public final class SqlTimeWithTimeZone
{
    private final int precision;
    private final long picos;
    private final int offsetMinutes;

    public static SqlTimeWithTimeZone newInstance(int precision, long picoseconds, int offsetMinutes)
    {
        return new SqlTimeWithTimeZone(precision, picoseconds, offsetMinutes);
    }

    private SqlTimeWithTimeZone(int precision, long picos, int offsetMinutes)
    {
        this.precision = precision;
        this.picos = picos;
        this.offsetMinutes = offsetMinutes;
    }

    public long getPicos()
    {
        return picos;
    }

    public int getOffsetMinutes()
    {
        return offsetMinutes;
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

        SqlTimeWithTimeZone other = (SqlTimeWithTimeZone) o;
        return precision == other.precision && normalizePicos(picos, offsetMinutes) == normalizePicos(other.picos, offsetMinutes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(precision, normalizePicos(picos, offsetMinutes));
    }

    @JsonValue
    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append(format(
                "%02d:%02d:%02d",
                picos / PICOSECONDS_PER_HOUR,
                (picos / PICOSECONDS_PER_MINUTE) % MINUTES_PER_HOUR,
                (picos / PICOSECONDS_PER_SECOND) % SECONDS_PER_MINUTE));

        if (precision > 0) {
            long scaledFraction = (picos % PICOSECONDS_PER_SECOND) / POWERS_OF_TEN[MAX_PRECISION - precision];
            builder.append(".");
            builder.append(format("%0" + precision + "d", scaledFraction));
        }
        builder.append(format("%+03d:%02d", offsetMinutes / 60, offsetMinutes % 60));

        return builder.toString();
    }
}
