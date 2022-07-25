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

import java.util.Objects;

import static io.trino.spi.type.TimeType.MAX_PRECISION;
import static io.trino.spi.type.Timestamps.MINUTES_PER_HOUR;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_HOUR;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MINUTE;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.POWERS_OF_TEN;
import static io.trino.spi.type.Timestamps.SECONDS_PER_MINUTE;
import static io.trino.spi.type.Timestamps.rescale;
import static java.lang.Math.abs;
import static java.lang.String.format;

public final class SqlTimeWithTimeZone
{
    private final int precision;
    private final long picos;
    private final int offsetMinutes;

    public static SqlTimeWithTimeZone newInstance(int precision, long picoseconds, int offsetMinutes)
    {
        if (precision < 0 || precision > 12) {
            throw new IllegalArgumentException("Invalid precision: " + precision);
        }
        if (rescale(rescale(picoseconds, 12, precision), precision, 12) != picoseconds) {
            throw new IllegalArgumentException(format("picoseconds contains data beyond specified precision (%s): %s", precision, picoseconds));
        }
        if (picoseconds < 0 || picoseconds >= PICOSECONDS_PER_DAY) {
            throw new IllegalArgumentException("picoseconds is out of range: " + picoseconds);
        }
        // TIME WITH TIME ZONE's valid offsets are [-14:00, 14:00]
        if (offsetMinutes < -14 * 60 || offsetMinutes > 14 * 60) {
            throw new IllegalArgumentException("offsetMinutes is out of range: " + offsetMinutes);
        }

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
        return precision == other.precision &&
                picos == other.picos &&
                offsetMinutes == other.offsetMinutes;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(precision, picos, offsetMinutes);
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
        builder.append(format("%s%02d:%02d", offsetMinutes >= 0 ? '+' : '-', abs(offsetMinutes / 60), abs(offsetMinutes % 60)));

        return builder.toString();
    }
}
