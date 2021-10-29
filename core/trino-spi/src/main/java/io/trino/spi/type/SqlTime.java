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
import static io.trino.spi.type.Timestamps.round;
import static java.lang.String.format;

public final class SqlTime
{
    private final int precision;
    private final long picos;

    public static SqlTime newInstance(int precision, long picos)
    {
        if (precision < 0 || precision > 12) {
            throw new IllegalArgumentException("Invalid precision: " + precision);
        }
        if (rescale(rescale(picos, 12, precision), precision, 12) != picos) {
            throw new IllegalArgumentException(format("picos contains data beyond specified precision (%s): %s", precision, picos));
        }
        if (picos < 0 || picos >= PICOSECONDS_PER_DAY) {
            throw new IllegalArgumentException("picos is out of range: " + picos);
        }

        return new SqlTime(precision, picos);
    }

    private SqlTime(int precision, long picos)
    {
        this.precision = precision;
        this.picos = picos;
    }

    public long getPicos()
    {
        return picos;
    }

    public SqlTime roundTo(int precision)
    {
        return new SqlTime(precision, round(picos, 12 - precision));
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
        SqlTime sqlTime = (SqlTime) o;
        return precision == sqlTime.precision &&
                picos == sqlTime.picos;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(precision, picos);
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
        return builder.toString();
    }
}
