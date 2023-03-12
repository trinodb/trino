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

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Objects;

import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.Timestamps.formatTimestamp;
import static io.trino.spi.type.Timestamps.round;
import static io.trino.spi.type.Timestamps.roundDiv;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.String.format;

public final class SqlTimestamp
{
    private final int precision;
    private final long epochMicros;
    private final int picosOfMicros;

    public static SqlTimestamp fromMillis(int precision, long millis)
    {
        return newInstanceWithRounding(precision, millis * 1000, 0);
    }

    public static SqlTimestamp newInstance(int precision, long epochMicros, int picosOfMicro)
    {
        if (precision < 0 || precision > 12) {
            throw new IllegalArgumentException("Invalid precision: " + precision);
        }
        if (precision <= 6) {
            if (picosOfMicro != 0) {
                throw new IllegalArgumentException(format("Expected picosOfMicro to be 0 for precision %s: %s", precision, picosOfMicro));
            }
            if (round(epochMicros, 6 - precision) != epochMicros) {
                throw new IllegalArgumentException(format("Expected 0s for digits beyond precision %s: epochMicros = %s", precision, epochMicros));
            }
        }
        else {
            if (round(picosOfMicro, 12 - precision) != picosOfMicro) {
                throw new IllegalArgumentException(format("Expected 0s for digits beyond precision %s: picosOfMicro = %s", precision, picosOfMicro));
            }
        }

        if (picosOfMicro < 0 || picosOfMicro > PICOSECONDS_PER_MICROSECOND) {
            throw new IllegalArgumentException("picosOfMicro is out of range: " + picosOfMicro);
        }

        return new SqlTimestamp(precision, epochMicros, picosOfMicro);
    }

    private static SqlTimestamp newInstanceWithRounding(int precision, long epochMicros, int picosOfMicro)
    {
        if (precision < 6) {
            epochMicros = round(epochMicros, 6 - precision);
            picosOfMicro = 0;
        }
        else if (precision == 6) {
            if (round(picosOfMicro, 6) == PICOSECONDS_PER_MICROSECOND) {
                epochMicros++;
            }
            picosOfMicro = 0;
        }
        else {
            picosOfMicro = (int) round(picosOfMicro, 12 - precision);
        }

        return new SqlTimestamp(precision, epochMicros, picosOfMicro);
    }

    private SqlTimestamp(int precision, long epochMicros, int picosOfMicro)
    {
        this.precision = precision;
        this.epochMicros = epochMicros;
        this.picosOfMicros = picosOfMicro;
    }

    public int getPrecision()
    {
        return precision;
    }

    public long getMillis()
    {
        return roundDiv(epochMicros, 1000);
    }

    public long getEpochMicros()
    {
        return epochMicros;
    }

    public int getPicosOfMicros()
    {
        return picosOfMicros;
    }

    public SqlTimestamp roundTo(int precision)
    {
        return newInstanceWithRounding(precision, epochMicros, picosOfMicros);
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
        SqlTimestamp that = (SqlTimestamp) o;
        return epochMicros == that.epochMicros &&
                picosOfMicros == that.picosOfMicros &&
                precision == that.precision;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(epochMicros, picosOfMicros, precision);
    }

    @JsonValue
    @Override
    public String toString()
    {
        return formatTimestamp(precision, epochMicros, picosOfMicros);
    }

    /**
     * @return timestamp rounded to nanosecond precision
     */
    public LocalDateTime toLocalDateTime()
    {
        long epochSecond = floorDiv(epochMicros, MICROSECONDS_PER_SECOND);
        int microOfSecond = floorMod(epochMicros, MICROSECONDS_PER_SECOND);
        int nanoOfSecond = (microOfSecond * NANOSECONDS_PER_MICROSECOND) +
                roundDiv(picosOfMicros, PICOSECONDS_PER_NANOSECOND);
        return LocalDateTime.ofEpochSecond(epochSecond, nanoOfSecond, ZoneOffset.UTC);
    }
}
