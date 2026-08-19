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
package io.trino.type;

import java.util.Objects;

import static io.trino.client.IntervalDayTime.formatInterval;
import static io.trino.client.IntervalDayTime.toMicros;
import static io.trino.spi.type.Timestamps.round;

public class SqlIntervalDayTime
{
    private static final long MICROS_PER_MILLI = 1000;
    private static final int MICROSECOND_FRACTIONAL_DIGITS = 6;

    private final long microseconds;
    private final int picosOfMicro;
    private final int fractionalPrecision;

    public SqlIntervalDayTime(long microseconds)
    {
        this(microseconds, 0, 6);
    }

    public SqlIntervalDayTime(long microseconds, int fractionalPrecision)
    {
        this(microseconds, 0, fractionalPrecision);
    }

    public SqlIntervalDayTime(long microseconds, int picosOfMicro, int fractionalPrecision)
    {
        this.microseconds = microseconds;
        this.picosOfMicro = picosOfMicro;
        this.fractionalPrecision = fractionalPrecision;
    }

    public SqlIntervalDayTime(int day, int hour, int minute, int second, int millis)
    {
        microseconds = toMicros(day, hour, minute, second, millis * MICROS_PER_MILLI);
        picosOfMicro = 0;
        fractionalPrecision = 6;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(microseconds, picosOfMicro);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SqlIntervalDayTime other = (SqlIntervalDayTime) obj;
        return this.microseconds == other.microseconds && this.picosOfMicro == other.picosOfMicro;
    }

    @Override
    public String toString()
    {
        return formatInterval(microseconds, picosOfMicro, fractionalPrecision);
    }

    public long getMicros()
    {
        return microseconds;
    }

    /// Rounds this value to `precision` fractional-seconds digits. The precision is
    /// at most {@link #MICROSECOND_FRACTIONAL_DIGITS}, so the sub-microsecond fraction
    /// drops out and only the microseconds are rounded half-up. Used to render the
    /// legacy millisecond wire form for clients without the `PARAMETRIC_INTERVAL`
    /// capability.
    public SqlIntervalDayTime roundTo(int precision)
    {
        return new SqlIntervalDayTime(round(microseconds, MICROSECOND_FRACTIONAL_DIGITS - precision), precision);
    }
}
