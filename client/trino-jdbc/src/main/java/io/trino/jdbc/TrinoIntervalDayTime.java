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
package io.trino.jdbc;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.client.IntervalDayTime.formatInterval;
import static io.trino.client.IntervalDayTime.toMicros;
import static java.lang.Math.multiplyExact;

public class TrinoIntervalDayTime
        implements Comparable<TrinoIntervalDayTime>
{
    private static final long MICROS_PER_MILLI = 1000;
    private static final int PICOS_PER_MICRO = 1_000_000;
    private static final int MAX_PRECISION = 12;
    private static final int DEFAULT_FRACTIONAL_PRECISION = 6;

    private final long microSeconds;
    private final int picosOfMicro;
    private final int fractionalPrecision;

    /// Creates an interval from a whole number of milliseconds. The argument is milliseconds — the unit
    /// this constructor has always used — so callers compiled against the previous API keep their
    /// contract; use {@link #TrinoIntervalDayTime(long, int, int)} for microsecond or finer precision.
    /// The microsecond-backed representation cannot hold the full legacy millisecond range, so a value
    /// that would overflow is rejected rather than silently wrapped.
    public TrinoIntervalDayTime(long milliSeconds)
    {
        this(multiplyExact(milliSeconds, MICROS_PER_MILLI), 0, DEFAULT_FRACTIONAL_PRECISION);
    }

    public TrinoIntervalDayTime(int day, int hour, int minute, int second, int millis)
    {
        this(toMicros(day, hour, minute, second, millis * MICROS_PER_MILLI), 0, DEFAULT_FRACTIONAL_PRECISION);
    }

    /// @param microSeconds the whole-microsecond count
    /// @param picosOfMicro the picoseconds within the last microsecond, in `[0, 1_000_000)`
    /// @param fractionalPrecision the number of fractional-second digits to render, 0 to 12
    public TrinoIntervalDayTime(long microSeconds, int picosOfMicro, int fractionalPrecision)
    {
        checkArgument(picosOfMicro >= 0 && picosOfMicro < PICOS_PER_MICRO, "picosOfMicro must be in [0, 1_000_000): %s", picosOfMicro);
        checkArgument(fractionalPrecision >= 0 && fractionalPrecision <= MAX_PRECISION, "fractionalPrecision must be in [0, 12]: %s", fractionalPrecision);
        this.microSeconds = microSeconds;
        this.picosOfMicro = picosOfMicro;
        this.fractionalPrecision = fractionalPrecision;
    }

    /// The value truncated to whole milliseconds. Sub-millisecond precision is exposed by
    /// {@link #getMicroSeconds()} and {@link #getPicosOfMicro()}.
    public long getMilliSeconds()
    {
        return microSeconds / MICROS_PER_MILLI;
    }

    /// The whole-microsecond count.
    public long getMicroSeconds()
    {
        return microSeconds;
    }

    /// The picoseconds within the last microsecond, in `[0, 1_000_000)`.
    public int getPicosOfMicro()
    {
        return picosOfMicro;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(microSeconds, picosOfMicro);
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
        TrinoIntervalDayTime other = (TrinoIntervalDayTime) obj;
        return this.microSeconds == other.microSeconds &&
                this.picosOfMicro == other.picosOfMicro;
    }

    @Override
    public int compareTo(TrinoIntervalDayTime o)
    {
        int result = Long.compare(microSeconds, o.microSeconds);
        return result != 0 ? result : Integer.compare(picosOfMicro, o.picosOfMicro);
    }

    @Override
    public String toString()
    {
        return formatInterval(microSeconds, picosOfMicro, fractionalPrecision);
    }
}
