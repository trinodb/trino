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

import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.type.IntervalDayTimeType.MAX_FRACTIONAL_PRECISION;

/// The physical value of a day-time interval whose fractional-seconds precision exceeds 6: a signed
/// count of microseconds together with the picoseconds within that microsecond, mirroring
/// [io.trino.spi.type.LongTimestamp].
public final class LongInterval
        implements Comparable<LongInterval>
{
    public static final int INSTANCE_SIZE = instanceSize(LongInterval.class);

    private final long micros;
    private final int picosOfMicro;

    public LongInterval(long micros, int picosOfMicro)
    {
        if (picosOfMicro < 0) {
            throw new IllegalArgumentException("picosOfMicro must be >= 0: " + picosOfMicro);
        }
        if (picosOfMicro >= PICOSECONDS_PER_MICROSECOND) {
            throw new IllegalArgumentException("picosOfMicro must be < 1_000_000: " + picosOfMicro);
        }
        this.micros = micros;
        this.picosOfMicro = picosOfMicro;
    }

    public long getMicros()
    {
        return micros;
    }

    public int getPicosOfMicro()
    {
        return picosOfMicro;
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
        LongInterval that = (LongInterval) o;
        return micros == that.micros && picosOfMicro == that.picosOfMicro;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(micros, picosOfMicro);
    }

    @Override
    public int compareTo(LongInterval other)
    {
        int value = Long.compare(micros, other.micros);
        if (value != 0) {
            return value;
        }
        return Integer.compare(picosOfMicro, other.picosOfMicro);
    }

    @Override
    public String toString()
    {
        return new SqlIntervalDayTime(micros, picosOfMicro, MAX_FRACTIONAL_PRECISION).toString();
    }
}
