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

import java.util.Objects;
import java.util.StringJoiner;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MILLISECOND;

public final class LongTimestampWithTimeZone
        implements Comparable<LongTimestampWithTimeZone>
{
    public static final int INSTANCE_SIZE = instanceSize(LongTimestampWithTimeZone.class);

    private final long epochMillis;
    private final int picosOfMilli; // number of picoseconds of the millisecond corresponding to epochMillis
    private final short timeZoneKey;

    public static LongTimestampWithTimeZone fromEpochSecondsAndFraction(long epochSecond, long fractionInPicos, TimeZoneKey timeZoneKey)
    {
        return fromEpochMillisAndFraction(
                epochSecond * 1_000 + fractionInPicos / 1_000_000_000,
                (int) (fractionInPicos % 1_000_000_000),
                timeZoneKey);
    }

    public static LongTimestampWithTimeZone fromEpochMillisAndFraction(long epochMillis, int picosOfMilli, TimeZoneKey timeZoneKey)
    {
        return new LongTimestampWithTimeZone(epochMillis, picosOfMilli, timeZoneKey.getKey());
    }

    public static LongTimestampWithTimeZone fromEpochMillisAndFraction(long epochMillis, int picosOfMilli, short timeZoneKey)
    {
        return new LongTimestampWithTimeZone(epochMillis, picosOfMilli, timeZoneKey);
    }

    private LongTimestampWithTimeZone(long epochMillis, int picosOfMilli, short timeZoneKey)
    {
        if (picosOfMilli < 0) {
            throw new IllegalArgumentException("picosOfMilli must be >= 0");
        }
        if (picosOfMilli >= PICOSECONDS_PER_MILLISECOND) {
            throw new IllegalArgumentException("picosOfMilli must be < " + PICOSECONDS_PER_MILLISECOND);
        }
        this.epochMillis = epochMillis;
        this.picosOfMilli = picosOfMilli;
        this.timeZoneKey = timeZoneKey;
    }

    public long getEpochMillis()
    {
        return epochMillis;
    }

    public int getPicosOfMilli()
    {
        return picosOfMilli;
    }

    public short getTimeZoneKey()
    {
        return timeZoneKey;
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
        LongTimestampWithTimeZone that = (LongTimestampWithTimeZone) o;
        return epochMillis == that.epochMillis &&
                picosOfMilli == that.picosOfMilli &&
                timeZoneKey == that.timeZoneKey;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(epochMillis, picosOfMilli, timeZoneKey);
    }

    @Override
    public int compareTo(LongTimestampWithTimeZone other)
    {
        int value = Long.compare(epochMillis, other.epochMillis);
        if (value != 0) {
            return value;
        }
        return Integer.compare(picosOfMilli, other.picosOfMilli);
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", LongTimestampWithTimeZone.class.getSimpleName() + "[", "]")
                .add("epochMillis=" + epochMillis)
                .add("picosOfMilli=" + picosOfMilli)
                .add("timeZoneKey=" + timeZoneKey)
                .toString();
    }
}
