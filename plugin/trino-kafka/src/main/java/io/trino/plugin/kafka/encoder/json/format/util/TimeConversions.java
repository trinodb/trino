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
package io.prestosql.plugin.kafka.encoder.json.format.util;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.type.Timestamps.NANOSECONDS_PER_DAY;
import static java.lang.Math.floorMod;

public final class TimeConversions
{
    public static final int MILLISECONDS_PER_SECOND = 1000; // TODO: duplicate of io.prestosql.plugin.base.type.DateTimes.MILLISECONDS_PER_SECOND
    public static final long MILLISECONDS_PER_DAY = 86_400_000;

    public static final int MICROSECONDS_PER_MILLISECOND = 1000; // TODO: duplicate of io.prestosql.plugin.base.type.DateTimes.MICROSECONDS_PER_MILLISECOND
    public static final int MICROSECONDS_PER_SECOND = 1_000_000; // TODO: duplicate of io.prestosql.plugin.base.type.DateTimes.MICROSECONDS_PER_SECOND

    public static final int NANOSECONDS_PER_MICROSECOND = 1_000; // TODO: duplicate of io.prestosql.plugin.base.type.DateTimes.NANOSECONDS_PER_MICROSECOND
    public static final int NANOSECONDS_PER_MILLISECOND = 1_000_000; // TODO: duplicate of io.prestosql.plugin.base.type.DateTimes.NANOSECONDS_PER_MILLISECOND

    public static final long PICOSECONDS_PER_SECOND = 1_000_000_000_000L;

    public static final int PICOSECONDS_PRECISION = 12;
    public static final int NANOSECONDS_PRECISION = 9;
    public static final int MILLISECONDS_PRECISION = 3;
    public static final int SECONDS_PRECISION = 0;

    // TODO: duplicate of io.prestosql.plugin.base.type.DateTimes.POWERS_OF_TEN
    private static final long[] POWERS_OF_TEN = {
            1L,
            10L,
            100L,
            1000L,
            10_000L,
            100_000L,
            1_000_000L,
            10_000_000L,
            100_000_000L,
            1_000_000_000L,
            10_000_000_000L,
            100_000_000_000L,
            1000_000_000_000L
    };

    private TimeConversions() {}

    // TODO: duplicate of io.prestosql.plugin.base.type.DateTimes.roundDiv
    private static long roundDiv(long value, long factor)
    {
        checkArgument(factor > 0, "factor must be positive");

        if (factor == 1) {
            return value;
        }

        if (value >= 0) {
            return (value + (factor / 2)) / factor;
        }

        return (value + 1 - (factor / 2)) / factor;
    }

    // TODO: duplicate of io.prestosql.plugin.base.type.DateTimes.round
    public static long round(long value, int magnitude)
    {
        return roundToNearest(value, POWERS_OF_TEN[magnitude]);
    }

    // TODO: duplicate of io.prestosql.plugin.base.type.DateTimes.roundToNearest
    public static long roundToNearest(long value, long bound)
    {
        return roundDiv(value, bound) * bound;
    }

    // TODO: duplicate of io.prestosql.plugin.base.type.DateTimes.scaleFactor
    public static long scaleFactor(int fromPrecision, int toPrecision)
    {
        if (fromPrecision > toPrecision) {
            throw new IllegalArgumentException("fromPrecision must be <= toPrecision");
        }

        return POWERS_OF_TEN[toPrecision - fromPrecision];
    }

    // TODO: duplicate of io.prestosql.plugin.base.type.DateTimes.rescale
    public static long rescale(long value, int fromPrecision, int toPrecision)
    {
        if (fromPrecision <= toPrecision) {
            value *= scaleFactor(fromPrecision, toPrecision);
        }
        else {
            value /= scaleFactor(toPrecision, fromPrecision);
        }

        return value;
    }

    // TODO: duplicate of io.prestosql.plugin.base.type.DateTimes.rescaleWithRounding
    public static long rescaleWithRounding(long value, int fromPrecision, int toPrecision)
    {
        value = round(value, fromPrecision - toPrecision);
        value = rescale(value, fromPrecision, toPrecision);
        return value;
    }

    // TODO: duplicate of io.prestosql.plugin.base.type.DateTimes.scaleEpochMicrosToMillis
    public static long scaleEpochMicrosToMillis(long value)
    {
        return Math.floorDiv(value, MICROSECONDS_PER_MILLISECOND);
    }

    // TODO: duplicate of io.prestosql.plugin.base.type.DateTimes.scaleEpochMicrosToSeconds
    public static long scaleEpochMicrosToSeconds(long epochMicros)
    {
        return Math.floorDiv(epochMicros, MICROSECONDS_PER_SECOND);
    }

    // TODO: duplicate of io.prestosql.plugin.base.type.DateTimes.scaleEpochMillisToSeconds
    public static long scaleEpochMillisToSeconds(long epochMillis)
    {
        return Math.floorDiv(epochMillis, MILLISECONDS_PER_SECOND);
    }

    // TODO: duplicate of io.prestosql.plugin.base.type.DateTimes.getMicrosOfSecond
    public static int getMicrosOfSecond(long epochMicros)
    {
        return floorMod(epochMicros, MICROSECONDS_PER_SECOND);
    }

    // TODO: duplicate of io.prestosql.plugin.base.type.DateTimes.getMillisOfSecond
    public static int getMillisOfSecond(long epochMillis)
    {
        return floorMod(epochMillis, MILLISECONDS_PER_SECOND);
    }

    public static long scalePicosToNanos(long picos)
    {
        return rescaleWithRounding(picos, PICOSECONDS_PRECISION, NANOSECONDS_PRECISION);
    }

    public static long scaleNanosToMillis(long nanos)
    {
        return rescaleWithRounding(nanos, NANOSECONDS_PRECISION, MILLISECONDS_PRECISION);
    }

    public static long scalePicosToMillis(long picos)
    {
        return rescaleWithRounding(picos, PICOSECONDS_PRECISION, MILLISECONDS_PRECISION);
    }

    public static long scalePicosToSeconds(long picos)
    {
        return rescaleWithRounding(picos, PICOSECONDS_PRECISION, SECONDS_PRECISION);
    }

    public static long getNanosOfDay(long epochNanos)
    {
        return Math.floorMod(epochNanos, NANOSECONDS_PER_DAY);
    }

    public static long getMillisOfDay(long epochMillis)
    {
        return Math.floorMod(epochMillis, MILLISECONDS_PER_DAY);
    }
}
