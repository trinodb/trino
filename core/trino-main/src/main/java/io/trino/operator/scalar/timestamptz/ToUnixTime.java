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
package io.trino.operator.scalar.timestamptz;

import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.StandardTypes;

import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_SECOND;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.fma;

@ScalarFunction("to_unixtime")
public final class ToUnixTime
{
    private ToUnixTime() {}

    @LiteralParameters("p")
    @SqlType(StandardTypes.DOUBLE)
    public static double toUnixTime(@SqlType("timestamp(p) with time zone") long timestamp)
    {
        return unpackMillisUtc(timestamp) * 1.0 / MILLISECONDS_PER_SECOND;
    }

    @LiteralParameters("p")
    @SqlType(StandardTypes.DOUBLE)
    public static double toUnixTime(@SqlType("timestamp(p) with time zone") LongTimestampWithTimeZone timestamp)
    {
        long epochSeconds = floorDiv(timestamp.getEpochMillis(), MILLISECONDS_PER_SECOND);
        long picosOfSecond = (long) floorMod(timestamp.getEpochMillis(), MILLISECONDS_PER_SECOND) * PICOSECONDS_PER_MILLISECOND + timestamp.getPicosOfMilli();
        // Carry the rounding errors of the fraction and of the sum, and round once at the end. The exact result is a multiple
        // of 10^-12, so it is either a midpoint between adjacent doubles or far from one compared to the errors left.
        double fraction = (double) picosOfSecond / PICOSECONDS_PER_SECOND;
        double fractionError = fma(-fraction, PICOSECONDS_PER_SECOND, picosOfSecond) / PICOSECONDS_PER_SECOND;
        double sum = epochSeconds + fraction;
        double sumError = fraction - (sum - epochSeconds);
        return sum + (sumError + fractionError);
    }
}
