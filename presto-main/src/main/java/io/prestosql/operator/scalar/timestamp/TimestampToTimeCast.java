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
package io.prestosql.operator.scalar.timestamp;

import io.prestosql.spi.function.LiteralParameter;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarOperator;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.LongTimestamp;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static io.prestosql.spi.function.OperatorType.CAST;
import static io.prestosql.type.DateTimes.MICROSECONDS_PER_SECOND;
import static io.prestosql.type.DateTimes.PICOSECONDS_PER_DAY;
import static io.prestosql.type.DateTimes.PICOSECONDS_PER_MICROSECOND;
import static io.prestosql.type.DateTimes.PICOSECONDS_PER_SECOND;
import static io.prestosql.type.DateTimes.rescale;
import static io.prestosql.type.DateTimes.round;
import static io.prestosql.type.DateTimes.scaleEpochMicrosToSeconds;
import static java.lang.Math.multiplyExact;

@ScalarOperator(CAST)
public final class TimestampToTimeCast
{
    private TimestampToTimeCast() {}

    @LiteralParameters({"sourcePrecision", "targetPrecision"})
    @SqlType("time(targetPrecision)")
    public static long cast(
            @LiteralParameter("sourcePrecision") long sourcePrecision,
            @LiteralParameter("targetPrecision") long targetPrecision,
            @SqlType("timestamp(sourcePrecision)") long timestamp)
    {
        long epochSeconds = scaleEpochMicrosToSeconds(timestamp);
        long microOfSecond = timestamp % MICROSECONDS_PER_SECOND;

        long microOfDay = multiplyExact(getSecondOfDay(epochSeconds), MICROSECONDS_PER_SECOND) + microOfSecond;

        if (targetPrecision < 6) {
            microOfDay = round(microOfDay, (int) (6 - targetPrecision));
        }

        return rescale(microOfDay, 6, 12) % PICOSECONDS_PER_DAY;
    }

    @LiteralParameters({"sourcePrecision", "targetPrecision"})
    @SqlType("time(targetPrecision)")
    public static long cast(
            @LiteralParameter("targetPrecision") long targetPrecision,
            @SqlType("timestamp(sourcePrecision)") LongTimestamp timestamp)
    {
        long epochSeconds = scaleEpochMicrosToSeconds(timestamp.getEpochMicros());
        long secondOfDay = getSecondOfDay(epochSeconds);

        long picoOfDay = multiplyExact(secondOfDay, PICOSECONDS_PER_SECOND) +
                multiplyExact(timestamp.getEpochMicros() % MICROSECONDS_PER_SECOND, PICOSECONDS_PER_MICROSECOND) +
                timestamp.getPicosOfMicro();

        return round(picoOfDay, (int) (12 - targetPrecision)) % PICOSECONDS_PER_DAY;
    }

    private static long getSecondOfDay(long epochSeconds)
    {
        return LocalDateTime.ofEpochSecond(epochSeconds, 0, ZoneOffset.UTC)
                .toLocalTime()
                .toSecondOfDay();
    }
}
