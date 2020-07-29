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
package io.prestosql.operator.scalar.timetz;

import io.prestosql.spi.function.LiteralParameter;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarOperator;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.LongTimeWithTimeZone;

import static io.prestosql.spi.function.OperatorType.CAST;
import static io.prestosql.spi.type.DateTimeEncoding.unpackTimeNanos;
import static io.prestosql.spi.type.TimeType.MAX_PRECISION;
import static io.prestosql.type.DateTimes.PICOSECONDS_PER_DAY;
import static io.prestosql.type.DateTimes.PICOSECONDS_PER_NANOSECOND;
import static io.prestosql.type.DateTimes.round;

@ScalarOperator(CAST)
public final class TimeWithTimeZoneToTimeCast
{
    private TimeWithTimeZoneToTimeCast() {}

    @LiteralParameters({"sourcePrecision", "targetPrecision"})
    @SqlType("time(targetPrecision)")
    public static long cast(
            @LiteralParameter("targetPrecision") long targetPrecision,
            @SqlType("time(sourcePrecision) with time zone") long packedTime)
    {
        return convert(targetPrecision, unpackTimeNanos(packedTime) * PICOSECONDS_PER_NANOSECOND);
    }

    @LiteralParameters({"sourcePrecision", "targetPrecision"})
    @SqlType("time(targetPrecision)")
    public static long cast(
            @LiteralParameter("targetPrecision") long targetPrecision,
            @SqlType("time(sourcePrecision) with time zone") LongTimeWithTimeZone timestamp)
    {
        return convert(targetPrecision, timestamp.getPicoSeconds());
    }

    private static long convert(long targetPrecision, long picos)
    {
        return round(picos, (int) (MAX_PRECISION - targetPrecision)) % PICOSECONDS_PER_DAY;
    }
}
