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
package io.trino.operator.scalar.timetz;

import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimeWithTimeZone;
import io.trino.spi.type.StandardTypes;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.DateTimeEncoding.packTimeWithTimeZone;
import static io.trino.spi.type.DateTimeEncoding.unpackOffsetMinutes;
import static io.trino.spi.type.DateTimeEncoding.unpackTimeNanos;
import static io.trino.type.DateTimes.NANOSECONDS_PER_DAY;
import static io.trino.type.DateTimes.NANOSECONDS_PER_MINUTE;
import static io.trino.type.DateTimes.PICOSECONDS_PER_DAY;
import static io.trino.type.DateTimes.PICOSECONDS_PER_MINUTE;
import static io.trino.util.Failures.checkCondition;
import static java.lang.Math.floorMod;
import static java.lang.Math.toIntExact;

@ScalarFunction(value = "$at_timezone", hidden = true)
public class AtTimeZoneWithOffset
{
    private AtTimeZoneWithOffset() {}

    @LiteralParameters({"x", "p"})
    @SqlType("time(p) with time zone")
    public static long atTimeZone(
            @SqlType("time(p) with time zone") long packedTime,
            @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long zoneOffset)
    {
        int offsetMinutes = getZoneOffsetMinutes(zoneOffset);
        long nanos = unpackTimeNanos(packedTime) - (unpackOffsetMinutes(packedTime) - offsetMinutes) * NANOSECONDS_PER_MINUTE;
        return packTimeWithTimeZone(floorMod(nanos, NANOSECONDS_PER_DAY), offsetMinutes);
    }

    @LiteralParameters({"x", "p"})
    @SqlType("time(p) with time zone")
    public static LongTimeWithTimeZone atTimeZone(
            @SqlType("time(p) with time zone") LongTimeWithTimeZone time,
            @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long zoneOffset)
    {
        int offsetMinutes = getZoneOffsetMinutes(zoneOffset);
        long picos = time.getPicoseconds() - (time.getOffsetMinutes() - offsetMinutes) * PICOSECONDS_PER_MINUTE;
        return new LongTimeWithTimeZone(floorMod(picos, PICOSECONDS_PER_DAY), getZoneOffsetMinutes(zoneOffset));
    }

    private static int getZoneOffsetMinutes(long interval)
    {
        checkCondition((interval % 60_000L) == 0L, INVALID_FUNCTION_ARGUMENT, "Invalid time zone offset interval: interval contains seconds");
        return toIntExact(interval / 60_000L);
    }
}
