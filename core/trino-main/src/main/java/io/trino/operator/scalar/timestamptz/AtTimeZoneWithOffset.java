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
package io.prestosql.operator.scalar.timestamptz;

import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.LongTimestampWithTimeZone;
import io.prestosql.spi.type.StandardTypes;

import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.prestosql.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.prestosql.spi.type.TimeZoneKey.getTimeZoneKeyForOffset;
import static io.prestosql.util.Failures.checkCondition;

@ScalarFunction(value = "at_timezone", hidden = true)
public class AtTimeZoneWithOffset
{
    private AtTimeZoneWithOffset() {}

    @LiteralParameters({"x", "p"})
    @SqlType("timestamp(p) with time zone")
    public static long atTimeZone(@SqlType("timestamp(p) with time zone") long packedEpochMillis, @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long zoneOffset)
    {
        return packDateTimeWithZone(unpackMillisUtc(packedEpochMillis), getTimeZoneKeyForOffset(getZoneOffsetMinutes(zoneOffset)));
    }

    @LiteralParameters({"x", "p"})
    @SqlType("timestamp(p) with time zone")
    public static LongTimestampWithTimeZone atTimeZone(@SqlType("timestamp(p) with time zone") LongTimestampWithTimeZone timestamp, @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long zoneOffset)
    {
        return LongTimestampWithTimeZone.fromEpochMillisAndFraction(timestamp.getEpochMillis(), timestamp.getPicosOfMilli(), getTimeZoneKeyForOffset(getZoneOffsetMinutes(zoneOffset)));
    }

    private static long getZoneOffsetMinutes(long interval)
    {
        checkCondition((interval % 60_000L) == 0L, INVALID_FUNCTION_ARGUMENT, "Invalid time zone offset interval: interval contains seconds");
        return interval / 60_000L;
    }
}
