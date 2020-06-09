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
import io.prestosql.spi.function.ScalarOperator;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.LongTimestampWithTimeZone;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.TimeZoneKey;

import static io.prestosql.spi.function.OperatorType.CAST;
import static io.prestosql.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.prestosql.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.prestosql.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.prestosql.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.prestosql.type.Timestamps.roundToEpochMillis;
import static io.prestosql.util.DateTimeZoneIndex.getChronology;

@ScalarOperator(CAST)
public final class TimestampWithTimeZoneToTimeWithTimezoneCast
{
    private TimestampWithTimeZoneToTimeWithTimezoneCast() {}

    @LiteralParameters("p")
    @SqlType(StandardTypes.TIME_WITH_TIME_ZONE)
    public static long cast(@SqlType("timestamp(p) with time zone") long packedEpochMillis)
    {
        return convert(unpackMillisUtc(packedEpochMillis), unpackZoneKey(packedEpochMillis));
    }

    @LiteralParameters("p")
    @SqlType(StandardTypes.TIME_WITH_TIME_ZONE)
    public static long cast(@SqlType("timestamp(p) with time zone") LongTimestampWithTimeZone timestamp)
    {
        return convert(roundToEpochMillis(timestamp), getTimeZoneKey(timestamp.getTimeZoneKey()));
    }

    private static long convert(long epochMillis, TimeZoneKey timeZoneKey)
    {
        int millis = getChronology(timeZoneKey).millisOfDay().get(epochMillis);
        millis -= getChronology(timeZoneKey).getZone().getOffset(millis);
        return packDateTimeWithZone(millis, timeZoneKey);
    }
}
