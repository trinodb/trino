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

import io.trino.operator.scalar.preimage.TimestampWithTimeZoneDatePreimage;
import io.trino.spi.function.FunctionPreimage;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimeZoneKey;
import org.joda.time.chrono.ISOChronology;

import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_DAY;
import static io.trino.util.DateTimeZoneIndex.getChronology;
import static java.lang.Math.floorDiv;

@FunctionPreimage(TimestampWithTimeZoneDatePreimage.class)
@ScalarOperator(CAST)
@ScalarFunction("date")
public final class TimestampWithTimeZoneToDateCast
{
    private TimestampWithTimeZoneToDateCast() {}

    @LiteralParameters("p")
    @SqlType(StandardTypes.DATE)
    public static long cast(@SqlType("timestamp(p) with time zone") long timestamp)
    {
        long epochMillis = unpackMillisUtc(timestamp);
        TimeZoneKey zoneKey = unpackZoneKey(timestamp);
        return toDate(epochMillis, zoneKey);
    }

    @LiteralParameters("p")
    @SqlType(StandardTypes.DATE)
    public static long cast(@SqlType("timestamp(p) with time zone") LongTimestampWithTimeZone timestamp)
    {
        return toDate(timestamp.getEpochMillis(), getTimeZoneKey(timestamp.getTimeZoneKey()));
    }

    private static long toDate(long epochMillis, TimeZoneKey zoneKey)
    {
        // Note: update TimestampWithTimeZoneDatePreimage whenever CAST behavior changes.
        // round down the current timestamp to days
        ISOChronology chronology = getChronology(zoneKey);
        long date = chronology.dayOfYear().roundFloor(epochMillis);
        // date is currently midnight in timezone of the original value
        // convert to UTC
        long millis = date + chronology.getZone().getOffset(date);
        // When midnight falls in a gap, the day starts later than midnight, so round down
        return floorDiv(millis, MILLISECONDS_PER_DAY);
    }
}
