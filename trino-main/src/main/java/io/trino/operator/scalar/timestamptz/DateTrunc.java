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

import io.airlift.slice.Slice;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.LongTimestampWithTimeZone;
import org.joda.time.chrono.ISOChronology;

import static io.prestosql.operator.scalar.DateTimeFunctions.getTimestampField;
import static io.prestosql.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.prestosql.spi.type.DateTimeEncoding.updateMillisUtc;
import static io.prestosql.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.prestosql.util.DateTimeZoneIndex.getChronology;
import static io.prestosql.util.DateTimeZoneIndex.unpackChronology;

@Description("Truncate to the specified precision")
@ScalarFunction("date_trunc")
public final class DateTrunc
{
    private DateTrunc() {}

    @LiteralParameters({"x", "p"})
    @SqlType("timestamp(p) with time zone")
    public static long truncate(
            @SqlType("varchar(x)") Slice unit,
            @SqlType("timestamp(p) with time zone") long packedEpochMillis)
    {
        ISOChronology chronology = unpackChronology(packedEpochMillis);
        long epochMillis = unpackMillisUtc(packedEpochMillis);

        epochMillis = getTimestampField(chronology, unit).roundFloor(epochMillis);

        return updateMillisUtc(epochMillis, packedEpochMillis);
    }

    @LiteralParameters({"x", "p"})
    @SqlType("timestamp(p) with time zone")
    public static LongTimestampWithTimeZone truncate(
            @SqlType("varchar(x)") Slice unit,
            @SqlType("timestamp(p) with time zone") LongTimestampWithTimeZone timestamp)
    {
        ISOChronology chronology = getChronology(getTimeZoneKey(timestamp.getTimeZoneKey()));
        long epochMillis = timestamp.getEpochMillis();

        epochMillis = getTimestampField(chronology, unit).roundFloor(epochMillis);

        // smallest unit of truncation is "millisecond", so the fraction is always 0
        return LongTimestampWithTimeZone.fromEpochMillisAndFraction(epochMillis, 0, timestamp.getTimeZoneKey());
    }
}
