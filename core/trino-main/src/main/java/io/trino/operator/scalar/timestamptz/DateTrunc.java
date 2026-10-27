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

import io.airlift.slice.Slice;
import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimestampWithTimeZone;

import java.time.ZoneId;

import static io.trino.operator.scalar.TimestampField.forTimestamp;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.updateMillisUtc;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.util.DateTimeZoneIndex.getZoneId;
import static io.trino.util.DateTimeZoneIndex.unpackZoneId;

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
        ZoneId zoneId = unpackZoneId(packedEpochMillis);
        long epochMillis = unpackMillisUtc(packedEpochMillis);

        epochMillis = forTimestamp(unit).truncate(epochMillis, zoneId);

        return updateMillisUtc(epochMillis, packedEpochMillis);
    }

    @LiteralParameters({"x", "p"})
    @SqlType("timestamp(p) with time zone")
    public static LongTimestampWithTimeZone truncate(
            @SqlType("varchar(x)") Slice unit,
            @SqlType("timestamp(p) with time zone") LongTimestampWithTimeZone timestamp)
    {
        ZoneId zoneId = getZoneId(getTimeZoneKey(timestamp.getTimeZoneKey()));
        long epochMillis = timestamp.getEpochMillis();

        epochMillis = forTimestamp(unit).truncate(epochMillis, zoneId);

        // smallest unit of truncation is "millisecond", so the fraction is always 0
        return LongTimestampWithTimeZone.fromEpochMillisAndFraction(epochMillis, 0, timestamp.getTimeZoneKey());
    }
}
