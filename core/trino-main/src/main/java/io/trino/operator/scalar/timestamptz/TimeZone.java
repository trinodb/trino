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
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimeZoneKey;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;

@Description("Time zone of the given timestamp")
@ScalarFunction("timezone")
public final class TimeZone
{
    private TimeZone() {}

    /**
     * Extracts the time zone from a `timestamp(p) with time zone` as a VARCHAR.
     * <p>
     * This function takes a packed long value representing the
     * timestamp with time zone and extracts the associated time zone.
     *
     * @param timestampWithTimeZone the packed long
     * representing a `timestamp(p) with time zone`
     * @return the time zone ID as a VARCHAR
     */
    @LiteralParameters("p")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice extractTimeZoneFromTimestamp(@SqlType("timestamp(p) with time zone") long timestampWithTimeZone)
    {
        TimeZoneKey timeZoneKey = unpackZoneKey(timestampWithTimeZone);
        return utf8Slice(timeZoneKey.getId());
    }

    /**
     * Extracts the time zone from a `timestamp(p) with
     * time zone` with a LongTimestampWithTimeZone format as a VARCHAR.
     * <p>
     * This function takes a LongTimestampWithTimeZone
     * object and extracts the associated time zone.
     *
     * @param timestampWithTimeZone the LongTimestampWithTimeZone object
     * @return the time zone ID as a VARCHAR
     */
    @LiteralParameters("p")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice extractTimeZoneFromTimestamp(@SqlType("timestamp(p) with time zone") LongTimestampWithTimeZone timestampWithTimeZone)
    {
        TimeZoneKey zoneKey = TimeZoneKey.getTimeZoneKey(timestampWithTimeZone.getTimeZoneKey());
        return utf8Slice(zoneKey.getId());
    }
}
