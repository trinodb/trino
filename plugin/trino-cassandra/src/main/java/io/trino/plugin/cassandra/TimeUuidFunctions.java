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
package io.trino.plugin.cassandra;

import io.airlift.slice.Slice;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;

import static com.datastax.driver.core.utils.UUIDs.unixTimestamp;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.StandardTypes.UUID;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.UuidType.trinoUuidToJavaUuid;

public final class TimeUuidFunctions
{
    private TimeUuidFunctions() {}

    @Description("Cassandra toTimestamp function")
    @ScalarFunction
    @SqlType("timestamp(3) with time zone")
    public static long timeuuidToTimestamp(@SqlType(UUID) Slice timeUuid)
    {
        long epochMillis = unixTimestamp(trinoUuidToJavaUuid(timeUuid));
        return packDateTimeWithZone(epochMillis, UTC_KEY);
    }
}
