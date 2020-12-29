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
package io.prestosql.plugin.cassandra;

import io.airlift.slice.Slice;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;

import java.util.UUID;

import static com.datastax.driver.core.utils.UUIDs.unixTimestamp;
import static io.prestosql.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.prestosql.spi.type.TimeZoneKey.UTC_KEY;

public final class TimeUuidFunctions
{
    private TimeUuidFunctions() {}

    @Description("Cassandra toTimestamp function")
    @ScalarFunction
    @SqlType("timestamp(3) with time zone")
    public static long timeuuidToTimestamp(@SqlType("varchar(36)") Slice timeUuid)
    {
        long epochMillis = unixTimestamp(UUID.fromString(timeUuid.toStringUtf8()));
        return packDateTimeWithZone(epochMillis, UTC_KEY);
    }
}
