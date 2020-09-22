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

import com.datastax.driver.core.utils.UUIDs;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimestampWithTimeZone;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.StandardTypes.UUID;
import static io.trino.spi.type.UuidType.javaUuidToTrinoUuid;

@Description("Cassandra maxTimeuuid function")
@ScalarFunction("max_timeuuid")
public final class MaxTimeUuid
{
    private MaxTimeUuid() {}

    @LiteralParameters("p")
    @SqlType(UUID)
    public static Slice maxTimeuuid(@SqlType("timestamp(p) with time zone") long packedEpochMillis)
    {
        long epochMillis = unpackMillisUtc(packedEpochMillis);
        return javaUuidToTrinoUuid(UUIDs.endOf(epochMillis));
    }

    @LiteralParameters("p")
    @SqlType(UUID)
    public static Slice maxTimeuuid(@SqlType("timestamp(p) with time zone") LongTimestampWithTimeZone timestamp)
    {
        throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Precision should be lower than or equal to 3");
    }
}
