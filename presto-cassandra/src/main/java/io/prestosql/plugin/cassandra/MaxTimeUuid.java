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

import com.datastax.driver.core.utils.UUIDs;
import io.airlift.slice.Slice;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.LongTimestampWithTimeZone;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.type.DateTimeEncoding.unpackMillisUtc;

@Description("Cassandra maxTimeuuid function")
@ScalarFunction("max_timeuuid")
public final class MaxTimeUuid
{
    private MaxTimeUuid() {}

    @LiteralParameters("p")
    @SqlType("varchar(36)")
    public static Slice maxTimeuuid(@SqlType("timestamp(p) with time zone") long timestamp)
    {
        long epochMillis = unpackMillisUtc(timestamp);
        return utf8Slice(UUIDs.endOf(epochMillis).toString());
    }

    @LiteralParameters("p")
    @SqlType("varchar(36)")
    public static Slice maxTimeuuid(@SqlType("timestamp(p) with time zone") LongTimestampWithTimeZone timestamp)
    {
        throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Millisecond precision should be lower than or equal to 3");
    }
}
