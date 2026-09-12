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

import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.StandardTypes;

import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;

@Description("Returns the UNIX timestamp in nanoseconds from a timestamp with time zone")
@ScalarFunction("to_unixtime_nanos")
public final class ToUnixTimeNanos
{
    private ToUnixTimeNanos() {}

    @LiteralParameters("p")
    @SqlType(StandardTypes.BIGINT)
    public static long toUnixTimeNanos(@SqlType("timestamp(p) with time zone") long timestamp)
    {
        return unpackMillisUtc(timestamp) * NANOSECONDS_PER_MILLISECOND;
    }

    @LiteralParameters("p")
    @SqlType(StandardTypes.BIGINT)
    public static long toUnixTimeNanos(@SqlType("timestamp(p) with time zone") LongTimestampWithTimeZone timestamp)
    {
        return timestamp.getEpochMillis() * NANOSECONDS_PER_MILLISECOND + timestamp.getPicosOfMilli() / PICOSECONDS_PER_NANOSECOND;
    }
}
