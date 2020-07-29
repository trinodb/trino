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
package io.prestosql.operator.scalar.timetz;

import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.LongTimeWithTimeZone;
import io.prestosql.spi.type.StandardTypes;

import static io.prestosql.spi.type.DateTimeEncoding.unpackTimeNanos;
import static io.prestosql.type.DateTimes.NANOSECONDS_PER_SECOND;
import static io.prestosql.type.DateTimes.PICOSECONDS_PER_SECOND;
import static io.prestosql.type.DateTimes.SECONDS_PER_MINUTE;

@Description("Second of the minute of the given time")
@ScalarFunction("second")
public class ExtractSecond
{
    private ExtractSecond() {}

    @LiteralParameters("p")
    @SqlType(StandardTypes.BIGINT)
    public static long extract(@SqlType("time(p) with time zone") long packedTime)
    {
        return (unpackTimeNanos(packedTime) / NANOSECONDS_PER_SECOND) % SECONDS_PER_MINUTE;
    }

    @LiteralParameters("p")
    @SqlType(StandardTypes.BIGINT)
    public static long extract(@SqlType("time(p) with time zone") LongTimeWithTimeZone time)
    {
        return (time.getPicoSeconds() / PICOSECONDS_PER_SECOND) % SECONDS_PER_MINUTE;
    }
}
