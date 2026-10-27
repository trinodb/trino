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
package io.trino.operator.scalar.timestamp;

import io.airlift.slice.Slice;
import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimestamp;

import static io.trino.operator.scalar.TimestampField.forTimestamp;
import static io.trino.type.DateTimes.scaleEpochMicrosToMillis;
import static io.trino.type.DateTimes.scaleEpochMillisToMicros;
import static java.time.ZoneOffset.UTC;

@Description("Truncate to the specified precision in the session timezone")
@ScalarFunction("date_trunc")
public final class DateTrunc
{
    private DateTrunc() {}

    @LiteralParameters({"x", "p"})
    @SqlType("timestamp(p)")
    public static long truncate(
            @SqlType("varchar(x)") Slice unit,
            @SqlType("timestamp(p)") long timestamp)
    {
        timestamp = scaleEpochMicrosToMillis(timestamp);

        long result = forTimestamp(unit).truncate(timestamp, UTC);

        return scaleEpochMillisToMicros(result);
    }

    @LiteralParameters({"x", "p"})
    @SqlType("timestamp(p)")
    public static LongTimestamp truncate(
            @SqlType("varchar(x)") Slice unit,
            @SqlType("timestamp(p)") LongTimestamp timestamp)
    {
        long epochMillis = scaleEpochMicrosToMillis(timestamp.getEpochMicros());

        long result = forTimestamp(unit).truncate(epochMillis, UTC);

        // smallest unit of truncation is "millisecond", so the fraction is always 0
        return new LongTimestamp(scaleEpochMillisToMicros(result), 0);
    }
}
