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
package io.prestosql.operator.scalar.timestamp;

import io.airlift.slice.Slice;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.LongTimestamp;
import org.joda.time.chrono.ISOChronology;

import static io.prestosql.operator.scalar.DateTimeFunctions.getTimestampField;
import static io.prestosql.type.DateTimes.scaleEpochMicrosToMillis;
import static io.prestosql.type.DateTimes.scaleEpochMillisToMicros;

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

        long result = getTimestampField(ISOChronology.getInstanceUTC(), unit).roundFloor(timestamp);

        return scaleEpochMillisToMicros(result);
    }

    @LiteralParameters({"x", "p"})
    @SqlType("timestamp(p)")
    public static LongTimestamp truncate(
            @SqlType("varchar(x)") Slice unit,
            @SqlType("timestamp(p)") LongTimestamp timestamp)
    {
        long epochMillis = scaleEpochMicrosToMillis(timestamp.getEpochMicros());

        long result = getTimestampField(ISOChronology.getInstanceUTC(), unit).roundFloor(epochMillis);

        // smallest unit of truncation is "millisecond", so the fraction is always 0
        return new LongTimestamp(scaleEpochMillisToMicros(result), 0);
    }
}
