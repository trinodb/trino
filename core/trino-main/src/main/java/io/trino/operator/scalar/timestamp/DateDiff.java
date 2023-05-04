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
import io.trino.spi.type.StandardTypes;
import org.joda.time.chrono.ISOChronology;

import static io.trino.operator.scalar.DateTimeFunctions.getTimestampField;
import static io.trino.type.DateTimes.scaleEpochMicrosToMillis;

@Description("Difference of the given times in the given unit")
@ScalarFunction(value = "date_diff", alias = "datediff")
public class DateDiff
{
    private DateDiff() {}

    @LiteralParameters({"x", "p"})
    @SqlType(StandardTypes.BIGINT)
    public static long diff(
            @SqlType("varchar(x)") Slice unit,
            @SqlType("timestamp(p)") long timestamp1,
            @SqlType("timestamp(p)") long timestamp2)
    {
        long epochMillis1 = scaleEpochMicrosToMillis(timestamp1);
        long epochMillis2 = scaleEpochMicrosToMillis(timestamp2);

        return getTimestampField(ISOChronology.getInstanceUTC(), unit).getDifferenceAsLong(epochMillis2, epochMillis1);
    }

    @LiteralParameters({"x", "p"})
    @SqlType(StandardTypes.BIGINT)
    public static long diff(
            @SqlType("varchar(x)") Slice unit,
            @SqlType("timestamp(p)") LongTimestamp timestamp1,
            @SqlType("timestamp(p)") LongTimestamp timestamp2)
    {
        // smallest unit of date_diff is "millisecond", so anything in the fraction is irrelevant
        return diff(unit, timestamp1.getEpochMicros(), timestamp2.getEpochMicros());
    }
}
