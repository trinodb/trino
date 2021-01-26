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
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.StandardTypes;

import static io.trino.operator.scalar.DateTimeFunctions.getTimestampField;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.updateMillisUtc;
import static io.trino.type.DateTimes.round;
import static io.trino.util.DateTimeZoneIndex.unpackChronology;
import static java.lang.Math.toIntExact;

@Description("Add the specified amount of time to the given timestamp")
@ScalarFunction("date_add")
public class DateAdd
{
    private DateAdd() {}

    @LiteralParameters({"x", "p"})
    @SqlType("timestamp(p) with time zone")
    public static long add(
            @LiteralParameter("p") long precision,
            @SqlType("varchar(x)") Slice unit,
            @SqlType(StandardTypes.BIGINT) long value,
            @SqlType("timestamp(p) with time zone") long packedEpochMillis)
    {
        long epochMillis = unpackMillisUtc(packedEpochMillis);

        epochMillis = getTimestampField(unpackChronology(packedEpochMillis), unit).add(epochMillis, toIntExact(value));
        epochMillis = round(epochMillis, (int) (3 - precision));

        return updateMillisUtc(epochMillis, packedEpochMillis);
    }

    @LiteralParameters({"x", "p"})
    @SqlType("timestamp(p) with time zone")
    public static LongTimestampWithTimeZone add(
            @SqlType("varchar(x)") Slice unit,
            @SqlType(StandardTypes.BIGINT) long value,
            @SqlType("timestamp(p) with time zone") LongTimestampWithTimeZone timestamp)
    {
        long epochMillis = getTimestampField(unpackChronology(timestamp.getTimeZoneKey()), unit).add(timestamp.getEpochMillis(), toIntExact(value));

        return LongTimestampWithTimeZone.fromEpochMillisAndFraction(epochMillis, timestamp.getPicosOfMilli(), timestamp.getTimeZoneKey());
    }
}
