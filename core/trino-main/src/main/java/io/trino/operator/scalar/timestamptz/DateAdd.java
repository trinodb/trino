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
package io.prestosql.operator.scalar.timestamptz;

import io.airlift.slice.Slice;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.LiteralParameter;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.LongTimestampWithTimeZone;
import io.prestosql.spi.type.StandardTypes;

import static io.prestosql.operator.scalar.DateTimeFunctions.getTimestampField;
import static io.prestosql.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.prestosql.spi.type.DateTimeEncoding.updateMillisUtc;
import static io.prestosql.type.DateTimes.round;
import static io.prestosql.util.DateTimeZoneIndex.unpackChronology;
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
