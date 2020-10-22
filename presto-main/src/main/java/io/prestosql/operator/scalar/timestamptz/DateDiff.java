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
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.LongTimestampWithTimeZone;
import io.prestosql.spi.type.StandardTypes;
import org.joda.time.chrono.ISOChronology;

import static io.prestosql.operator.scalar.DateTimeFunctions.getTimestampField;
import static io.prestosql.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.prestosql.util.DateTimeZoneIndex.unpackChronology;

@Description("Difference of the given times in the given unit")
@ScalarFunction("date_diff")
public class DateDiff
{
    private DateDiff() {}

    @LiteralParameters({"x", "p"})
    @SqlType(StandardTypes.BIGINT)
    public static long diff(
            @SqlType("varchar(x)") Slice unit,
            @SqlType("timestamp(p) with time zone") long packedEpochMillis1,
            @SqlType("timestamp(p) with time zone") long packedEpochMillis2)
    {
        return getTimestampField(unpackChronology(packedEpochMillis1), unit)
                .getDifferenceAsLong(unpackMillisUtc(packedEpochMillis2), unpackMillisUtc(packedEpochMillis1));
    }

    @LiteralParameters({"x", "p"})
    @SqlType(StandardTypes.BIGINT)
    public static long diff(
            @SqlType("varchar(x)") Slice unit,
            @SqlType("timestamp(p) with time zone") LongTimestampWithTimeZone timestamp1,
            @SqlType("timestamp(p) with time zone") LongTimestampWithTimeZone timestamp2)
    {
        long epochMillis1 = timestamp1.getEpochMillis();
        long epochMillis2 = timestamp2.getEpochMillis();

        ISOChronology chronology = ISOChronology.getInstanceUTC();
        return getTimestampField(chronology, unit).getDifferenceAsLong(epochMillis2, epochMillis1);
    }
}
