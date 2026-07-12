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
import io.trino.spi.function.ConstantArgument;
import io.trino.spi.function.ConstantSpecialization;
import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.ScalarFunctionImplementationChoice;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimestampWithTimeZone;
import org.joda.time.DateTimeField;
import org.joda.time.chrono.ISOChronology;

import java.util.function.Function;

import static io.trino.operator.scalar.DateTimeFunctions.getTimestampFieldProvider;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.updateMillisUtc;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.util.DateTimeZoneIndex.getChronology;
import static io.trino.util.DateTimeZoneIndex.unpackChronology;

@Description("Truncate to the specified precision")
@ScalarFunction("date_trunc")
public final class DateTrunc
{
    private DateTrunc() {}

    private static long truncate(Function<ISOChronology, DateTimeField> fieldProvider, long packedEpochMillis)
    {
        ISOChronology chronology = unpackChronology(packedEpochMillis);
        long epochMillis = unpackMillisUtc(packedEpochMillis);

        epochMillis = fieldProvider.apply(chronology).roundFloor(epochMillis);

        return updateMillisUtc(epochMillis, packedEpochMillis);
    }

    private static LongTimestampWithTimeZone truncate(Function<ISOChronology, DateTimeField> fieldProvider, LongTimestampWithTimeZone timestamp)
    {
        ISOChronology chronology = getChronology(getTimeZoneKey(timestamp.getTimeZoneKey()));
        long epochMillis = timestamp.getEpochMillis();

        epochMillis = fieldProvider.apply(chronology).roundFloor(epochMillis);

        // smallest unit of truncation is "millisecond", so the fraction is always 0
        return LongTimestampWithTimeZone.fromEpochMillisAndFraction(epochMillis, 0, timestamp.getTimeZoneKey());
    }

    @ScalarFunctionImplementationChoice
    public static final class Row
    {
        private Row() {}

        @LiteralParameters({"x", "p"})
        @SqlType("timestamp(p) with time zone")
        public static long truncate(@SqlType("varchar(x)") Slice unit, @SqlType("timestamp(p) with time zone") long packedEpochMillis)
        {
            return DateTrunc.truncate(getTimestampFieldProvider(unit), packedEpochMillis);
        }

        @LiteralParameters({"x", "p"})
        @SqlType("timestamp(p) with time zone")
        public static LongTimestampWithTimeZone truncate(@SqlType("varchar(x)") Slice unit, @SqlType("timestamp(p) with time zone") LongTimestampWithTimeZone timestamp)
        {
            return DateTrunc.truncate(getTimestampFieldProvider(unit), timestamp);
        }
    }

    @ConstantSpecialization(arguments = 0)
    public static final class ConstantUnit
    {
        private final Function<ISOChronology, DateTimeField> fieldProvider;

        public ConstantUnit(@ConstantArgument(0) Slice unit)
        {
            fieldProvider = getTimestampFieldProvider(unit);
        }

        @LiteralParameters("p")
        @SqlType("timestamp(p) with time zone")
        public long truncate(@SqlType("timestamp(p) with time zone") long packedEpochMillis)
        {
            return DateTrunc.truncate(fieldProvider, packedEpochMillis);
        }

        @LiteralParameters("p")
        @SqlType("timestamp(p) with time zone")
        public LongTimestampWithTimeZone truncate(@SqlType("timestamp(p) with time zone") LongTimestampWithTimeZone timestamp)
        {
            return DateTrunc.truncate(fieldProvider, timestamp);
        }
    }
}
