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
import io.trino.spi.type.StandardTypes;
import org.joda.time.DateTimeField;
import org.joda.time.chrono.ISOChronology;

import java.util.function.Function;

import static io.trino.operator.scalar.DateTimeFunctions.getTimestampFieldProvider;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.util.DateTimeZoneIndex.unpackChronology;

@Description("Difference of the given times in the given unit")
@ScalarFunction("date_diff")
public final class DateDiff
{
    private DateDiff() {}

    private static long diff(Function<ISOChronology, DateTimeField> fieldProvider, long packedEpochMillis1, long packedEpochMillis2)
    {
        return fieldProvider.apply(unpackChronology(packedEpochMillis1))
                .getDifferenceAsLong(unpackMillisUtc(packedEpochMillis2), unpackMillisUtc(packedEpochMillis1));
    }

    private static long diff(Function<ISOChronology, DateTimeField> fieldProvider, LongTimestampWithTimeZone timestamp1, LongTimestampWithTimeZone timestamp2)
    {
        long epochMillis1 = timestamp1.getEpochMillis();
        long epochMillis2 = timestamp2.getEpochMillis();

        ISOChronology chronology = ISOChronology.getInstanceUTC();
        return fieldProvider.apply(chronology).getDifferenceAsLong(epochMillis2, epochMillis1);
    }

    @ScalarFunctionImplementationChoice
    public static final class Row
    {
        private Row() {}

        @LiteralParameters({"x", "p"})
        @SqlType(StandardTypes.BIGINT)
        public static long diff(@SqlType("varchar(x)") Slice unit, @SqlType("timestamp(p) with time zone") long packedEpochMillis1, @SqlType("timestamp(p) with time zone") long packedEpochMillis2)
        {
            return DateDiff.diff(getTimestampFieldProvider(unit), packedEpochMillis1, packedEpochMillis2);
        }

        @LiteralParameters({"x", "p"})
        @SqlType(StandardTypes.BIGINT)
        public static long diff(@SqlType("varchar(x)") Slice unit, @SqlType("timestamp(p) with time zone") LongTimestampWithTimeZone timestamp1, @SqlType("timestamp(p) with time zone") LongTimestampWithTimeZone timestamp2)
        {
            return DateDiff.diff(getTimestampFieldProvider(unit), timestamp1, timestamp2);
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
        @SqlType(StandardTypes.BIGINT)
        public long diff(@SqlType("timestamp(p) with time zone") long packedEpochMillis1, @SqlType("timestamp(p) with time zone") long packedEpochMillis2)
        {
            return DateDiff.diff(fieldProvider, packedEpochMillis1, packedEpochMillis2);
        }

        @LiteralParameters("p")
        @SqlType(StandardTypes.BIGINT)
        public long diff(@SqlType("timestamp(p) with time zone") LongTimestampWithTimeZone timestamp1, @SqlType("timestamp(p) with time zone") LongTimestampWithTimeZone timestamp2)
        {
            return DateDiff.diff(fieldProvider, timestamp1, timestamp2);
        }
    }
}
