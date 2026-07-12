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
import io.trino.spi.function.ConstantArgument;
import io.trino.spi.function.ConstantSpecialization;
import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.ScalarFunctionImplementationChoice;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.StandardTypes;
import org.joda.time.DateTimeField;
import org.joda.time.chrono.ISOChronology;

import static io.trino.operator.scalar.DateTimeFunctions.getTimestampField;
import static io.trino.type.DateTimes.scaleEpochMicrosToMillis;

@Description("Difference of the given times in the given unit")
@ScalarFunction("date_diff")
public final class DateDiff
{
    private DateDiff() {}

    private static long diff(DateTimeField field, long timestamp1, long timestamp2)
    {
        long epochMillis1 = scaleEpochMicrosToMillis(timestamp1);
        long epochMillis2 = scaleEpochMicrosToMillis(timestamp2);

        return field.getDifferenceAsLong(epochMillis2, epochMillis1);
    }

    private static long diff(DateTimeField field, LongTimestamp timestamp1, LongTimestamp timestamp2)
    {
        // smallest unit of date_diff is "millisecond", so anything in the fraction is irrelevant
        return diff(field, timestamp1.getEpochMicros(), timestamp2.getEpochMicros());
    }

    public static long diff(Slice unit, long timestamp1, long timestamp2)
    {
        return diff(getTimestampField(ISOChronology.getInstanceUTC(), unit), timestamp1, timestamp2);
    }

    public static long diff(Slice unit, LongTimestamp timestamp1, LongTimestamp timestamp2)
    {
        return diff(getTimestampField(ISOChronology.getInstanceUTC(), unit), timestamp1, timestamp2);
    }

    @ScalarFunctionImplementationChoice
    public static final class Row
    {
        private Row() {}

        @LiteralParameters({"x", "p"})
        @SqlType(StandardTypes.BIGINT)
        public static long diff(@SqlType("varchar(x)") Slice unit, @SqlType("timestamp(p)") long timestamp1, @SqlType("timestamp(p)") long timestamp2)
        {
            return DateDiff.diff(getTimestampField(ISOChronology.getInstanceUTC(), unit), timestamp1, timestamp2);
        }

        @LiteralParameters({"x", "p"})
        @SqlType(StandardTypes.BIGINT)
        public static long diff(@SqlType("varchar(x)") Slice unit, @SqlType("timestamp(p)") LongTimestamp timestamp1, @SqlType("timestamp(p)") LongTimestamp timestamp2)
        {
            return DateDiff.diff(getTimestampField(ISOChronology.getInstanceUTC(), unit), timestamp1, timestamp2);
        }
    }

    @ConstantSpecialization(arguments = 0)
    public static final class ConstantUnit
    {
        private final DateTimeField field;

        public ConstantUnit(@ConstantArgument(0) Slice unit)
        {
            field = getTimestampField(ISOChronology.getInstanceUTC(), unit);
        }

        @LiteralParameters("p")
        @SqlType(StandardTypes.BIGINT)
        public long diff(@SqlType("timestamp(p)") long timestamp1, @SqlType("timestamp(p)") long timestamp2)
        {
            return DateDiff.diff(field, timestamp1, timestamp2);
        }

        @LiteralParameters("p")
        @SqlType(StandardTypes.BIGINT)
        public long diff(@SqlType("timestamp(p)") LongTimestamp timestamp1, @SqlType("timestamp(p)") LongTimestamp timestamp2)
        {
            return DateDiff.diff(field, timestamp1, timestamp2);
        }
    }
}
