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
import io.trino.operator.scalar.DateTimeFunctions;
import io.trino.spi.TrinoException;
import io.trino.spi.function.ConstantArgument;
import io.trino.spi.function.ConstantSpecialization;
import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.ScalarFunctionImplementationChoice;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.StandardTypes;
import org.joda.time.DateTimeField;
import org.joda.time.chrono.ISOChronology;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.Timestamps.round;
import static io.trino.type.DateTimes.getMicrosOfMilli;
import static io.trino.type.DateTimes.scaleEpochMicrosToMillis;
import static io.trino.type.DateTimes.scaleEpochMillisToMicros;

@Description("Add the specified amount of time to the given timestamp")
@ScalarFunction("date_add")
public final class DateAdd
{
    private DateAdd() {}

    private static long add(long precision, DateTimeField field, long value, long timestamp)
    {
        try {
            long epochMillis = scaleEpochMicrosToMillis(timestamp);
            int microsOfMilli = getMicrosOfMilli(timestamp);

            epochMillis = field.add(epochMillis, value);

            if (precision <= 3) {
                epochMillis = round(epochMillis, (int) (3 - precision));
            }

            return scaleEpochMillisToMicros(epochMillis) + microsOfMilli;
        }
        catch (IllegalArgumentException | ArithmeticException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, e.getMessage());
        }
    }

    private static LongTimestamp add(DateTimeField field, long value, LongTimestamp timestamp)
    {
        return new LongTimestamp(
                add(6, field, value, timestamp.getEpochMicros()),
                timestamp.getPicosOfMicro());
    }

    @ScalarFunctionImplementationChoice
    public static final class Row
    {
        private Row() {}

        @LiteralParameters({"x", "p"})
        @SqlType("timestamp(p)")
        public static long add(@LiteralParameter("p") long precision, @SqlType("varchar(x)") Slice unit, @SqlType(StandardTypes.BIGINT) long value, @SqlType("timestamp(p)") long timestamp)
        {
            return DateAdd.add(precision, DateTimeFunctions.getTimestampField(ISOChronology.getInstanceUTC(), unit), value, timestamp);
        }

        @LiteralParameters({"x", "p"})
        @SqlType("timestamp(p)")
        public static LongTimestamp add(@SqlType("varchar(x)") Slice unit, @SqlType(StandardTypes.BIGINT) long value, @SqlType("timestamp(p)") LongTimestamp timestamp)
        {
            return DateAdd.add(DateTimeFunctions.getTimestampField(ISOChronology.getInstanceUTC(), unit), value, timestamp);
        }
    }

    @ConstantSpecialization(arguments = 0)
    public static final class ConstantUnit
    {
        private final DateTimeField field;

        public ConstantUnit(@ConstantArgument(0) Slice unit)
        {
            field = DateTimeFunctions.getTimestampField(ISOChronology.getInstanceUTC(), unit);
        }

        @LiteralParameters("p")
        @SqlType("timestamp(p)")
        public long add(@LiteralParameter("p") long precision, @SqlType(StandardTypes.BIGINT) long value, @SqlType("timestamp(p)") long timestamp)
        {
            return DateAdd.add(precision, field, value, timestamp);
        }

        @LiteralParameters("p")
        @SqlType("timestamp(p)")
        public LongTimestamp add(@SqlType(StandardTypes.BIGINT) long value, @SqlType("timestamp(p)") LongTimestamp timestamp)
        {
            return DateAdd.add(field, value, timestamp);
        }
    }
}
