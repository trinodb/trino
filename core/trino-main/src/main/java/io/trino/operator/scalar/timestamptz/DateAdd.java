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
import io.trino.spi.TrinoException;
import io.trino.spi.function.ConstantArgument;
import io.trino.spi.function.ConstantSpecialization;
import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameter;
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
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.updateMillisUtc;
import static io.trino.spi.type.Timestamps.round;
import static io.trino.util.DateTimeZoneIndex.unpackChronology;

@Description("Add the specified amount of time to the given timestamp")
@ScalarFunction("date_add")
public final class DateAdd
{
    private DateAdd() {}

    private static long add(long precision, Function<ISOChronology, DateTimeField> fieldProvider, long value, long packedEpochMillis)
    {
        try {
            long epochMillis = unpackMillisUtc(packedEpochMillis);

            epochMillis = fieldProvider.apply(unpackChronology(packedEpochMillis)).add(epochMillis, value);
            epochMillis = round(epochMillis, (int) (3 - precision));

            return updateMillisUtc(epochMillis, packedEpochMillis);
        }
        catch (IllegalArgumentException | ArithmeticException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, e.getMessage());
        }
    }

    private static LongTimestampWithTimeZone add(Function<ISOChronology, DateTimeField> fieldProvider, long value, LongTimestampWithTimeZone timestamp)
    {
        try {
            long epochMillis = fieldProvider.apply(unpackChronology(timestamp.getTimeZoneKey())).add(timestamp.getEpochMillis(), value);

            return LongTimestampWithTimeZone.fromEpochMillisAndFraction(epochMillis, timestamp.getPicosOfMilli(), timestamp.getTimeZoneKey());
        }
        catch (IllegalArgumentException | ArithmeticException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, e.getMessage());
        }
    }

    @ScalarFunctionImplementationChoice
    public static final class Row
    {
        private Row() {}

        @LiteralParameters({"x", "p"})
        @SqlType("timestamp(p) with time zone")
        public static long add(@LiteralParameter("p") long precision, @SqlType("varchar(x)") Slice unit, @SqlType(StandardTypes.BIGINT) long value, @SqlType("timestamp(p) with time zone") long packedEpochMillis)
        {
            return DateAdd.add(precision, getTimestampFieldProvider(unit), value, packedEpochMillis);
        }

        @LiteralParameters({"x", "p"})
        @SqlType("timestamp(p) with time zone")
        public static LongTimestampWithTimeZone add(@SqlType("varchar(x)") Slice unit, @SqlType(StandardTypes.BIGINT) long value, @SqlType("timestamp(p) with time zone") LongTimestampWithTimeZone timestamp)
        {
            return DateAdd.add(getTimestampFieldProvider(unit), value, timestamp);
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
        public long add(@LiteralParameter("p") long precision, @SqlType(StandardTypes.BIGINT) long value, @SqlType("timestamp(p) with time zone") long packedEpochMillis)
        {
            return DateAdd.add(precision, fieldProvider, value, packedEpochMillis);
        }

        @LiteralParameters("p")
        @SqlType("timestamp(p) with time zone")
        public LongTimestampWithTimeZone add(@SqlType(StandardTypes.BIGINT) long value, @SqlType("timestamp(p) with time zone") LongTimestampWithTimeZone timestamp)
        {
            return DateAdd.add(fieldProvider, value, timestamp);
        }
    }
}
