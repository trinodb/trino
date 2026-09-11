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
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.ConstantArgument;
import io.trino.spi.function.ConstantSpecialization;
import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.ScalarFunctionImplementationChoice;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.StandardTypes;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormatter;

import static io.trino.operator.scalar.DateTimeFunctions.createDateTimeFormatter;
import static io.trino.operator.scalar.DateTimeFunctions.dateFormat;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.type.DateTimes.roundToEpochMillis;
import static io.trino.util.DateTimeZoneIndex.getChronology;
import static io.trino.util.DateTimeZoneIndex.unpackChronology;

@ScalarFunction
@Description("Formats the given timestamp by the given format")
public final class DateFormat
{
    private DateFormat() {}

    private static Slice format(ConnectorSession session, long epochMillis, ISOChronology chronology, Slice formatString)
    {
        return dateFormat(chronology, session.getLocale(), epochMillis, formatString);
    }

    private static Slice format(ConnectorSession session, long epochMillis, ISOChronology chronology, DateTimeFormatter formatter)
    {
        return dateFormat(chronology, session.getLocale(), epochMillis, formatter);
    }

    @ScalarFunctionImplementationChoice
    public static final class Row
    {
        private Row() {}

        @LiteralParameters({"x", "p"})
        @SqlType(StandardTypes.VARCHAR)
        public static Slice format(ConnectorSession session, @SqlType("timestamp(p) with time zone") long packedEpochMillis, @SqlType("varchar(x)") Slice formatString)
        {
            return DateFormat.format(session, unpackMillisUtc(packedEpochMillis), unpackChronology(packedEpochMillis), formatString);
        }

        @LiteralParameters({"x", "p"})
        @SqlType(StandardTypes.VARCHAR)
        public static Slice format(ConnectorSession session, @SqlType("timestamp(p) with time zone") LongTimestampWithTimeZone timestamp, @SqlType("varchar(x)") Slice formatString)
        {
            return DateFormat.format(session, roundToEpochMillis(timestamp), getChronology(getTimeZoneKey(timestamp.getTimeZoneKey())), formatString);
        }
    }

    @ConstantSpecialization(arguments = 1)
    public static final class ConstantFormat
    {
        private final DateTimeFormatter formatter;

        public ConstantFormat(@ConstantArgument(1) Slice formatString)
        {
            formatter = createDateTimeFormatter(formatString);
        }

        @LiteralParameters("p")
        @SqlType(StandardTypes.VARCHAR)
        public Slice format(ConnectorSession session, @SqlType("timestamp(p) with time zone") long packedEpochMillis)
        {
            return DateFormat.format(session, unpackMillisUtc(packedEpochMillis), unpackChronology(packedEpochMillis), formatter);
        }

        @LiteralParameters("p")
        @SqlType(StandardTypes.VARCHAR)
        public Slice format(ConnectorSession session, @SqlType("timestamp(p) with time zone") LongTimestampWithTimeZone timestamp)
        {
            return DateFormat.format(session, roundToEpochMillis(timestamp), getChronology(getTimeZoneKey(timestamp.getTimeZoneKey())), formatter);
        }
    }
}
