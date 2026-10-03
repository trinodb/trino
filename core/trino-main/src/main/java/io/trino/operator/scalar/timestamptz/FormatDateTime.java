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
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.type.DateTimes.roundToEpochMillis;
import static io.trino.util.DateTimeZoneIndex.getChronology;
import static io.trino.util.DateTimeZoneIndex.unpackChronology;

@Description("Formats the given time by the given format")
@ScalarFunction("format_datetime")
public final class FormatDateTime
{
    private FormatDateTime() {}

    private static Slice format(ConnectorSession session, long epochMillis, ISOChronology chronology, DateTimeFormatter formatter)
    {
        try {
            return utf8Slice(formatter.withChronology(chronology)
                    .withLocale(session.getLocale())
                    .print(epochMillis));
        }
        catch (Exception e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }

    private static DateTimeFormatter createFormatter(Slice formatString)
    {
        try {
            return DateTimeFormat.forPattern(formatString.toStringUtf8());
        }
        catch (Exception e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }

    @ScalarFunctionImplementationChoice
    public static final class Row
    {
        private Row() {}

        @LiteralParameters({"x", "p"})
        @SqlType(StandardTypes.VARCHAR)
        public static Slice format(ConnectorSession session, @SqlType("timestamp(p) with time zone") long packedEpochMillis, @SqlType("varchar(x)") Slice formatString)
        {
            return FormatDateTime.format(session, unpackMillisUtc(packedEpochMillis), unpackChronology(packedEpochMillis), createFormatter(formatString));
        }

        @LiteralParameters({"x", "p"})
        @SqlType(StandardTypes.VARCHAR)
        public static Slice format(ConnectorSession session, @SqlType("timestamp(p) with time zone") LongTimestampWithTimeZone timestamp, @SqlType("varchar(x)") Slice formatString)
        {
            return FormatDateTime.format(session, roundToEpochMillis(timestamp), getChronology(getTimeZoneKey(timestamp.getTimeZoneKey())), createFormatter(formatString));
        }
    }

    @ConstantSpecialization(arguments = 1)
    public static final class ConstantFormat
    {
        private final DateTimeFormatter formatter;

        public ConstantFormat(@ConstantArgument(1) Slice formatString)
        {
            formatter = createFormatter(formatString);
        }

        @LiteralParameters("p")
        @SqlType(StandardTypes.VARCHAR)
        public Slice format(ConnectorSession session, @SqlType("timestamp(p) with time zone") long packedEpochMillis)
        {
            return FormatDateTime.format(session, unpackMillisUtc(packedEpochMillis), unpackChronology(packedEpochMillis), formatter);
        }

        @LiteralParameters("p")
        @SqlType(StandardTypes.VARCHAR)
        public Slice format(ConnectorSession session, @SqlType("timestamp(p) with time zone") LongTimestampWithTimeZone timestamp)
        {
            return FormatDateTime.format(session, roundToEpochMillis(timestamp), getChronology(getTimeZoneKey(timestamp.getTimeZoneKey())), formatter);
        }
    }
}
