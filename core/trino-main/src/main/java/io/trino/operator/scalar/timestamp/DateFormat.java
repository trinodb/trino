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
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.ConstantArgument;
import io.trino.spi.function.ConstantSpecialization;
import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.ScalarFunctionImplementationChoice;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.StandardTypes;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormatter;

import static io.trino.operator.scalar.DateTimeFunctions.createDateTimeFormatter;
import static io.trino.operator.scalar.DateTimeFunctions.dateFormat;
import static io.trino.spi.type.Timestamps.epochMicrosToMillisWithRounding;

@ScalarFunction
@Description("Formats the given timestamp by the given format")
public final class DateFormat
{
    private DateFormat() {}

    private static Slice format(ConnectorSession session, long timestamp, Slice formatString)
    {
        timestamp = epochMicrosToMillisWithRounding(timestamp);
        return dateFormat(ISOChronology.getInstanceUTC(), session.getLocale(), timestamp, formatString);
    }

    private static Slice format(ConnectorSession session, long timestamp, DateTimeFormatter formatter)
    {
        // TODO: currently, date formatting only supports up to millis, so round to that unit
        timestamp = epochMicrosToMillisWithRounding(timestamp);

        return dateFormat(ISOChronology.getInstanceUTC(), session.getLocale(), timestamp, formatter);
    }

    @ScalarFunctionImplementationChoice
    public static final class Row
    {
        private Row() {}

        @LiteralParameters({"x", "p"})
        @SqlType(StandardTypes.VARCHAR)
        public static Slice format(ConnectorSession session, @SqlType("timestamp(p)") long timestamp, @SqlType("varchar(x)") Slice formatString)
        {
            return DateFormat.format(session, timestamp, formatString);
        }

        @LiteralParameters({"x", "p"})
        @SqlType(StandardTypes.VARCHAR)
        public static Slice format(ConnectorSession session, @SqlType("timestamp(p)") LongTimestamp timestamp, @SqlType("varchar(x)") Slice formatString)
        {
            return DateFormat.format(session, timestamp.getEpochMicros(), formatString);
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
        public Slice format(ConnectorSession session, @SqlType("timestamp(p)") long timestamp)
        {
            return DateFormat.format(session, timestamp, formatter);
        }

        @LiteralParameters("p")
        @SqlType(StandardTypes.VARCHAR)
        public Slice format(ConnectorSession session, @SqlType("timestamp(p)") LongTimestamp timestamp)
        {
            return DateFormat.format(session, timestamp.getEpochMicros(), formatter);
        }
    }
}
