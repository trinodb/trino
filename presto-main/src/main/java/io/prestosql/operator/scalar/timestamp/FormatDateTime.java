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
package io.prestosql.operator.scalar.timestamp;

import io.airlift.slice.Slice;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.LiteralParameter;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.LongTimestamp;
import io.prestosql.spi.type.StandardTypes;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormat;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.type.DateTimes.round;
import static io.prestosql.type.DateTimes.scaleEpochMicrosToMillis;

@Description("Formats the given time by the given format")
@ScalarFunction("format_datetime")
public class FormatDateTime
{
    private FormatDateTime() {}

    @LiteralParameters({"x", "p"})
    @SqlType(StandardTypes.VARCHAR)
    public static Slice format(ConnectorSession session, @SqlType("timestamp(p)") long timestamp, @SqlType("varchar(x)") Slice formatString)
    {
        // TODO: currently, date formatting only supports up to millis, so we round to that unit
        timestamp = scaleEpochMicrosToMillis(round(timestamp, 3));

        if (datetimeFormatSpecifiesZone(formatString)) {
            // Timezone is unknown for TIMESTAMP w/o TZ so it cannot be printed out.
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "format_datetime for TIMESTAMP type, cannot use 'Z' nor 'z' in format, as this type does not contain TZ information");
        }
        ISOChronology chronology = ISOChronology.getInstanceUTC();

        try {
            return utf8Slice(DateTimeFormat.forPattern(formatString.toStringUtf8())
                    .withChronology(chronology)
                    .withLocale(session.getLocale())
                    .print(timestamp));
        }
        catch (Exception e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }

    @LiteralParameters({"x", "p"})
    @SqlType(StandardTypes.VARCHAR)
    public static Slice formatDatetime(@LiteralParameter("p") long precision, ConnectorSession session, @SqlType("timestamp(p)") LongTimestamp timestamp, @SqlType("varchar(x)") Slice formatString)
    {
        // Currently, date formatting only supports up to millis, so anything in the microsecond fraction is irrelevant
        return format(session, timestamp.getEpochMicros(), formatString);
    }

    /**
     * Checks whether {@link DateTimeFormat} pattern contains time zone-related field.
     */
    private static boolean datetimeFormatSpecifiesZone(Slice formatString)
    {
        boolean quoted = false;
        for (char c : formatString.toStringUtf8().toCharArray()) {
            if (quoted) {
                if (c == '\'') {
                    quoted = false;
                }
                continue;
            }

            switch (c) {
                case 'z':
                case 'Z':
                    return true;
                case '\'':
                    // '' (two apostrophes) in a pattern denote single apostrophe and here we interpret this as "start quote" + "end quote".
                    // This has no impact on method's result value.
                    quoted = true;
                    break;
            }
        }
        return false;
    }
}
