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
package io.trino.plugin.teradata.functions;

import io.airlift.concurrent.ThreadLocalCache;
import io.airlift.slice.Slice;
import io.trino.plugin.teradata.functions.dateformat.DateFormatParser.Formatter;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.StandardTypes;

import java.time.DateTimeException;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.teradata.functions.dateformat.DateFormatParser.createDateTimeFormatter;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class TeradataDateFunctions
{
    private static final ThreadLocalCache<Slice, Formatter> DATETIME_FORMATTER_CACHE =
            new ThreadLocalCache<>(100, format -> createDateTimeFormatter(format.toStringUtf8()));

    private TeradataDateFunctions() {}

    @Description("Converts a string to a DATE data type")
    @ScalarFunction("to_date")
    @SqlType(StandardTypes.DATE)
    public static long toDate(ConnectorSession session, @SqlType(StandardTypes.VARCHAR) Slice dateTime, @SqlType(StandardTypes.VARCHAR) Slice formatString)
    {
        try {
            long millis = parseMillis(dateTime, formatString);
            return MILLISECONDS.toDays(millis);
        }
        catch (Throwable t) {
            throwIfInstanceOf(t, Error.class);
            throwIfInstanceOf(t, TrinoException.class);
            throw new TrinoException(GENERIC_INTERNAL_ERROR, t);
        }
    }

    @Description("Converts a string to a TIMESTAMP data type")
    @ScalarFunction("to_timestamp")
    @SqlType("timestamp(3)")
    public static long toTimestamp(
            ConnectorSession session,
            @SqlType(StandardTypes.VARCHAR) Slice dateTime,
            @SqlType(StandardTypes.VARCHAR) Slice formatString)
    {
        return parseMillis(dateTime, formatString) * MICROSECONDS_PER_MILLISECOND;
    }

    private static long parseMillis(Slice dateTime, Slice formatString)
    {
        Formatter formatter = DATETIME_FORMATTER_CACHE.get(formatString);

        try {
            return formatter.parseMillis(dateTime.toString(UTF_8));
        }
        catch (DateTimeException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }

    @Description("Formats a timestamp")
    @ScalarFunction("to_char")
    public static class ToChar
    {
        @LiteralParameters("p")
        @SqlType(StandardTypes.VARCHAR)
        public static Slice toChar(
                ConnectorSession session,
                @SqlType("timestamp(p) with time zone") long timestampWithTimeZone,
                @SqlType(StandardTypes.VARCHAR) Slice formatString)
        {
            Formatter formatter = DATETIME_FORMATTER_CACHE.get(formatString);
            return utf8Slice(formatter.format(unpackMillisUtc(timestampWithTimeZone), unpackZoneKey(timestampWithTimeZone).getZoneId(), session.getLocale()));
        }

        @LiteralParameters("p")
        @SqlType(StandardTypes.VARCHAR)
        public static Slice toChar(
                ConnectorSession session,
                @SqlType("timestamp(p) with time zone") LongTimestampWithTimeZone timestampWithTimeZone,
                @SqlType(StandardTypes.VARCHAR) Slice formatString)
        {
            Formatter formatter = DATETIME_FORMATTER_CACHE.get(formatString);
            return utf8Slice(formatter.format(timestampWithTimeZone.getEpochMillis(), getTimeZoneKey(timestampWithTimeZone.getTimeZoneKey()).getZoneId(), session.getLocale()));
        }
    }
}
