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
package io.trino.plugin.trino;

import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.type.DateTimeEncoding;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampWithTimeZoneType;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static java.util.Objects.requireNonNull;

final class TimestampWithTimeZoneTransport
{
    private static final Pattern TRANSPORT_VALUE_PATTERN = Pattern.compile(
            "(?<date>[+-]?\\d{4,}-\\d{2}-\\d{2})T" +
                    "(?<time>\\d{2}:\\d{2}:\\d{2})" +
                    "(?:\\.(?<fraction>\\d{1,12}))?" +
                    "(?<offset>Z|[+-]\\d{2}:\\d{2}(?::\\d{2})?)\\|" +
                    "(?<zone>.+)");
    private static final long PICOSECONDS_PER_MILLISECOND = 1_000_000_000L;

    private TimestampWithTimeZoneTransport() {}

    static String readExpression(String expression)
    {
        requireNonNull(expression, "expression is null");
        // Trino's ISO formatter rejects historical zones with second-level offsets.
        // Encode the instant in UTC and carry the original zone ID separately.
        return "to_iso8601(at_timezone(" + expression + ", 'UTC')) || '|' || format_datetime(" + expression + ", 'ZZZ')";
    }

    static long parseShortTimestampWithTimeZone(String value)
    {
        ParsedValue parsed = parse(value);
        return DateTimeEncoding.packDateTimeWithZone(parsed.epochMillis(), parsed.timeZoneKey());
    }

    static LongTimestampWithTimeZone parseLongTimestampWithTimeZone(String value)
    {
        ParsedValue parsed = parse(value);
        return LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                parsed.epochMillis(),
                parsed.picosOfMilli(),
                parsed.timeZoneKey());
    }

    static LongWriteFunction shortPredicateWriteFunction(TimestampWithTimeZoneType type)
    {
        String bindExpression = bindExpression(type);
        return new LongWriteFunction()
        {
            @Override
            public String getBindExpression()
            {
                return bindExpression;
            }

            @Override
            public void set(PreparedStatement statement, int index, long value)
                    throws SQLException
            {
                statement.setString(index, formatShortParameterValue(value, type));
            }
        };
    }

    static ObjectWriteFunction longPredicateWriteFunction(TimestampWithTimeZoneType type)
    {
        String bindExpression = bindExpression(type);
        return new ObjectWriteFunction()
        {
            @Override
            public Class<?> getJavaType()
            {
                return LongTimestampWithTimeZone.class;
            }

            @Override
            public String getBindExpression()
            {
                return bindExpression;
            }

            @Override
            public void set(PreparedStatement statement, int index, Object value)
                    throws SQLException
            {
                statement.setString(index, formatLongParameterValue((LongTimestampWithTimeZone) value, type));
            }
        };
    }

    static String formatShortParameterValue(long value, TimestampWithTimeZoneType type)
    {
        return formatParameterValue(
                DateTimeEncoding.unpackMillisUtc(value),
                0,
                type.getPrecision());
    }

    static String formatLongParameterValue(LongTimestampWithTimeZone value, TimestampWithTimeZoneType type)
    {
        requireNonNull(value, "value is null");
        return formatParameterValue(
                value.getEpochMillis(),
                value.getPicosOfMilli(),
                type.getPrecision());
    }

    static String shortZoneId(long value)
    {
        return DateTimeEncoding.unpackZoneKey(value).getId();
    }

    static String longZoneId(LongTimestampWithTimeZone value)
    {
        requireNonNull(value, "value is null");
        return TimeZoneKey.getTimeZoneKey(value.getTimeZoneKey()).getId();
    }

    private static ParsedValue parse(String value)
    {
        requireNonNull(value, "value is null");
        Matcher matcher = TRANSPORT_VALUE_PATTERN.matcher(value.trim());
        if (!matcher.matches()) {
            throw new TrinoException(JDBC_ERROR, "Invalid timestamp with time zone transport value: " + value);
        }

        LocalDate date = TemporalTransportCodec.parseDate(matcher.group("date"));
        LocalTime time = LocalTime.parse(matcher.group("time"));
        ZoneOffset offset = ZoneOffset.of(matcher.group("offset"));
        long fractionPicos = parseFractionToPicos(matcher.group("fraction"));
        long epochSecond = LocalDateTime.of(date, time).toEpochSecond(offset);
        long epochMillis = Math.addExact(
                Math.multiplyExact(epochSecond, 1_000L),
                fractionPicos / PICOSECONDS_PER_MILLISECOND);
        int picosOfMilli = (int) (fractionPicos % PICOSECONDS_PER_MILLISECOND);
        return new ParsedValue(
                epochMillis,
                picosOfMilli,
                TimeZoneKey.getTimeZoneKey(matcher.group("zone")));
    }

    private static String bindExpression(TimestampWithTimeZoneType type)
    {
        return "CAST(? AS timestamp(" + type.getPrecision() + ") with time zone)";
    }

    private static String formatParameterValue(long epochMillis, int picosOfMilli, int precision)
    {
        // Predicates compare instants, so UTC avoids both DST ambiguity and historical
        // second-level offsets. Renderer constants restore the original zone separately.
        LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneOffset.UTC);
        long fractionPicos = dateTime.getNano() * 1_000L + picosOfMilli;
        String value = String.format(
                Locale.ROOT,
                "%s %02d:%02d:%02d",
                dateTime.toLocalDate(),
                dateTime.getHour(),
                dateTime.getMinute(),
                dateTime.getSecond());
        if (precision > 0) {
            value += "." + String.format(Locale.ROOT, "%012d", fractionPicos).substring(0, precision);
        }
        return value + " +00:00";
    }

    private static long parseFractionToPicos(String fraction)
    {
        if (fraction == null) {
            return 0;
        }
        return Long.parseLong((fraction + "000000000000").substring(0, 12));
    }

    private record ParsedValue(long epochMillis, int picosOfMilli, TimeZoneKey timeZoneKey) {}
}
