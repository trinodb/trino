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

import com.google.common.base.Splitter;
import io.trino.client.IntervalDayTime;
import io.trino.client.IntervalYearMonth;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.spi.TrinoException;
import io.trino.spi.type.DateTimeEncoding;
import io.trino.spi.type.LongTimeWithTimeZone;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import java.util.function.LongFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static java.lang.Math.toIntExact;

final class TemporalTransportCodec
{
    private static final Pattern UNSIGNED_EXTENDED_DATE_PATTERN = Pattern.compile("\\d{5,}-\\d{2}-\\d{2}");
    private static final Pattern TIME_WITH_TIME_ZONE_PATTERN = Pattern.compile("(?<time>\\d{2}:\\d{2}:\\d{2}(?:\\.\\d{1,12})?)\\s*(?<offset>[+-]\\d{2}:\\d{2})");
    private static final Pattern TIMESTAMP_PATTERN = Pattern.compile("(?<date>[+-]?\\d{4,}-\\d{2}-\\d{2}) (?<time>\\d{2}:\\d{2}:\\d{2}(?:\\.\\d{1,12})?)");
    private static final long PICOSECONDS_PER_SECOND = 1_000_000_000_000L;
    private static final long PICOSECONDS_PER_MINUTE = 60 * PICOSECONDS_PER_SECOND;
    private static final long PICOSECONDS_PER_HOUR = 60 * PICOSECONDS_PER_MINUTE;
    private static final long PICOSECONDS_PER_DAY = 24 * PICOSECONDS_PER_HOUR;

    private TemporalTransportCodec() {}

    static LocalDate parseDate(String value)
    {
        String normalized = value.trim();
        if (UNSIGNED_EXTENDED_DATE_PATTERN.matcher(normalized).matches()) {
            normalized = "+" + normalized;
        }
        return LocalDate.parse(normalized);
    }

    static long parseTimeToPicos(String value)
    {
        return parseClockTime(value).picosOfDay();
    }

    static long parseShortTimestamp(String value)
    {
        return parseTimestampValue(value).epochMicros();
    }

    static LongTimestamp parseLongTimestamp(String value)
    {
        ParsedTimestampValue parsedTimestamp = parseTimestampValue(value);
        return new LongTimestamp(parsedTimestamp.epochMicros(), parsedTimestamp.picosOfMicro());
    }

    static long parseShortTimeWithTimeZone(String value)
    {
        ParsedTimeWithTimeZoneValue parsedTime = parseTimeWithTimeZoneValue(value);
        return DateTimeEncoding.packTimeWithTimeZone(parsedTime.picosOfDay() / 1_000L, parsedTime.offsetMinutes());
    }

    static LongTimeWithTimeZone parseLongTimeWithTimeZone(String value)
    {
        ParsedTimeWithTimeZoneValue parsedTime = parseTimeWithTimeZoneValue(value);
        return new LongTimeWithTimeZone(parsedTime.picosOfDay(), parsedTime.offsetMinutes());
    }

    static long parseIntervalValue(String value, Type type)
    {
        if (TrinoTypeClassifier.isIntervalYearToMonthType(type)) {
            return IntervalYearMonth.parseMonths(value);
        }
        return IntervalDayTime.parseMillis(value);
    }

    static WriteMapping timestampWriteMapping(TimestampType type)
    {
        String bindType = "timestamp(" + type.getPrecision() + ")";
        if (type.isShort()) {
            return WriteMapping.longMapping(bindType, shortTimestampTransportWriteFunction(type));
        }
        return WriteMapping.objectMapping(bindType, longTimestampTransportWriteFunction(type));
    }

    static WriteMapping timeWithTimeZoneWriteMapping(TimeWithTimeZoneType type)
    {
        String bindType = "time(" + type.getPrecision() + ") with time zone";
        if (type.isShort()) {
            return WriteMapping.longMapping(bindType, shortTimeWithTimeZoneTransportWriteFunction(type));
        }
        return WriteMapping.objectMapping(bindType, longTimeWithTimeZoneTransportWriteFunction(type));
    }

    static WriteMapping intervalWriteMapping(Type type)
    {
        String bindType = TrinoTypeClassifier.isIntervalYearToMonthType(type) ? "interval year to month" : "interval day to second";
        return WriteMapping.longMapping(bindType, intervalTransportWriteFunction(type));
    }

    static LongWriteFunction timeTransportWriteFunction(TimeType type)
    {
        String bindType = "time(" + type.getPrecision() + ")";
        return stringLongTransportWriteFunction(bindType, value -> formatTimeValue(value, type.getPrecision()));
    }

    static LongWriteFunction shortTimestampTransportWriteFunction(TimestampType type)
    {
        String bindType = "timestamp(" + type.getPrecision() + ")";
        return stringLongTransportWriteFunction(bindType, value -> formatShortTimestampValue(value, type));
    }

    static ObjectWriteFunction longTimestampTransportWriteFunction(TimestampType type)
    {
        String bindType = "timestamp(" + type.getPrecision() + ")";
        return stringObjectTransportWriteFunction(LongTimestamp.class, bindType, value -> formatLongTimestampValue(value, type));
    }

    static LongWriteFunction shortTimeWithTimeZoneTransportWriteFunction(TimeWithTimeZoneType type)
    {
        String bindType = "time(" + type.getPrecision() + ") with time zone";
        return stringLongTransportWriteFunction(bindType, value -> formatShortTimeWithTimeZoneValue(value, type));
    }

    static ObjectWriteFunction longTimeWithTimeZoneTransportWriteFunction(TimeWithTimeZoneType type)
    {
        String bindType = "time(" + type.getPrecision() + ") with time zone";
        return stringObjectTransportWriteFunction(LongTimeWithTimeZone.class, bindType, value -> formatLongTimeWithTimeZoneValue(value, type));
    }

    static LongWriteFunction intervalTransportWriteFunction(Type type)
    {
        return new LongWriteFunction()
        {
            @Override
            public String getBindExpression()
            {
                return intervalBindExpression(type);
            }

            @Override
            public void set(PreparedStatement statement, int index, long value)
                    throws SQLException
            {
                if (TrinoTypeClassifier.isIntervalYearToMonthType(type)) {
                    statement.setInt(index, toIntExact(value));
                    return;
                }
                statement.setLong(index, value);
            }
        };
    }

    private static String intervalBindExpression(Type type)
    {
        if (TrinoTypeClassifier.isIntervalYearToMonthType(type)) {
            return "INTERVAL '1' MONTH * CAST(? AS INTEGER)";
        }
        return "INTERVAL '0.001' SECOND * CAST(? AS BIGINT)";
    }

    private static LongWriteFunction stringLongTransportWriteFunction(String bindType, LongFunction<String> formatter)
    {
        return new LongWriteFunction()
        {
            @Override
            public String getBindExpression()
            {
                return castBindExpression(bindType);
            }

            @Override
            public void set(PreparedStatement statement, int index, long value)
                    throws SQLException
            {
                statement.setString(index, formatter.apply(value));
            }
        };
    }

    private static <T> ObjectWriteFunction stringObjectTransportWriteFunction(Class<T> javaType, String bindType, Function<T, String> formatter)
    {
        return new ObjectWriteFunction()
        {
            @Override
            public Class<?> getJavaType()
            {
                return javaType;
            }

            @Override
            public String getBindExpression()
            {
                return castBindExpression(bindType);
            }

            @Override
            public void set(PreparedStatement statement, int index, Object value)
                    throws SQLException
            {
                statement.setString(index, formatter.apply(javaType.cast(value)));
            }
        };
    }

    private static String castBindExpression(String bindType)
    {
        return "CAST(? AS " + bindType + ")";
    }

    private static String formatShortTimestampValue(long value, TimestampType type)
    {
        return formatTimestampValue(value, 0, type.getPrecision());
    }

    private static String formatLongTimestampValue(LongTimestamp value, TimestampType type)
    {
        return formatTimestampValue(value.getEpochMicros(), value.getPicosOfMicro(), type.getPrecision());
    }

    private static String formatTimestampValue(long epochMicros, int picosOfMicro, int precision)
    {
        long epochSecond = Math.floorDiv(epochMicros, 1_000_000L);
        long microsOfSecond = Math.floorMod(epochMicros, 1_000_000L);
        LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(epochSecond, microsOfSecond * 1_000L), ZoneOffset.UTC);
        long picoFraction = microsOfSecond * 1_000_000L + picosOfMicro;
        return formatDateTimeValue(dateTime, picoFraction, precision);
    }

    private static String formatShortTimeWithTimeZoneValue(long value, TimeWithTimeZoneType type)
    {
        long picosOfDay = DateTimeEncoding.unpackTimeNanos(value) * 1_000L;
        int offsetMinutes = DateTimeEncoding.unpackOffsetMinutes(value);
        return formatTimeWithTimeZoneValue(picosOfDay, offsetMinutes, type.getPrecision());
    }

    private static String formatLongTimeWithTimeZoneValue(LongTimeWithTimeZone value, TimeWithTimeZoneType type)
    {
        return formatTimeWithTimeZoneValue(value.getPicoseconds(), value.getOffsetMinutes(), type.getPrecision());
    }

    private static String formatTimeWithTimeZoneValue(long picosOfDay, int offsetMinutes, int precision)
    {
        return formatTimeValue(picosOfDay, precision) + formatOffset(offsetMinutes);
    }

    private static String formatDateTimeValue(LocalDateTime dateTime, long picoFraction, int precision)
    {
        String value = String.format(
                Locale.ROOT,
                "%s %02d:%02d:%02d",
                dateTime.toLocalDate(),
                dateTime.getHour(),
                dateTime.getMinute(),
                dateTime.getSecond());
        return value + formatFraction(picoFraction, precision);
    }

    private static String formatTimeValue(long picosOfDay, int precision)
    {
        long normalized = Math.floorMod(picosOfDay, PICOSECONDS_PER_DAY);
        long hours = normalized / PICOSECONDS_PER_HOUR;
        normalized %= PICOSECONDS_PER_HOUR;
        long minutes = normalized / PICOSECONDS_PER_MINUTE;
        normalized %= PICOSECONDS_PER_MINUTE;
        long seconds = normalized / PICOSECONDS_PER_SECOND;
        long picoFraction = normalized % PICOSECONDS_PER_SECOND;
        return String.format(Locale.ROOT, "%02d:%02d:%02d", hours, minutes, seconds) + formatFraction(picoFraction, precision);
    }

    private static String formatFraction(long picoFraction, int precision)
    {
        if (precision == 0) {
            return "";
        }
        return "." + String.format(Locale.ROOT, "%012d", picoFraction).substring(0, precision);
    }

    private static String formatOffset(int offsetMinutes)
    {
        int absOffsetMinutes = Math.abs(offsetMinutes);
        int hours = absOffsetMinutes / 60;
        int minutes = absOffsetMinutes % 60;
        return String.format(Locale.ROOT, "%s%02d:%02d", offsetMinutes >= 0 ? "+" : "-", hours, minutes);
    }

    private record ParsedClockTime(long picosOfDay, long fractionPicosOfSecond) {}

    private record ParsedTimeWithTimeZoneValue(long picosOfDay, int offsetMinutes) {}

    private record ParsedTimestampValue(long epochMicros, int picosOfMicro) {}

    private static ParsedClockTime parseClockTime(String value)
    {
        List<String> parts = Splitter.on(':').splitToList(value.trim());
        if (parts.size() != 3) {
            throw new TrinoException(JDBC_ERROR, "Invalid time value: " + value);
        }
        int hour = Integer.parseInt(parts.get(0));
        int minute = Integer.parseInt(parts.get(1));
        String[] secondsAndFraction = parts.get(2).split("\\.", 2);
        int second = Integer.parseInt(secondsAndFraction[0]);
        long fractionPicos = secondsAndFraction.length == 2 ? parseFractionToPicos(secondsAndFraction[1]) : 0;
        long picosOfDay = (((hour * 60L) + minute) * 60L + second) * PICOSECONDS_PER_SECOND + fractionPicos;
        return new ParsedClockTime(picosOfDay, fractionPicos);
    }

    private static ParsedTimeWithTimeZoneValue parseTimeWithTimeZoneValue(String value)
    {
        Matcher matcher = TIME_WITH_TIME_ZONE_PATTERN.matcher(value.trim());
        if (!matcher.matches()) {
            throw new TrinoException(JDBC_ERROR, "Invalid time with time zone value: " + value);
        }
        ParsedClockTime parsedClockTime = parseClockTime(matcher.group("time"));
        int offsetMinutes = ZoneOffset.of(matcher.group("offset")).getTotalSeconds() / 60;
        return new ParsedTimeWithTimeZoneValue(parsedClockTime.picosOfDay(), offsetMinutes);
    }

    private static ParsedTimestampValue parseTimestampValue(String value)
    {
        Matcher matcher = TIMESTAMP_PATTERN.matcher(value.trim());
        if (!matcher.matches()) {
            throw new TrinoException(JDBC_ERROR, "Invalid timestamp value: " + value);
        }
        LocalDate date = parseDate(matcher.group("date"));
        ParsedClockTime parsedClockTime = parseClockTime(matcher.group("time"));
        LocalDateTime localDateTime = LocalDateTime.of(date, LocalTime.ofNanoOfDay(parsedClockTime.picosOfDay() / 1_000L));
        long epochSecond = localDateTime.toEpochSecond(ZoneOffset.UTC);
        long epochMicros = epochSecond * 1_000_000L + parsedClockTime.fractionPicosOfSecond() / 1_000_000L;
        int picosOfMicro = (int) (parsedClockTime.fractionPicosOfSecond() % 1_000_000L);
        return new ParsedTimestampValue(epochMicros, picosOfMicro);
    }

    private static long parseFractionToPicos(String fraction)
    {
        String normalized = fraction.length() >= 12
                ? fraction.substring(0, 12)
                : (fraction + "000000000000").substring(0, 12);
        return Long.parseLong(normalized);
    }
}
