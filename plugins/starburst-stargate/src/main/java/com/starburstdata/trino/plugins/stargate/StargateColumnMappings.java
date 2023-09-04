/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.stargate;

import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.LongReadFunction;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.ObjectReadFunction;
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.spi.type.LongTimeWithTimeZone;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.SqlTime;
import io.trino.spi.type.SqlTimeWithTimeZone;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.SqlTimestampWithTimeZone;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import jakarta.annotation.Nullable;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateWriteFunctionUsingSqlDate;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.packTimeWithTimeZone;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackOffsetMinutes;
import static io.trino.spi.type.DateTimeEncoding.unpackTimeNanos;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.rescale;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimeWithTimeZoneType.createTimeWithTimeZoneType;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.SECONDS_PER_MINUTE;
import static java.lang.Integer.parseInt;
import static java.lang.Math.multiplyExact;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;

final class StargateColumnMappings
{
    private StargateColumnMappings() {}

    public static final Pattern TIME_PATTERN = Pattern.compile("(?<hour>\\d\\d):(?<minute>\\d\\d):(?<second>\\d\\d)(?:\\.(?<fraction>\\d{1,12}))?");
    public static final Pattern TIME_TZ_PATTERN = Pattern.compile("" +
            "(?<hour>\\d\\d):(?<minute>\\d\\d):(?<second>\\d\\d)(?:\\.(?<fraction>\\d{1,12}))?\\b" +
            "\\s*(?<zoneOffset>.+)");
    public static final Pattern TIMESTAMP_PATTERN = Pattern.compile("" +
            "(?<year>[-+]?\\d{4,})-(?<month>\\d\\d)-(?<day>\\d\\d) " +
            "(?<hour>\\d\\d):(?<minute>\\d\\d):(?<second>\\d\\d)(?:\\.(?<fraction>\\d{1,12}))?");
    public static final Pattern TIMESTAMP_TZ_PATTERN = Pattern.compile("" +
            "(?<year>[-+]?\\d{4,})-(?<month>\\d\\d)-(?<day>\\d\\d) " +
            "(?<hour>\\d\\d):(?<minute>\\d\\d):(?<second>\\d\\d)(?:\\.(?<fraction>\\d{1,12}))?\\b" +
            "\\s*(?<timezone>.+)");

    public static ColumnMapping stargateDateColumnMapping()
    {
        return ColumnMapping.longMapping(
                DATE,
                new LongReadFunction()
                {
                    @Override
                    public boolean isNull(ResultSet resultSet, int columnIndex)
                            throws SQLException
                    {
                        // resultSet.getObject fails for certain dates
                        // TODO simplify this read function once https://github.com/trinodb/trino/issues/6242 is fixed
                        resultSet.getString(columnIndex);
                        return resultSet.wasNull();
                    }

                    @Override
                    public long readLong(ResultSet resultSet, int columnIndex)
                            throws SQLException
                    {
                        LocalDate localDate = LocalDate.parse(resultSet.getString(columnIndex));
                        return localDate.toEpochDay();
                    }
                },
                dateWriteFunctionUsingSqlDate());
    }

    public static ColumnMapping stargateTimeColumnMapping(int decimalDigits)
    {
        int precision = decimalDigits;
        return ColumnMapping.longMapping(
                createTimeType(precision),
                timeReadFunction(),
                timeWriteFunction(precision));
    }

    public static WriteMapping stargateTimeWriteMapping(TimeType timeType)
    {
        int precision = timeType.getPrecision();
        return WriteMapping.longMapping(format("time(%s)", precision), timeWriteFunction(precision));
    }

    private static LongReadFunction timeReadFunction()
    {
        return TimeReadFunction.INSTANCE;
    }

    private enum TimeReadFunction
            implements LongReadFunction
    {
        INSTANCE;

        @Override
        public boolean isNull(ResultSet resultSet, int columnIndex)
                throws SQLException
        {
            // resultSet.getObject fails for TIME with precision > 9
            resultSet.getString(columnIndex);
            return resultSet.wasNull();
        }

        @Override
        public long readLong(ResultSet resultSet, int columnIndex)
                throws SQLException
        {
            String value = resultSet.getString(columnIndex);
            Matcher matcher = TIME_PATTERN.matcher(value);
            if (!matcher.matches()) {
                throw new IllegalArgumentException("Invalid time: " + value);
            }

            int hour = parseInt(matcher.group("hour"));
            int minute = parseInt(matcher.group("minute"));
            int second = parseInt(matcher.group("second"));
            @Nullable
            String fraction = matcher.group("fraction");

            return makePicosOfDay(value, hour, minute, second, fraction);
        }
    }

    private static LongWriteFunction timeWriteFunction(int precision)
    {
        return (statement, index, value) -> {
            String formatted = SqlTime.newInstance(precision, value).toString();
            statement.setObject(index, formatted, Types.TIME);
        };
    }

    public static ColumnMapping stargateTimeWithTimeZoneColumnMapping(int decimalDigits)
    {
        int precision = decimalDigits;
        TimeWithTimeZoneType timeWithTimeZoneType = createTimeWithTimeZoneType(precision);
        if (timeWithTimeZoneType.isShort()) {
            return ColumnMapping.longMapping(
                    timeWithTimeZoneType,
                    shortTimeWithTimeZoneReadFunction(),
                    shortTimeWithTimeZoneWriteFunction(precision));
        }
        return ColumnMapping.objectMapping(
                timeWithTimeZoneType,
                longTimeWithTimeZoneReadFunction(),
                longTimeWithTimeZoneWriteFunction(precision));
    }

    private static LongReadFunction shortTimeWithTimeZoneReadFunction()
    {
        return (resultSet, columnIndex) -> {
            String value = resultSet.getString(columnIndex);
            LongTimeWithTimeZone longTimeWithTimeZone = parseTimeWithTimeZone(value);
            verify(longTimeWithTimeZone.getPicoseconds() % PICOSECONDS_PER_NANOSECOND == 0, "Unexpected sub-nanosecond precision: %s", longTimeWithTimeZone);
            return packTimeWithTimeZone(longTimeWithTimeZone.getPicoseconds() / PICOSECONDS_PER_NANOSECOND, longTimeWithTimeZone.getOffsetMinutes());
        };
    }

    private static ObjectReadFunction longTimeWithTimeZoneReadFunction()
    {
        return ObjectReadFunction.of(LongTimeWithTimeZone.class, (resultSet, columnIndex) -> {
            String value = resultSet.getString(columnIndex);
            return parseTimeWithTimeZone(value);
        });
    }

    private static LongTimeWithTimeZone parseTimeWithTimeZone(String value)
    {
        Matcher matcher = TIME_TZ_PATTERN.matcher(value);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid time with time zone: " + value);
        }

        int hour = parseInt(matcher.group("hour"));
        int minute = parseInt(matcher.group("minute"));
        int second = parseInt(matcher.group("second"));
        @Nullable
        String fraction = matcher.group("fraction");
        String zoneOffset = matcher.group("zoneOffset");

        long picoseconds = makePicosOfDay(value, hour, minute, second, fraction);

        int offsetSeconds = ZoneOffset.of(zoneOffset).getTotalSeconds();
        verify(offsetSeconds % SECONDS_PER_MINUTE == 0, "Unsupported offset seconds: %s", value);
        int offsetMinutes = toIntExact(offsetSeconds / SECONDS_PER_MINUTE);

        return new LongTimeWithTimeZone(picoseconds, offsetMinutes);
    }

    public static WriteMapping stargateTimeWithTimeZoneWriteMapping(TimeWithTimeZoneType timeWithTimeZoneType)
    {
        int precision = timeWithTimeZoneType.getPrecision();
        String dataType = format("time(%s) with time zone", precision);
        if (timeWithTimeZoneType.isShort()) {
            return WriteMapping.longMapping(dataType, shortTimeWithTimeZoneWriteFunction(precision));
        }
        return WriteMapping.objectMapping(dataType, longTimeWithTimeZoneWriteFunction(precision));
    }

    private static LongWriteFunction shortTimeWithTimeZoneWriteFunction(int precision)
    {
        return (statement, index, value) -> {
            String formatted = SqlTimeWithTimeZone.newInstance(precision, unpackTimeNanos(value) * PICOSECONDS_PER_NANOSECOND, unpackOffsetMinutes(value)).toString();
            statement.setObject(index, formatted, Types.TIME_WITH_TIMEZONE);
        };
    }

    private static ObjectWriteFunction longTimeWithTimeZoneWriteFunction(int precision)
    {
        return ObjectWriteFunction.of(LongTimeWithTimeZone.class, (statement, index, value) -> {
            String formatted = SqlTimeWithTimeZone.newInstance(precision, value.getPicoseconds(), value.getOffsetMinutes()).toString();
            statement.setObject(index, formatted, Types.TIME_WITH_TIMEZONE);
        });
    }

    public static ColumnMapping stargateTimestampColumnMapping(int decimalDigits)
    {
        int precision = decimalDigits;
        TimestampType timestampType = createTimestampType(precision);
        if (timestampType.isShort()) {
            return ColumnMapping.longMapping(
                    timestampType,
                    shortTimestampReadFunction(),
                    shortTimestampWriteFunction(precision));
        }
        return ColumnMapping.objectMapping(
                timestampType,
                longTimestampReadFunction(),
                longTimestampWriteFunction(precision));
    }

    public static WriteMapping stargateTimestampWriteMapping(TimestampType timestampType)
    {
        int precision = timestampType.getPrecision();
        String dataType = format("timestamp(%s)", precision);
        if (timestampType.isShort()) {
            return WriteMapping.longMapping(dataType, shortTimestampWriteFunction(precision));
        }
        return WriteMapping.objectMapping(dataType, longTimestampWriteFunction(precision));
    }

    private static LongReadFunction shortTimestampReadFunction()
    {
        return (resultSet, columnIndex) -> {
            String value = resultSet.getString(columnIndex);
            LongTimestamp longTimestamp = parseTimestamp(value);
            verify(longTimestamp.getPicosOfMicro() == 0, "Unexpected sub-microsecond precision: %s", longTimestamp);
            return longTimestamp.getEpochMicros();
        };
    }

    private static ObjectReadFunction longTimestampReadFunction()
    {
        return ObjectReadFunction.of(LongTimestamp.class, (resultSet, columnIndex) -> {
            String value = resultSet.getString(columnIndex);
            return parseTimestamp(value);
        });
    }

    private static LongTimestamp parseTimestamp(String value)
    {
        Matcher matcher = TIMESTAMP_PATTERN.matcher(value);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid timestamp: " + value);
        }

        String year = matcher.group("year");
        String month = matcher.group("month");
        String day = matcher.group("day");
        String hour = matcher.group("hour");
        String minute = matcher.group("minute");
        String second = matcher.group("second");
        @Nullable
        String fraction = matcher.group("fraction");

        long epochSecond = LocalDateTime.of(parseInt(year), parseInt(month), parseInt(day), parseInt(hour), parseInt(minute), parseInt(second))
                .toEpochSecond(ZoneOffset.UTC);
        long picosOfSecond;
        if (fraction != null) {
            picosOfSecond = rescale(Long.parseLong(fraction), fraction.length(), 12);
        }
        else {
            picosOfSecond = 0;
        }

        return longTimestamp(epochSecond, picosOfSecond);
    }

    private static LongTimestamp longTimestamp(long epochSecond, long picosOfSecond)
    {
        return new LongTimestamp(
                multiplyExact(epochSecond, MICROSECONDS_PER_SECOND) + picosOfSecond / PICOSECONDS_PER_MICROSECOND,
                toIntExact(picosOfSecond % PICOSECONDS_PER_MICROSECOND));
    }

    private static LongWriteFunction shortTimestampWriteFunction(int precision)
    {
        return (statement, index, value) -> {
            String formatted = SqlTimestamp.newInstance(precision, value, 0).toString();
            statement.setObject(index, formatted, Types.TIMESTAMP);
        };
    }

    private static ObjectWriteFunction longTimestampWriteFunction(int precision)
    {
        return ObjectWriteFunction.of(LongTimestamp.class, (statement, index, value) -> {
            String formatted = SqlTimestamp.newInstance(precision, value.getEpochMicros(), value.getPicosOfMicro()).toString();
            statement.setObject(index, formatted, Types.TIMESTAMP);
        });
    }

    public static ColumnMapping stargateTimestampWithTimeZoneColumnMapping(int decimalDigits)
    {
        int precision = decimalDigits;
        TimestampWithTimeZoneType timestampWithTimeZoneType = createTimestampWithTimeZoneType(precision);
        if (timestampWithTimeZoneType.isShort()) {
            return ColumnMapping.longMapping(
                    timestampWithTimeZoneType,
                    shortTimestampWithTimeZoneReadFunction(),
                    shortTimestampWithTimeZoneWriteFunction(precision));
        }
        return ColumnMapping.objectMapping(
                timestampWithTimeZoneType,
                longTimestampWithTimeZoneReadFunction(),
                longTimestampWithTimeZoneWriteFunction(precision));
    }

    private static LongReadFunction shortTimestampWithTimeZoneReadFunction()
    {
        return (resultSet, columnIndex) -> {
            String value = resultSet.getString(columnIndex);
            LongTimestampWithTimeZone longTimestampWithTimeZone = parseTimestampWithTimeZone(value);
            verify(longTimestampWithTimeZone.getPicosOfMilli() == 0, "Unexpected sub-millisecond precision: %s", longTimestampWithTimeZone);
            return packDateTimeWithZone(longTimestampWithTimeZone.getEpochMillis(), longTimestampWithTimeZone.getTimeZoneKey());
        };
    }

    private static ObjectReadFunction longTimestampWithTimeZoneReadFunction()
    {
        return ObjectReadFunction.of(LongTimestampWithTimeZone.class, (resultSet, columnIndex) -> {
            String value = resultSet.getString(columnIndex);
            return parseTimestampWithTimeZone(value);
        });
    }

    private static LongTimestampWithTimeZone parseTimestampWithTimeZone(String value)
    {
        Matcher matcher = TIMESTAMP_TZ_PATTERN.matcher(value);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid timestamp with time zone: " + value);
        }

        String year = matcher.group("year");
        String month = matcher.group("month");
        String day = matcher.group("day");
        String hour = matcher.group("hour");
        String minute = matcher.group("minute");
        String second = matcher.group("second");
        @Nullable
        String fraction = matcher.group("fraction");
        String timezone = matcher.group("timezone");

        LocalDateTime localDateTime = LocalDateTime.of(parseInt(year), parseInt(month), parseInt(day), parseInt(hour), parseInt(minute), parseInt(second));
        ZoneId zoneId = ZoneId.of(timezone);
        List<ZoneOffset> offsets = zoneId.getRules().getValidOffsets(localDateTime);
        if (offsets.isEmpty()) {
            throw new IllegalArgumentException("Invalid timestamp with time zone: " + value);
        }

        // Using first offset following `LiteralInterpreter.LiteralVisitor#visitTimestampLiteral` behavior
        ZoneOffset offset = offsets.get(0);

        long epochSecond = localDateTime.toEpochSecond(offset);
        long picosOfSecond;
        if (fraction != null) {
            picosOfSecond = rescale(Long.parseLong(fraction), fraction.length(), 12);
        }
        else {
            picosOfSecond = 0;
        }

        return LongTimestampWithTimeZone.fromEpochSecondsAndFraction(epochSecond, picosOfSecond, TimeZoneKey.getTimeZoneKey(timezone));
    }

    public static WriteMapping stargateTimestampWithTimeZoneWriteMapping(TimestampWithTimeZoneType timestampWithTimeZoneType)
    {
        int precision = timestampWithTimeZoneType.getPrecision();
        String dataType = format("timestamp(%s) with time zone", precision);
        if (timestampWithTimeZoneType.isShort()) {
            return WriteMapping.longMapping(dataType, shortTimestampWithTimeZoneWriteFunction(precision));
        }
        return WriteMapping.objectMapping(dataType, longTimestampWithTimeZoneWriteFunction(precision));
    }

    private static LongWriteFunction shortTimestampWithTimeZoneWriteFunction(int precision)
    {
        return (statement, index, value) -> {
            String formatted = SqlTimestampWithTimeZone.newInstance(precision, unpackMillisUtc(value), 0, unpackZoneKey(value)).toString();
            statement.setObject(index, formatted, Types.TIMESTAMP_WITH_TIMEZONE);
        };
    }

    private static ObjectWriteFunction longTimestampWithTimeZoneWriteFunction(int precision)
    {
        return ObjectWriteFunction.of(LongTimestampWithTimeZone.class, (statement, index, value) -> {
            String formatted = SqlTimestampWithTimeZone.newInstance(precision, value.getEpochMillis(), value.getPicosOfMilli(), getTimeZoneKey(value.getTimeZoneKey())).toString();
            statement.setObject(index, formatted, Types.TIMESTAMP_WITH_TIMEZONE);
        });
    }

    private static long makePicosOfDay(String originalValue, int hour, int minute, int second, @Nullable String unparsedFraction)
    {
        if (hour > 23 || minute > 59 || second > 59) {
            throw new IllegalArgumentException("Invalid time: " + originalValue);
        }

        long picosOfSecond;
        if (unparsedFraction != null) {
            long fractionValue = Long.parseLong(unparsedFraction);
            verify(unparsedFraction.length() <= 12, "Fraction too long: %s", unparsedFraction);
            picosOfSecond = rescale(fractionValue, unparsedFraction.length(), 12);
        }
        else {
            picosOfSecond = 0;
        }

        return (((hour * 60L) + minute) * 60 + second) * PICOSECONDS_PER_SECOND + picosOfSecond;
    }
}
