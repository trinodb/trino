/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.prestoconnector;

import io.prestosql.plugin.jdbc.ColumnMapping;
import io.prestosql.plugin.jdbc.LongReadFunction;
import io.prestosql.plugin.jdbc.LongWriteFunction;
import io.prestosql.plugin.jdbc.ObjectReadFunction;
import io.prestosql.plugin.jdbc.ObjectWriteFunction;
import io.prestosql.plugin.jdbc.WriteMapping;
import io.prestosql.spi.type.LongTimestamp;
import io.prestosql.spi.type.LongTimestampWithTimeZone;
import io.prestosql.spi.type.SqlTime;
import io.prestosql.spi.type.SqlTimestamp;
import io.prestosql.spi.type.SqlTimestampWithTimeZone;
import io.prestosql.spi.type.TimeType;
import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TimestampWithTimeZoneType;

import javax.annotation.Nullable;

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
import static io.prestosql.plugin.jdbc.StandardColumnMappings.dateWriteFunction;
import static io.prestosql.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.prestosql.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.prestosql.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.Decimals.rescale;
import static io.prestosql.spi.type.TimeType.createTimeType;
import static io.prestosql.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.prestosql.spi.type.TimestampType.createTimestampType;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.prestosql.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.prestosql.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.prestosql.spi.type.Timestamps.PICOSECONDS_PER_SECOND;
import static java.lang.Integer.parseInt;
import static java.lang.Math.multiplyExact;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;

final class PrestoColumnMappings
{
    private PrestoColumnMappings() {}

    public static final Pattern TIME_PATTERN = Pattern.compile("(?<hour>\\d\\d):(?<minute>\\d\\d):(?<second>\\d\\d)(?:\\.(?<fraction>\\d{1,12}))?");
    public static final Pattern TIMESTAMP_PATTERN = Pattern.compile("" +
            "(?<year>[-+]?\\d{4,})-(?<month>\\d\\d)-(?<day>\\d\\d) " +
            "(?<hour>\\d\\d):(?<minute>\\d\\d):(?<second>\\d\\d)(?:\\.(?<fraction>\\d{1,12}))?");
    public static final Pattern TIMESTAMP_TZ_PATTERN = Pattern.compile("" +
            "(?<year>[-+]?\\d{4,})-(?<month>\\d\\d)-(?<day>\\d\\d) " +
            "(?<hour>\\d\\d):(?<minute>\\d\\d):(?<second>\\d\\d)(?:\\.(?<fraction>\\d{1,12}))?\\b" +
            "\\s*(?<timezone>.+)");

    public static ColumnMapping prestoDateColumnMapping()
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
                        // TODO simplify this read function once https://github.com/prestosql/presto/issues/6242 is fixed
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
                dateWriteFunction());
    }

    public static ColumnMapping prestoTimeColumnMapping(int decimalDigits)
    {
        int precision = decimalDigits;
        return ColumnMapping.longMapping(
                createTimeType(precision),
                timeReadFunction(),
                timeWriteFunction(precision));
    }

    public static WriteMapping prestoTimeWriteMapping(TimeType timeType)
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

    public static ColumnMapping prestoTimestampColumnMapping(int decimalDigits)
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

    public static WriteMapping prestoTimestampWriteMapping(TimestampType timestampType)
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

    public static ColumnMapping prestoTimestampWithTimeZoneColumnMapping(int decimalDigits)
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

    public static WriteMapping prestoTimestampWithTimeZoneWriteMapping(TimestampWithTimeZoneType timestampWithTimeZoneType)
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
            statement.setObject(index, formatted, Types.TIMESTAMP);
        };
    }

    private static ObjectWriteFunction longTimestampWithTimeZoneWriteFunction(int precision)
    {
        return ObjectWriteFunction.of(LongTimestampWithTimeZone.class, (statement, index, value) -> {
            String formatted = SqlTimestampWithTimeZone.newInstance(precision, value.getEpochMillis(), value.getPicosOfMilli(), getTimeZoneKey(value.getTimeZoneKey())).toString();
            statement.setObject(index, formatted, Types.TIMESTAMP);
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
