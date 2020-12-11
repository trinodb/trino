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
import io.prestosql.plugin.jdbc.WriteMapping;
import io.prestosql.spi.type.SqlTime;
import io.prestosql.spi.type.TimeType;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.prestosql.plugin.jdbc.StandardColumnMappings.dateWriteFunction;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.Decimals.rescale;
import static io.prestosql.spi.type.TimeType.createTimeType;
import static io.prestosql.spi.type.Timestamps.PICOSECONDS_PER_SECOND;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;

final class PrestoColumnMappings
{
    private PrestoColumnMappings() {}

    public static final Pattern TIME_PATTERN = Pattern.compile("(?<hour>\\d\\d):(?<minute>\\d\\d):(?<second>\\d\\d)(?:\\.(?<fraction>\\d{1,12}))?");

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

            if (hour > 23 || minute > 59 || second > 59) {
                throw new IllegalArgumentException("Invalid time: " + value);
            }

            long picosOfSecond;
            String fraction = matcher.group("fraction");
            if (fraction != null) {
                long fractionValue = Long.parseLong(fraction);
                picosOfSecond = rescale(fractionValue, fraction.length(), 12);
            }
            else {
                picosOfSecond = 0;
            }

            return (((hour * 60L) + minute) * 60 + second) * PICOSECONDS_PER_SECOND + picosOfSecond;
        }
    }

    private static LongWriteFunction timeWriteFunction(int precision)
    {
        return (statement, index, value) -> {
            String formatted = SqlTime.newInstance(precision, value).toString();
            statement.setObject(index, formatted, Types.TIME);
        };
    }
}
