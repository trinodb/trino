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
package io.trino.operator.scalar;

import io.airlift.concurrent.ThreadLocalCache;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.Duration;
import io.trino.operator.scalar.timestamptz.CurrentTimestamp;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimeZoneKey;
import io.trino.type.DateTimes;
import io.trino.util.DateTimeUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeField;
import org.joda.time.Days;
import org.joda.time.LocalDate;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.ISODateTimeFormat;

import java.math.BigInteger;
import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.operator.scalar.QuarterOfYearDateTimeField.QUARTER_OF_YEAR;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.Int128Math.rescale;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKeyForOffset;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_SECOND;
import static io.trino.type.DateTimes.PICOSECONDS_PER_NANOSECOND;
import static io.trino.type.DateTimes.PICOSECONDS_PER_SECOND;
import static io.trino.type.DateTimes.scaleEpochMillisToMicros;
import static io.trino.util.DateTimeZoneIndex.getChronology;
import static io.trino.util.DateTimeZoneIndex.packDateTimeWithZone;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class DateTimeFunctions
{
    private static final ThreadLocalCache<Slice, DateTimeFormatter> DATETIME_FORMATTER_CACHE =
            new ThreadLocalCache<>(100, DateTimeFunctions::createDateTimeFormatter);

    private static final ISOChronology UTC_CHRONOLOGY = ISOChronology.getInstanceUTC();
    private static final DateTimeField DAY_OF_WEEK = UTC_CHRONOLOGY.dayOfWeek();
    private static final DateTimeField DAY_OF_MONTH = UTC_CHRONOLOGY.dayOfMonth();
    private static final DateTimeField DAY_OF_YEAR = UTC_CHRONOLOGY.dayOfYear();
    private static final DateTimeField WEEK_OF_YEAR = UTC_CHRONOLOGY.weekOfWeekyear();
    private static final DateTimeField YEAR_OF_WEEK = UTC_CHRONOLOGY.weekyear();
    private static final DateTimeField MONTH_OF_YEAR = UTC_CHRONOLOGY.monthOfYear();
    private static final DateTimeField QUARTER = QUARTER_OF_YEAR.getField(UTC_CHRONOLOGY);
    private static final DateTimeField YEAR = UTC_CHRONOLOGY.year();
    private static final int MILLISECONDS_IN_SECOND = 1000;
    private static final int MILLISECONDS_IN_MINUTE = 60 * MILLISECONDS_IN_SECOND;
    private static final int MILLISECONDS_IN_HOUR = 60 * MILLISECONDS_IN_MINUTE;
    private static final int MILLISECONDS_IN_DAY = 24 * MILLISECONDS_IN_HOUR;
    private static final int PIVOT_YEAR = 2020; // yy = 70 will correspond to 1970 but 69 to 2069
    private static final Slice ISO_8601_DATE_FORMAT = Slices.utf8Slice("%Y-%m-%d");

    private DateTimeFunctions() {}

    @ScalarFunction
    @Description("Current timestamp with time zone")
    @SqlType("timestamp(3) with time zone")
    public static long now(ConnectorSession session)
    {
        return CurrentTimestamp.shortTimestamp(3, session, null);
    }

    @Description("Current date")
    @ScalarFunction
    @SqlType(StandardTypes.DATE)
    public static long currentDate(ConnectorSession session)
    {
        ISOChronology chronology = getChronology(session.getTimeZoneKey());

        // It is ok for this method to use the Object interfaces because it is constant folded during
        // plan optimization
        LocalDate currentDate = new DateTime(session.getStart().toEpochMilli(), chronology).toLocalDate();
        return Days.daysBetween(new LocalDate(1970, 1, 1), currentDate).getDays();
    }

    @Description("Current time zone")
    @ScalarFunction("current_timezone")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice currentTimeZone(ConnectorSession session)
    {
        return utf8Slice(session.getTimeZoneKey().getId());
    }

    @ScalarFunction("from_unixtime")
    @SqlType("timestamp(3) with time zone")
    public static long fromUnixTime(ConnectorSession session, @SqlType(StandardTypes.DOUBLE) double unixTime)
    {
        // TODO (https://github.com/trinodb/trino/issues/5781)
        return packDateTimeWithZone(Math.round(unixTime * 1000), session.getTimeZoneKey());
    }

    @ScalarFunction("from_unixtime")
    @SqlType("timestamp(3) with time zone")
    public static long fromUnixTime(@SqlType(StandardTypes.DOUBLE) double unixTime, @SqlType(StandardTypes.BIGINT) long hoursOffset, @SqlType(StandardTypes.BIGINT) long minutesOffset)
    {
        TimeZoneKey timeZoneKey;
        try {
            timeZoneKey = getTimeZoneKeyForOffset(toIntExact(hoursOffset * 60 + minutesOffset));
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, e);
        }
        return packDateTimeWithZone(Math.round(unixTime * 1000), timeZoneKey);
    }

    @ScalarFunction("from_unixtime")
    @LiteralParameters("x")
    @SqlType("timestamp(3) with time zone")
    public static long fromUnixTime(@SqlType(StandardTypes.DOUBLE) double unixTime, @SqlType("varchar(x)") Slice zoneId)
    {
        return packDateTimeWithZone(Math.round(unixTime * 1000), zoneId.toStringUtf8());
    }

    @ScalarFunction("from_unixtime_nanos")
    public static final class FromUnixtimeNanosDecimal
    {
        private FromUnixtimeNanosDecimal() {}

        @LiteralParameters({"p", "s"})
        @SqlType("timestamp(9) with time zone")
        public static LongTimestampWithTimeZone fromLong(@LiteralParameter("s") long scale, ConnectorSession session, @SqlType("decimal(p, s)") Int128 unixTimeNanos)
        {
            // TODO (https://github.com/trinodb/trino/issues/5781)
            Int128 decimal = rescale(unixTimeNanos, -(int) scale);
            BigInteger unixTimeNanosInt = decimal.toBigInteger();
            long epochSeconds = unixTimeNanosInt.divide(BigInteger.valueOf(NANOSECONDS_PER_SECOND)).longValue();
            long nanosOfSecond = unixTimeNanosInt.remainder(BigInteger.valueOf(NANOSECONDS_PER_SECOND)).longValue();
            long picosOfSecond = nanosOfSecond * PICOSECONDS_PER_NANOSECOND;
            // simulate floorDiv and floorMod as BigInteger does not support those
            if (picosOfSecond < 0) {
                epochSeconds -= 1;
                picosOfSecond += PICOSECONDS_PER_SECOND;
            }
            return DateTimes.longTimestampWithTimeZone(epochSeconds, picosOfSecond, session.getTimeZoneKey().getZoneId());
        }

        @LiteralParameters({"p", "s"})
        @SqlType("timestamp(9) with time zone")
        public static LongTimestampWithTimeZone fromShort(@LiteralParameter("s") long scale, ConnectorSession session, @SqlType("decimal(p, s)") long unixTimeNanos)
        {
            // TODO (https://github.com/trinodb/trino/issues/5781)
            long roundedUnixTimeNanos = MathFunctions.Round.roundShort(scale, unixTimeNanos);
            return fromUnixtimeNanosLong(session, roundedUnixTimeNanos);
        }
    }

    @ScalarFunction("from_unixtime_nanos")
    @SqlType("timestamp(9) with time zone")
    public static LongTimestampWithTimeZone fromUnixtimeNanosLong(ConnectorSession session, @SqlType(StandardTypes.BIGINT) long unixTimeNanos)
    {
        long epochSeconds = floorDiv(unixTimeNanos, NANOSECONDS_PER_SECOND);
        long nanosOfSecond = floorMod(unixTimeNanos, NANOSECONDS_PER_SECOND);
        long picosOfSecond = nanosOfSecond * PICOSECONDS_PER_NANOSECOND;

        return DateTimes.longTimestampWithTimeZone(epochSeconds, picosOfSecond, session.getTimeZoneKey().getZoneId());
    }

    @ScalarFunction("to_iso8601")
    @SqlType("varchar(16)")
    // Standard format is YYYY-MM-DD, which gives up to 10 characters.
    // However extended notation with format ±(Y)+-MM-DD is also acceptable and as the maximum year
    // represented by 64bits timestamp is ~584944387 it may require up to 16 characters to represent a date.
    public static Slice toISO8601FromDate(@SqlType(StandardTypes.DATE) long date)
    {
        DateTimeFormatter formatter = ISODateTimeFormat.date()
                .withChronology(UTC_CHRONOLOGY);
        return utf8Slice(formatter.print(DAYS.toMillis(date)));
    }

    @ScalarFunction("from_iso8601_timestamp")
    @LiteralParameters("x")
    @SqlType("timestamp(3) with time zone")
    public static long fromISO8601Timestamp(ConnectorSession session, @SqlType("varchar(x)") Slice iso8601DateTime)
    {
        DateTimeFormatter formatter = ISODateTimeFormat.dateTimeParser()
                .withChronology(getChronology(session.getTimeZoneKey()))
                .withOffsetParsed();
        return packDateTimeWithZone(parseDateTimeHelper(formatter, iso8601DateTime.toStringUtf8()));
    }

    @ScalarFunction("from_iso8601_timestamp_nanos")
    @LiteralParameters("x")
    @SqlType("timestamp(9) with time zone")
    public static LongTimestampWithTimeZone fromIso8601TimestampNanos(ConnectorSession session, @SqlType("varchar(x)") Slice iso8601DateTime)
    {
        java.time.format.DateTimeFormatter formatter = java.time.format.DateTimeFormatter.ISO_DATE_TIME;
        String datetimeString = iso8601DateTime.toStringUtf8();

        TemporalAccessor parsedDatetime;
        try {
            parsedDatetime = formatter.parseBest(datetimeString, ZonedDateTime::from, LocalDateTime::from);
        }
        catch (DateTimeParseException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, e);
        }

        ZonedDateTime zonedDatetime;
        if (parsedDatetime instanceof ZonedDateTime) {
            zonedDatetime = (ZonedDateTime) parsedDatetime;
        }
        else {
            zonedDatetime = ((LocalDateTime) parsedDatetime).atZone(session.getTimeZoneKey().getZoneId());
        }

        long picosOfSecond = zonedDatetime.getNano() * ((long) PICOSECONDS_PER_NANOSECOND);
        TimeZoneKey zone = TimeZoneKey.getTimeZoneKey(zonedDatetime.getZone().getId());
        return LongTimestampWithTimeZone.fromEpochSecondsAndFraction(zonedDatetime.toEpochSecond(), picosOfSecond, zone);
    }

    @ScalarFunction("from_iso8601_date")
    @LiteralParameters("x")
    @SqlType(StandardTypes.DATE)
    public static long fromISO8601Date(@SqlType("varchar(x)") Slice iso8601DateTime)
    {
        DateTimeFormatter formatter = ISODateTimeFormat.dateElementParser()
                .withChronology(UTC_CHRONOLOGY);
        DateTime dateTime = parseDateTimeHelper(formatter, iso8601DateTime.toStringUtf8());
        return MILLISECONDS.toDays(dateTime.getMillis());
    }

    @Description("Truncate to the specified precision in the session timezone")
    @ScalarFunction("date_trunc")
    @LiteralParameters("x")
    @SqlType(StandardTypes.DATE)
    public static long truncateDate(@SqlType("varchar(x)") Slice unit, @SqlType(StandardTypes.DATE) long date)
    {
        long millis = getDateField(UTC_CHRONOLOGY, unit).roundFloor(DAYS.toMillis(date));
        return MILLISECONDS.toDays(millis);
    }

    @Description("Add the specified amount of date to the given date")
    @LiteralParameters("x")
    @ScalarFunction("date_add")
    @SqlType(StandardTypes.DATE)
    public static long addFieldValueDate(@SqlType("varchar(x)") Slice unit, @SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.DATE) long date)
    {
        long millis = getDateField(UTC_CHRONOLOGY, unit).add(DAYS.toMillis(date), toIntExact(value));
        return MILLISECONDS.toDays(millis);
    }

    @Description("Difference of the given dates in the given unit")
    @ScalarFunction("date_diff")
    @LiteralParameters("x")
    @SqlType(StandardTypes.BIGINT)
    public static long diffDate(@SqlType("varchar(x)") Slice unit, @SqlType(StandardTypes.DATE) long date1, @SqlType(StandardTypes.DATE) long date2)
    {
        return getDateField(UTC_CHRONOLOGY, unit).getDifferenceAsLong(DAYS.toMillis(date2), DAYS.toMillis(date1));
    }

    private static DateTimeField getDateField(ISOChronology chronology, Slice unit)
    {
        String unitString = unit.toStringUtf8().toLowerCase(ENGLISH);
        switch (unitString) {
            case "day":
                return chronology.dayOfMonth();
            case "week":
                return chronology.weekOfWeekyear();
            case "month":
                return chronology.monthOfYear();
            case "quarter":
                return QUARTER_OF_YEAR.getField(chronology);
            case "year":
                return chronology.year();
        }
        throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "'" + unitString + "' is not a valid DATE field");
    }

    public static DateTimeField getTimestampField(ISOChronology chronology, Slice unit)
    {
        String unitString = unit.toStringUtf8().toLowerCase(ENGLISH);
        switch (unitString) {
            case "millisecond":
                return chronology.millisOfSecond();
            case "second":
                return chronology.secondOfMinute();
            case "minute":
                return chronology.minuteOfHour();
            case "hour":
                return chronology.hourOfDay();
            case "day":
                return chronology.dayOfMonth();
            case "week":
                return chronology.weekOfWeekyear();
            case "month":
                return chronology.monthOfYear();
            case "quarter":
                return QUARTER_OF_YEAR.getField(chronology);
            case "year":
                return chronology.year();
        }
        throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "'" + unitString + "' is not a valid Timestamp field");
    }

    @Description("Parses the specified date/time by the given format")
    @ScalarFunction
    @LiteralParameters({"x", "y"})
    @SqlType("timestamp(3) with time zone")
    public static long parseDatetime(ConnectorSession session, @SqlType("varchar(x)") Slice datetime, @SqlType("varchar(y)") Slice formatString)
    {
        try {
            return packDateTimeWithZone(parseDateTimeHelper(
                    DateTimeFormat.forPattern(formatString.toStringUtf8())
                            .withChronology(getChronology(session.getTimeZoneKey()))
                            .withOffsetParsed()
                            .withLocale(session.getLocale()),
                    datetime.toStringUtf8()));
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }

    private static DateTime parseDateTimeHelper(DateTimeFormatter formatter, String datetimeString)
    {
        try {
            return formatter.parseDateTime(datetimeString);
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }

    public static Slice dateFormat(ISOChronology chronology, Locale locale, long timestamp, Slice formatString)
    {
        DateTimeFormatter formatter = DATETIME_FORMATTER_CACHE.get(formatString)
                .withChronology(chronology)
                .withLocale(locale);

        return utf8Slice(formatter.print(timestamp));
    }

    @ScalarFunction
    @LiteralParameters({"x", "y"})
    @SqlType("timestamp(3)") // TODO: increase precision?
    public static long dateParse(ConnectorSession session, @SqlType("varchar(x)") Slice dateTime, @SqlType("varchar(y)") Slice formatString)
    {
        if (ISO_8601_DATE_FORMAT.equals(formatString)) {
            try {
                long days = DateTimeUtils.parseDate(dateTime.toStringUtf8());
                return scaleEpochMillisToMicros(days * MILLISECONDS_PER_DAY);
            }
            catch (IllegalArgumentException | ArithmeticException | DateTimeException e) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, e);
            }
        }

        DateTimeFormatter formatter = DATETIME_FORMATTER_CACHE.get(formatString)
                .withZoneUTC()
                .withLocale(session.getLocale());

        try {
            return scaleEpochMillisToMicros(formatter.parseMillis(dateTime.toStringUtf8()));
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }

    @Description("Millisecond of the second of the given interval")
    @ScalarFunction("millisecond")
    @SqlType(StandardTypes.BIGINT)
    public static long millisecondFromInterval(@SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long milliseconds)
    {
        return milliseconds % MILLISECONDS_IN_SECOND;
    }

    @Description("Second of the minute of the given interval")
    @ScalarFunction("second")
    @SqlType(StandardTypes.BIGINT)
    public static long secondFromInterval(@SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long milliseconds)
    {
        return (milliseconds % MILLISECONDS_IN_MINUTE) / MILLISECONDS_IN_SECOND;
    }

    @Description("Minute of the hour of the given interval")
    @ScalarFunction("minute")
    @SqlType(StandardTypes.BIGINT)
    public static long minuteFromInterval(@SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long milliseconds)
    {
        return (milliseconds % MILLISECONDS_IN_HOUR) / MILLISECONDS_IN_MINUTE;
    }

    @Description("Hour of the day of the given interval")
    @ScalarFunction("hour")
    @SqlType(StandardTypes.BIGINT)
    public static long hourFromInterval(@SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long milliseconds)
    {
        return (milliseconds % MILLISECONDS_IN_DAY) / MILLISECONDS_IN_HOUR;
    }

    @Description("Day of the week of the given date")
    @ScalarFunction(value = "day_of_week", alias = "dow")
    @SqlType(StandardTypes.BIGINT)
    public static long dayOfWeekFromDate(@SqlType(StandardTypes.DATE) long date)
    {
        return DAY_OF_WEEK.get(DAYS.toMillis(date));
    }

    @Description("Day of the month of the given date")
    @ScalarFunction(value = "day", alias = "day_of_month")
    @SqlType(StandardTypes.BIGINT)
    public static long dayFromDate(@SqlType(StandardTypes.DATE) long date)
    {
        return DAY_OF_MONTH.get(DAYS.toMillis(date));
    }

    @Description("Day of the month of the given interval")
    @ScalarFunction(value = "day", alias = "day_of_month")
    @SqlType(StandardTypes.BIGINT)
    public static long dayFromInterval(@SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long milliseconds)
    {
        return milliseconds / MILLISECONDS_IN_DAY;
    }

    @Description("Last day of the month of the given date")
    @ScalarFunction("last_day_of_month")
    @SqlType(StandardTypes.DATE)
    public static long lastDayOfMonthFromDate(@SqlType(StandardTypes.DATE) long date)
    {
        long millis = UTC_CHRONOLOGY.monthOfYear().roundCeiling(DAYS.toMillis(date) + 1) - MILLISECONDS_IN_DAY;
        return MILLISECONDS.toDays(millis);
    }

    @Description("Day of the year of the given date")
    @ScalarFunction(value = "day_of_year", alias = "doy")
    @SqlType(StandardTypes.BIGINT)
    public static long dayOfYearFromDate(@SqlType(StandardTypes.DATE) long date)
    {
        return DAY_OF_YEAR.get(DAYS.toMillis(date));
    }

    @Description("Week of the year of the given date")
    @ScalarFunction(value = "week", alias = "week_of_year")
    @SqlType(StandardTypes.BIGINT)
    public static long weekFromDate(@SqlType(StandardTypes.DATE) long date)
    {
        return WEEK_OF_YEAR.get(DAYS.toMillis(date));
    }

    @Description("Year of the ISO week of the given date")
    @ScalarFunction(value = "year_of_week", alias = "yow")
    @SqlType(StandardTypes.BIGINT)
    public static long yearOfWeekFromDate(@SqlType(StandardTypes.DATE) long date)
    {
        return YEAR_OF_WEEK.get(DAYS.toMillis(date));
    }

    @Description("Month of the year of the given date")
    @ScalarFunction("month")
    @SqlType(StandardTypes.BIGINT)
    public static long monthFromDate(@SqlType(StandardTypes.DATE) long date)
    {
        return MONTH_OF_YEAR.get(DAYS.toMillis(date));
    }

    @Description("Month of the year of the given interval")
    @ScalarFunction("month")
    @SqlType(StandardTypes.BIGINT)
    public static long monthFromInterval(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long months)
    {
        return months % 12;
    }

    @Description("Quarter of the year of the given date")
    @ScalarFunction("quarter")
    @SqlType(StandardTypes.BIGINT)
    public static long quarterFromDate(@SqlType(StandardTypes.DATE) long date)
    {
        return QUARTER.get(DAYS.toMillis(date));
    }

    @Description("Year of the given date")
    @ScalarFunction("year")
    @SqlType(StandardTypes.BIGINT)
    public static long yearFromDate(@SqlType(StandardTypes.DATE) long date)
    {
        return YEAR.get(DAYS.toMillis(date));
    }

    @Description("Year of the given interval")
    @ScalarFunction("year")
    @SqlType(StandardTypes.BIGINT)
    public static long yearFromInterval(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long months)
    {
        return months / 12;
    }

    public static DateTimeFormatter createDateTimeFormatter(Slice format)
    {
        DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();

        String formatString = format.toStringUtf8();
        boolean escaped = false;
        for (int i = 0; i < formatString.length(); i++) {
            char character = formatString.charAt(i);

            if (escaped) {
                switch (character) {
                    case 'a': // %a Abbreviated weekday name (Sun..Sat)
                        builder.appendDayOfWeekShortText();
                        break;
                    case 'b': // %b Abbreviated month name (Jan..Dec)
                        builder.appendMonthOfYearShortText();
                        break;
                    case 'c': // %c Month, numeric (0..12)
                        builder.appendMonthOfYear(1);
                        break;
                    case 'd': // %d Day of the month, numeric (00..31)
                        builder.appendDayOfMonth(2);
                        break;
                    case 'e': // %e Day of the month, numeric (0..31)
                        builder.appendDayOfMonth(1);
                        break;
                    case 'f': // %f Microseconds (000000..999999)
                        builder.appendFractionOfSecond(6, 9);
                        break;
                    case 'H': // %H Hour (00..23)
                        builder.appendHourOfDay(2);
                        break;
                    case 'h': // %h Hour (01..12)
                    case 'I': // %I Hour (01..12)
                        builder.appendClockhourOfHalfday(2);
                        break;
                    case 'i': // %i Minutes, numeric (00..59)
                        builder.appendMinuteOfHour(2);
                        break;
                    case 'j': // %j Day of year (001..366)
                        builder.appendDayOfYear(3);
                        break;
                    case 'k': // %k Hour (0..23)
                        builder.appendHourOfDay(1);
                        break;
                    case 'l': // %l Hour (1..12)
                        builder.appendClockhourOfHalfday(1);
                        break;
                    case 'M': // %M Month name (January..December)
                        builder.appendMonthOfYearText();
                        break;
                    case 'm': // %m Month, numeric (00..12)
                        builder.appendMonthOfYear(2);
                        break;
                    case 'p': // %p AM or PM
                        builder.appendHalfdayOfDayText();
                        break;
                    case 'r': // %r Time, 12-hour (hh:mm:ss followed by AM or PM)
                        builder.appendClockhourOfHalfday(2)
                                .appendLiteral(':')
                                .appendMinuteOfHour(2)
                                .appendLiteral(':')
                                .appendSecondOfMinute(2)
                                .appendLiteral(' ')
                                .appendHalfdayOfDayText();
                        break;
                    case 'S': // %S Seconds (00..59)
                    case 's': // %s Seconds (00..59)
                        builder.appendSecondOfMinute(2);
                        break;
                    case 'T': // %T Time, 24-hour (hh:mm:ss)
                        builder.appendHourOfDay(2)
                                .appendLiteral(':')
                                .appendMinuteOfHour(2)
                                .appendLiteral(':')
                                .appendSecondOfMinute(2);
                        break;
                    case 'v': // %v Week (01..53), where Monday is the first day of the week; used with %x
                        builder.appendWeekOfWeekyear(2);
                        break;
                    case 'x': // %x Year for the week, where Monday is the first day of the week, numeric, four digits; used with %v
                        builder.appendWeekyear(4, 4);
                        break;
                    case 'W': // %W Weekday name (Sunday..Saturday)
                        builder.appendDayOfWeekText();
                        break;
                    case 'Y': // %Y Year, numeric, four digits
                        builder.appendYear(4, 4);
                        break;
                    case 'y': // %y Year, numeric (two digits)
                        builder.appendTwoDigitYear(PIVOT_YEAR);
                        break;
                    case 'w': // %w Day of the week (0=Sunday..6=Saturday)
                    case 'U': // %U Week (00..53), where Sunday is the first day of the week
                    case 'u': // %u Week (00..53), where Monday is the first day of the week
                    case 'V': // %V Week (01..53), where Sunday is the first day of the week; used with %X
                    case 'X': // %X Year for the week where Sunday is the first day of the week, numeric, four digits; used with %V
                    case 'D': // %D Day of the month with English suffix (0th, 1st, 2nd, 3rd, …)
                        throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("%%%s not supported in date format string", character));
                    case '%': // %% A literal “%” character
                        builder.appendLiteral('%');
                        break;
                    default: // %<x> The literal character represented by <x>
                        builder.appendLiteral(character);
                        break;
                }
                escaped = false;
            }
            else if (character == '%') {
                escaped = true;
            }
            else {
                builder.appendLiteral(character);
            }
        }

        try {
            return builder.toFormatter();
        }
        catch (UnsupportedOperationException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }

    @Description("Convert duration string to an interval")
    @ScalarFunction("parse_duration")
    @LiteralParameters("x")
    @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND)
    public static long parseDuration(@SqlType("varchar(x)") Slice duration)
    {
        try {
            return Duration.valueOf(duration.toStringUtf8()).toMillis();
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }

    @ScalarFunction("to_milliseconds")
    @SqlType(StandardTypes.BIGINT)
    public static long toMilliseconds(@SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long value)
    {
        return value;
    }
}
