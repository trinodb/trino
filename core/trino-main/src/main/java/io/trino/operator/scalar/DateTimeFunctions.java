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
import io.trino.type.LongInterval;
import io.trino.util.DateTimeFormat;
import io.trino.util.DateTimeUtils;
import io.trino.util.FastDate;

import java.math.BigInteger;
import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.Int128Math.rescale;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKeyForOffset;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_SECOND;
import static io.trino.type.DateTimes.scaleEpochMillisToMicros;
import static java.lang.Math.abs;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.time.ZoneOffset.UTC;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class DateTimeFunctions
{
    private static final ThreadLocalCache<Slice, DateTimeFormat> DATETIME_FORMATTER_CACHE =
            new ThreadLocalCache<>(100, DateTimeFunctions::createDateTimeFormatter);
    private static final ThreadLocalCache<Slice, DateTimeFormat> PATTERN_FORMATTER_CACHE =
            new ThreadLocalCache<>(100, format -> DateTimeFormat.forPattern(format.toStringUtf8()));
    private static final DateTimeFormat ISO_DATE_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd");
    private static final Slice ISO_8601_DATE_FORMAT = utf8Slice("%Y-%m-%d");

    private DateTimeFunctions() {}

    @ScalarFunction(neverFails = true)
    @Description("Current timestamp with time zone")
    @SqlType("timestamp(3) with time zone")
    public static long now(ConnectorSession session)
    {
        return CurrentTimestamp.shortTimestamp(3, session, null);
    }

    @Description("Current date")
    @ScalarFunction(neverFails = true)
    @SqlType(StandardTypes.DATE)
    public static long currentDate(ConnectorSession session)
    {
        return session.getStart().atZone(session.getTimeZoneKey().getZoneId()).toLocalDate().toEpochDay();
    }

    @Description("Current time zone")
    @ScalarFunction(value = "current_timezone", neverFails = true)
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
        try {
            return packDateTimeWithZone(unixTimeToEpochMillis(unixTime), session.getTimeZoneKey());
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }

    @ScalarFunction("from_unixtime")
    @SqlType("timestamp(3) with time zone")
    public static long fromUnixTime(@SqlType(StandardTypes.DOUBLE) double unixTime, @SqlType(StandardTypes.BIGINT) long hoursOffset, @SqlType(StandardTypes.BIGINT) long minutesOffset)
    {
        TimeZoneKey timeZoneKey;
        try {
            timeZoneKey = getTimeZoneKeyForOffset((hoursOffset * 60) + minutesOffset);
            return packDateTimeWithZone(unixTimeToEpochMillis(unixTime), timeZoneKey);
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }

    @ScalarFunction("from_unixtime")
    @LiteralParameters("x")
    @SqlType("timestamp(3) with time zone")
    public static long fromUnixTime(@SqlType(StandardTypes.DOUBLE) double unixTime, @SqlType("varchar(x)") Slice zoneId)
    {
        try {
            return packDateTimeWithZone(unixTimeToEpochMillis(unixTime), zoneId.toStringUtf8());
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }

    private static long unixTimeToEpochMillis(double unixTime)
    {
        double millis = unixTime * MILLISECONDS_PER_SECOND;
        if (abs(millis) >= 0x1p52) {
            // Integral and too large to pack; the saturating cast keeps it so
            return (long) millis;
        }
        // Round half away from zero, treating unixTime as a tie when it is the double nearest a half millisecond, as
        // CAST(unixTime AS decimal(p, 3)) does for all but far-future values. The exact product is within 0.5 ulp of millis
        // and a decimal that rounds to unixTime within 1.5 ulp, so away from a half millisecond, millis rounds the same.
        double floor = Math.floor(millis);
        double fraction = millis - floor;
        if (abs(fraction - 0.5) > 2 * Math.ulp(millis)) {
            return Math.round(millis);
        }
        long lowerMillis = (long) floor;
        double halfMillisecond = (2 * lowerMillis + 1) / (2.0 * MILLISECONDS_PER_SECOND);
        if (unixTime > halfMillisecond || (unixTime == halfMillisecond && unixTime > 0)) {
            return lowerMillis + 1;
        }
        return lowerMillis;
    }

    @ScalarFunction("from_unixtime_nanos")
    public static final class FromUnixtimeNanosDecimal
    {
        private FromUnixtimeNanosDecimal() {}

        @LiteralParameters({"p", "s"})
        @SqlType("timestamp(9) with time zone")
        public static LongTimestampWithTimeZone fromLong(@LiteralParameter("s") int scale, ConnectorSession session, @SqlType("decimal(p, s)") Int128 unixTimeNanos)
        {
            // TODO (https://github.com/trinodb/trino/issues/5781)
            Int128 decimal = rescale(unixTimeNanos, -scale);
            BigInteger unixTimeNanosInt = decimal.toBigInteger();
            long epochSeconds = unixTimeNanosInt.divide(BigInteger.valueOf(NANOSECONDS_PER_SECOND)).longValue();
            long nanosOfSecond = unixTimeNanosInt.remainder(BigInteger.valueOf(NANOSECONDS_PER_SECOND)).longValue();
            long picosOfSecond = nanosOfSecond * PICOSECONDS_PER_NANOSECOND;
            // simulate floorDiv and floorMod as BigInteger does not support those
            if (picosOfSecond < 0) {
                epochSeconds -= 1;
                picosOfSecond += PICOSECONDS_PER_SECOND;
            }
            try {
                return DateTimes.longTimestampWithTimeZone(epochSeconds, picosOfSecond, session.getTimeZoneKey().getZoneId());
            }
            catch (ArithmeticException e) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, e);
            }
        }

        @LiteralParameters({"p", "s"})
        @SqlType("timestamp(9) with time zone")
        public static LongTimestampWithTimeZone fromShort(@LiteralParameter("s") int scale, ConnectorSession session, @SqlType("decimal(p, s)") long unixTimeNanos)
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
        return utf8Slice(ISO_DATE_FORMAT.format(DAYS.toMillis(date), UTC, Locale.ENGLISH));
    }

    @ScalarFunction("from_iso8601_timestamp")
    @LiteralParameters("x")
    @SqlType("timestamp(3) with time zone")
    public static long fromISO8601Timestamp(ConnectorSession session, @SqlType("varchar(x)") Slice iso8601DateTime)
    {
        try {
            ZonedDateTime dateTime = DateTimeFormat.parseIsoTimestamp(iso8601DateTime.toStringUtf8(), session.getTimeZoneKey().getZoneId());
            return packDateTimeWithZone(dateTime.toInstant().toEpochMilli(), dateTime.getZone().getId());
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }

    @ScalarFunction("from_iso8601_timestamp_nanos")
    @LiteralParameters("x")
    @SqlType("timestamp(9) with time zone")
    public static LongTimestampWithTimeZone fromIso8601TimestampNanos(ConnectorSession session, @SqlType("varchar(x)") Slice iso8601DateTime)
    {
        DateTimeFormatter formatter = DateTimeFormatter.ISO_DATE_TIME;
        String datetimeString = iso8601DateTime.toStringUtf8();

        TemporalAccessor parsedDatetime;
        try {
            parsedDatetime = formatter.parseBest(datetimeString, ZonedDateTime::from, LocalDateTime::from);
        }
        catch (DateTimeParseException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, e);
        }

        ZonedDateTime zonedDatetime = switch (parsedDatetime) {
            case ZonedDateTime value -> value;
            case LocalDateTime value -> value.atZone(session.getTimeZoneKey().getZoneId());
            default -> throw new IllegalStateException("Unexpected value: " + parsedDatetime);
        };

        long picosOfSecond = zonedDatetime.getNano() * ((long) PICOSECONDS_PER_NANOSECOND);
        TimeZoneKey zone = TimeZoneKey.getTimeZoneKey(zonedDatetime.getZone().getId());
        return LongTimestampWithTimeZone.fromEpochSecondsAndFraction(zonedDatetime.toEpochSecond(), picosOfSecond, zone);
    }

    @ScalarFunction("from_iso8601_date")
    @LiteralParameters("x")
    @SqlType(StandardTypes.DATE)
    public static long fromISO8601Date(@SqlType("varchar(x)") Slice iso8601DateTime)
    {
        try {
            return DateTimeFormat.parseIsoDate(iso8601DateTime.toStringUtf8()).toEpochDay();
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }

    @Description("Truncate to the specified precision in the session timezone")
    @ScalarFunction("date_trunc")
    @LiteralParameters("x")
    @SqlType(StandardTypes.DATE)
    public static long truncateDate(@SqlType("varchar(x)") Slice unit, @SqlType(StandardTypes.DATE) long date)
    {
        long millis = TimestampField.forDate(unit).truncate(DAYS.toMillis(date), UTC);
        return MILLISECONDS.toDays(millis);
    }

    @Description("Add the specified amount of date to the given date")
    @LiteralParameters("x")
    @ScalarFunction("date_add")
    @SqlType(StandardTypes.DATE)
    public static long addFieldValueDate(@SqlType("varchar(x)") Slice unit, @SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.DATE) long date)
    {
        try {
            long millis = TimestampField.forDate(unit).add(DAYS.toMillis(date), value, UTC);
            return MILLISECONDS.toDays(millis);
        }
        catch (IllegalArgumentException | ArithmeticException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }

    @Description("Difference of the given dates in the given unit")
    @ScalarFunction("date_diff")
    @LiteralParameters("x")
    @SqlType(StandardTypes.BIGINT)
    public static long diffDate(@SqlType("varchar(x)") Slice unit, @SqlType(StandardTypes.DATE) long date1, @SqlType(StandardTypes.DATE) long date2)
    {
        return TimestampField.forDate(unit).difference(DAYS.toMillis(date2), DAYS.toMillis(date1), UTC);
    }

    public static boolean isValidDateUnit(Slice unit)
    {
        try {
            TimestampField.forDate(unit);
            return true;
        }
        catch (TrinoException e) {
            return false;
        }
    }

    public static boolean isValidTimestampUnit(Slice unit)
    {
        try {
            TimestampField.forTimestamp(unit);
            return true;
        }
        catch (TrinoException e) {
            return false;
        }
    }

    @Description("Parses the specified date/time by the given format")
    @ScalarFunction
    @LiteralParameters({"x", "y"})
    @SqlType("timestamp(3) with time zone")
    public static long parseDatetime(ConnectorSession session, @SqlType("varchar(x)") Slice datetime, @SqlType("varchar(y)") Slice formatString)
    {
        try {
            ZonedDateTime dateTime = PATTERN_FORMATTER_CACHE.get(formatString)
                    .parse(datetime.toStringUtf8(), session.getTimeZoneKey().getZoneId(), session.getLocale());
            return packDateTimeWithZone(dateTime.toInstant().toEpochMilli(), dateTime.getZone().getId());
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }

    public static Slice dateFormat(ZoneId zoneId, Locale locale, long timestamp, Slice formatString)
    {
        return utf8Slice(DATETIME_FORMATTER_CACHE.get(formatString).format(timestamp, zoneId, locale));
    }

    @ScalarFunction
    @LiteralParameters({"x", "y"})
    @SqlType("timestamp(3)") // TODO: increase precision?
    public static long dateParse(ConnectorSession session, @SqlType("varchar(x)") Slice dateTime, @SqlType("varchar(y)") Slice formatString)
    {
        if (ISO_8601_DATE_FORMAT.equals(formatString)) {
            try {
                long days = DateTimeUtils.parseDate(dateTime);
                return scaleEpochMillisToMicros(days * MILLISECONDS_PER_DAY);
            }
            catch (IllegalArgumentException | ArithmeticException | DateTimeException e) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, e);
            }
        }

        try {
            long millis = DATETIME_FORMATTER_CACHE.get(formatString)
                    .parse(dateTime.toStringUtf8(), UTC, session.getLocale())
                    .toInstant()
                    .toEpochMilli();
            return scaleEpochMillisToMicros(millis);
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }

    @Description("Day of the week of the given date")
    @ScalarFunction(value = "day_of_week", alias = "dow", neverFails = true)
    @SqlType(StandardTypes.BIGINT)
    public static long dayOfWeekFromDate(@SqlType(StandardTypes.DATE) long date)
    {
        return floorMod(date + 3, 7) + 1;
    }

    @Description("Day of the month of the given date")
    @ScalarFunction(value = "day", alias = "day_of_month", neverFails = true)
    @SqlType(StandardTypes.BIGINT)
    public static long dayFromDate(@SqlType(StandardTypes.DATE) long date)
    {
        return FastDate.dayOf((int) date);
    }

    @Description("Last day of the month of the given date")
    @ScalarFunction("last_day_of_month")
    @SqlType(StandardTypes.DATE)
    public static long lastDayOfMonthFromDate(@SqlType(StandardTypes.DATE) long date)
    {
        int days = (int) date;
        return (long) days - FastDate.dayOf(days) + FastDate.daysInMonthOf(days);
    }

    @Description("Day of the year of the given date")
    @ScalarFunction(value = "day_of_year", alias = "doy", neverFails = true)
    @SqlType(StandardTypes.BIGINT)
    public static long dayOfYearFromDate(@SqlType(StandardTypes.DATE) long date)
    {
        return FastDate.dayOfYearOf((int) date);
    }

    @Description("Week of the year of the given date")
    @ScalarFunction(value = "week", alias = "week_of_year", neverFails = true)
    @SqlType(StandardTypes.BIGINT)
    public static long weekFromDate(@SqlType(StandardTypes.DATE) long date)
    {
        int thursday = (int) (7 * floorDiv(date + 3, 7));
        return (FastDate.dayOfYearOf(thursday) + 6) / 7;
    }

    @Description("Year of the ISO week of the given date")
    @ScalarFunction(value = "year_of_week", alias = "yow")
    @SqlType(StandardTypes.BIGINT)
    public static long yearOfWeekFromDate(@SqlType(StandardTypes.DATE) long date)
    {
        int thursday = (int) (7 * floorDiv(date + 3, 7));
        return FastDate.yearOf(thursday);
    }

    @Description("Month of the year of the given date")
    @ScalarFunction(value = "month", neverFails = true)
    @SqlType(StandardTypes.BIGINT)
    public static long monthFromDate(@SqlType(StandardTypes.DATE) long date)
    {
        return FastDate.monthOf((int) date);
    }

    @Description("Quarter of the year of the given date")
    @ScalarFunction(value = "quarter", neverFails = true)
    @SqlType(StandardTypes.BIGINT)
    public static long quarterFromDate(@SqlType(StandardTypes.DATE) long date)
    {
        return FastDate.quarterOf((int) date);
    }

    @Description("Year of the given date")
    @ScalarFunction(value = "year", neverFails = true)
    @SqlType(StandardTypes.BIGINT)
    public static long yearFromDate(@SqlType(StandardTypes.DATE) long date)
    {
        return FastDate.yearOf((int) date);
    }

    public static DateTimeFormat createDateTimeFormatter(Slice format)
    {
        try {
            return DateTimeFormat.forMysqlPattern(format.toStringUtf8());
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, e.getMessage(), e);
        }
    }

    @Description("Convert duration string to an interval")
    @ScalarFunction("parse_duration")
    @LiteralParameters("x")
    @SqlType("interval day(9) to second")
    public static long parseDuration(@SqlType("varchar(x)") Slice duration)
    {
        try {
            return Math.multiplyExact(Duration.valueOf(duration.toStringUtf8()).toMillis(), 1000);
        }
        catch (ArithmeticException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Duration exceeds interval range: " + duration.toStringUtf8(), e);
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }

    @ScalarFunction("to_milliseconds")
    public static final class ToMilliseconds
    {
        private ToMilliseconds() {}

        @LiteralParameters({"q", "r"})
        @SqlType(StandardTypes.BIGINT)
        public static long toMilliseconds(@SqlType("interval day(q) to second(r)") long value)
        {
            return value / 1000;
        }

        @LiteralParameters({"q", "r"})
        @SqlType(StandardTypes.BIGINT)
        public static long toMilliseconds(@SqlType("interval day(q) to second(r)") LongInterval value)
        {
            long micros = value.getMicros();
            if (micros < 0 && value.getPicosOfMicro() > 0) {
                micros++;
            }
            return micros / 1000;
        }
    }
}
