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
package io.prestosql.testing;

import io.prestosql.spi.type.SqlDate;
import io.prestosql.spi.type.SqlTime;
import io.prestosql.spi.type.SqlTimeWithTimeZone;
import io.prestosql.spi.type.SqlTimestamp;
import io.prestosql.spi.type.SqlTimestampWithTimeZone;
import io.prestosql.spi.type.TimeZoneKey;
import org.joda.time.DateTime;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;

import static io.prestosql.type.DateTimes.MILLISECONDS_PER_SECOND;
import static io.prestosql.type.DateTimes.NANOSECONDS_PER_MILLISECOND;
import static io.prestosql.type.DateTimes.PICOSECONDS_PER_NANOSECOND;
import static io.prestosql.type.DateTimes.PICOSECONDS_PER_SECOND;
import static java.lang.Math.toIntExact;
import static java.time.temporal.ChronoField.EPOCH_DAY;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public final class DateTimeTestingUtils
{
    private static final int NANOSECONDS_PER_SECOND = 1_000_000_000;

    private DateTimeTestingUtils() {}

    public static SqlDate sqlDateOf(int year, int monthOfYear, int dayOfMonth)
    {
        return sqlDateOf(LocalDate.of(year, monthOfYear, dayOfMonth));
    }

    public static SqlDate sqlDateOf(LocalDate date)
    {
        return new SqlDate((int) date.getLong(EPOCH_DAY));
    }

    public static SqlTimeWithTimeZone sqlTimeWithTimeZoneOf(int precision, int hour, int minuteOfHour, int secondOfMinute, int nanoOfSecond, int offsetHours, int offsetMinutes)
    {
        long picos = (hour * 3600 + minuteOfHour * 60 + secondOfMinute) * PICOSECONDS_PER_SECOND + scaleNanosToPicos(nanoOfSecond);
        return SqlTimeWithTimeZone.newInstance(precision, picos, offsetHours * 60 + offsetMinutes);
    }

    public static SqlTimestampWithTimeZone sqlTimestampWithTimeZoneOf(int precision, int year, int month, int day, int hour, int minute, int second, int nanoOfSecond, TimeZoneKey timeZoneKey)
    {
        ZonedDateTime base = ZonedDateTime.of(year, month, day, hour, minute, second, 0, timeZoneKey.getZoneId());

        long epochMillis = base.toEpochSecond() * MILLISECONDS_PER_SECOND + nanoOfSecond / NANOSECONDS_PER_MILLISECOND;
        int picosOfMilli = (int) scaleNanosToPicos(nanoOfSecond);

        return SqlTimestampWithTimeZone.newInstance(precision, epochMillis, picosOfMilli, timeZoneKey);
    }

    public static SqlTimestamp sqlTimestampOf(
            int precision,
            int year,
            int monthOfYear,
            int dayOfMonth,
            int hourOfDay,
            int minuteOfHour,
            int secondOfMinute,
            int millisOfSecond)
    {
        return sqlTimestampOf(precision, LocalDateTime.of(year, monthOfYear, dayOfMonth, hourOfDay, minuteOfHour, secondOfMinute, millisToNanos(millisOfSecond)));
    }

    /**
     * Constructs standard (non-legacy) TIMESTAMP value corresponding to argument
     */
    public static SqlTimestamp sqlTimestampOf(int precision, LocalDateTime dateTime)
    {
        return sqlTimestampOf(precision, DAYS.toMillis(dateTime.toLocalDate().toEpochDay()) + NANOSECONDS.toMillis(dateTime.toLocalTime().toNanoOfDay()));
    }

    public static SqlTimestamp sqlTimestampOf(int precision, DateTime dateTime)
    {
        return sqlTimestampOf(precision, dateTime.getMillis());
    }

    /**
     * @deprecated Use {@link #sqlTimestampOf(int precision, DateTime dateTime)}
     */
    @Deprecated
    public static SqlTimestamp sqlTimestampOf(DateTime dateTime)
    {
        return sqlTimestampOf(dateTime.getMillis());
    }

    /**
     * @deprecated Use {@link #sqlTimestampOf(int precision, long millis)}
     */
    @Deprecated
    public static SqlTimestamp sqlTimestampOf(long millis)
    {
        return sqlTimestampOf(3, millis);
    }

    public static SqlTimestamp sqlTimestampOf(int precision, long millis)
    {
        return SqlTimestamp.fromMillis(precision, millis);
    }

    public static SqlTime sqlTimeOf(
            int precision,
            int hour,
            int minuteOfHour,
            int secondOfMinute,
            int nanoOfSecond)
    {
        return sqlTimeOf(precision, LocalTime.of(hour, minuteOfHour, secondOfMinute, nanoOfSecond));
    }

    /**
     * @deprecated Use {@link #sqlTimeOf(int precision, int hour, int minuteOfHour, int secondOfMinute, int nanoOfSecond)}
     */
    @Deprecated
    public static SqlTime sqlTimeOf(
            int hourOfDay,
            int minuteOfHour,
            int secondOfMinute,
            int millisOfSecond)
    {
        LocalTime time = LocalTime.of(hourOfDay, minuteOfHour, secondOfMinute, millisToNanos(millisOfSecond));
        return sqlTimeOf(time);
    }

    /**
     * @deprecated Use {@link #sqlTimeOf(int precision, LocalTime time)}
     */
    @Deprecated
    public static SqlTime sqlTimeOf(LocalTime time)
    {
        return sqlTimeOf(3, time);
    }

    public static SqlTime sqlTimeOf(int precision, LocalTime time)
    {
        return SqlTime.newInstance(precision, time.toNanoOfDay() * PICOSECONDS_PER_NANOSECOND);
    }

    public static long scaleNanosToPicos(long nanos)
    {
        return Math.multiplyExact(nanos, PICOSECONDS_PER_NANOSECOND);
    }

    private static int millisToNanos(int millisOfSecond)
    {
        return toIntExact(MILLISECONDS.toNanos(millisOfSecond));
    }
}
