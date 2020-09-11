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

import io.prestosql.spi.type.SqlTime;
import io.prestosql.spi.type.SqlTimestamp;
import org.joda.time.DateTime;

import java.time.LocalDateTime;
import java.time.LocalTime;

import static io.prestosql.type.DateTimes.PICOSECONDS_PER_NANOSECOND;
import static java.lang.Math.toIntExact;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public final class DateTimeTestingUtils
{
    private DateTimeTestingUtils() {}

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
            int hourOfDay,
            int minuteOfHour,
            int secondOfMinute,
            int millisOfSecond)
    {
        LocalTime time = LocalTime.of(hourOfDay, minuteOfHour, secondOfMinute, millisToNanos(millisOfSecond));
        return sqlTimeOf(time);
    }

    public static SqlTime sqlTimeOf(LocalTime time)
    {
        return SqlTime.newInstance(3, time.toNanoOfDay() * PICOSECONDS_PER_NANOSECOND);
    }

    private static int millisToNanos(int millisOfSecond)
    {
        return toIntExact(MILLISECONDS.toNanos(millisOfSecond));
    }
}
