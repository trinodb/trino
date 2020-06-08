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

import io.prestosql.Session;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.SqlTime;
import io.prestosql.spi.type.SqlTimestamp;
import io.prestosql.spi.type.TimeZoneKey;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;

import static io.prestosql.util.DateTimeZoneIndex.getDateTimeZone;
import static java.lang.Math.toIntExact;
import static java.time.ZoneOffset.UTC;
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
            int millisOfSecond,
            Session session)
    {
        return sqlTimestampOf(
                precision,
                year,
                monthOfYear,
                dayOfMonth,
                hourOfDay,
                minuteOfHour,
                secondOfMinute,
                millisOfSecond,
                getDateTimeZone(session.getTimeZoneKey()),
                session.getTimeZoneKey(),
                session.toConnectorSession());
    }

    public static SqlTimestamp sqlTimestampOf(
            int precision,
            int year,
            int monthOfYear,
            int dayOfMonth,
            int hourOfDay,
            int minuteOfHour,
            int secondOfMinute,
            int millisOfSecond,
            DateTimeZone baseZone,
            TimeZoneKey timestampZone,
            ConnectorSession session)
    {
        if (session.isLegacyTimestamp()) {
            return SqlTimestamp.legacyFromMillis(precision, new DateTime(year, monthOfYear, dayOfMonth, hourOfDay, minuteOfHour, secondOfMinute, millisOfSecond, baseZone).getMillis(), timestampZone);
        }
        return sqlTimestampOf(precision, LocalDateTime.of(year, monthOfYear, dayOfMonth, hourOfDay, minuteOfHour, secondOfMinute, millisToNanos(millisOfSecond)));
    }

    /**
     * Constructs standard (non-legacy) TIMESTAMP value corresponding to argument
     */
    public static SqlTimestamp sqlTimestampOf(int precision, LocalDateTime dateTime)
    {
        return SqlTimestamp.fromMillis(precision, DAYS.toMillis(dateTime.toLocalDate().toEpochDay()) + NANOSECONDS.toMillis(dateTime.toLocalTime().toNanoOfDay()));
    }

    public static SqlTimestamp sqlTimestampOf(int precision, DateTime dateTime, Session session)
    {
        return sqlTimestampOf(precision, dateTime.getMillis(), session.toConnectorSession());
    }

    /**
     * @deprecated Use {@link #sqlTimestampOf(int precision, DateTime dateTime, Session session)}
     */
    @Deprecated
    public static SqlTimestamp sqlTimestampOf(DateTime dateTime, Session session)
    {
        return sqlTimestampOf(3, dateTime.getMillis(), session.toConnectorSession());
    }

    /**
     * @deprecated Use {@link #sqlTimestampOf(int precision, long millis, ConnectorSession session)}
     */
    @Deprecated
    public static SqlTimestamp sqlTimestampOf(long millis, ConnectorSession session)
    {
        return sqlTimestampOf(3, millis, session);
    }

    public static SqlTimestamp sqlTimestampOf(int precision, long millis, ConnectorSession session)
    {
        if (session.isLegacyTimestamp()) {
            return SqlTimestamp.legacyFromMillis(precision, millis, session.getTimeZoneKey());
        }
        else {
            return SqlTimestamp.fromMillis(precision, millis);
        }
    }

    public static SqlTime sqlTimeOf(
            int hourOfDay,
            int minuteOfHour,
            int secondOfMinute,
            int millisOfSecond,
            Session session)
    {
        return sqlTimeOf(hourOfDay, minuteOfHour, secondOfMinute, millisOfSecond, session.toConnectorSession());
    }

    public static SqlTime sqlTimeOf(
            int hourOfDay,
            int minuteOfHour,
            int secondOfMinute,
            int millisOfSecond,
            ConnectorSession session)
    {
        LocalTime time = LocalTime.of(hourOfDay, minuteOfHour, secondOfMinute, millisToNanos(millisOfSecond));
        return sqlTimeOf(time, session);
    }

    public static SqlTime sqlTimeOf(LocalTime time, ConnectorSession session)
    {
        if (session.isLegacyTimestamp()) {
            long millisUtc = LocalDate.ofEpochDay(0)
                    .atTime(time)
                    .atZone(UTC)
                    .withZoneSameLocal(ZoneId.of(session.getTimeZoneKey().getId()))
                    .toInstant()
                    .toEpochMilli();
            return new SqlTime(millisUtc, session.getTimeZoneKey());
        }
        return new SqlTime(NANOSECONDS.toMillis(time.toNanoOfDay()));
    }

    private static int millisToNanos(int millisOfSecond)
    {
        return toIntExact(MILLISECONDS.toNanos(millisOfSecond));
    }
}
