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
package io.prestosql.type;

import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.ScalarOperator;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;
import org.joda.time.DateTimeField;
import org.joda.time.chrono.ISOChronology;

import java.util.concurrent.TimeUnit;

import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.function.OperatorType.ADD;
import static io.prestosql.spi.function.OperatorType.SUBTRACT;
import static io.prestosql.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.prestosql.spi.type.DateTimeEncoding.updateMillisUtc;
import static io.prestosql.util.DateTimeZoneIndex.getChronology;
import static io.prestosql.util.DateTimeZoneIndex.unpackChronology;

public final class DateTimeOperators
{
    private static final DateTimeField MILLIS_OF_DAY = ISOChronology.getInstanceUTC().millisOfDay();
    private static final DateTimeField MONTH_OF_YEAR_UTC = ISOChronology.getInstanceUTC().monthOfYear();

    private DateTimeOperators()
    {
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.DATE)
    public static long datePlusIntervalDayToSecond(@SqlType(StandardTypes.DATE) long date, @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long interval)
    {
        if (MILLIS_OF_DAY.get(interval) != 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Cannot add hour, minutes or seconds to a date");
        }
        return date + TimeUnit.MILLISECONDS.toDays(interval);
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.DATE)
    public static long intervalDayToSecondPlusDate(@SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long interval, @SqlType(StandardTypes.DATE) long date)
    {
        return datePlusIntervalDayToSecond(date, interval);
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.TIME)
    public static long timePlusIntervalDayToSecond(ConnectorSession session, @SqlType(StandardTypes.TIME) long time, @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long interval)
    {
        return modulo24Hour(getChronology(session.getTimeZoneKey()), time + interval);
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.TIME)
    public static long intervalDayToSecondPlusTime(ConnectorSession session, @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long interval, @SqlType(StandardTypes.TIME) long time)
    {
        return timePlusIntervalDayToSecond(session, time, interval);
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.TIME_WITH_TIME_ZONE)
    public static long timeWithTimeZonePlusIntervalDayToSecond(@SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long time, @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long interval)
    {
        return updateMillisUtc((long) modulo24Hour(unpackChronology(time), unpackMillisUtc(time) + interval), time);
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.TIME_WITH_TIME_ZONE)
    public static long intervalDayToSecondPlusTimeWithTimeZone(@SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long interval, @SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long time)
    {
        return timeWithTimeZonePlusIntervalDayToSecond(time, interval);
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.TIMESTAMP)
    public static long timestampPlusIntervalDayToSecond(@SqlType(StandardTypes.TIMESTAMP) long timestamp, @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long interval)
    {
        return timestamp + interval;
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.TIMESTAMP)
    public static long intervalDayToSecondPlusTimestamp(@SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long interval, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        return timestampPlusIntervalDayToSecond(timestamp, interval);
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long timestampWithTimeZonePlusIntervalDayToSecond(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestamp, @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long interval)
    {
        return updateMillisUtc(unpackMillisUtc(timestamp) + interval, timestamp);
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long intervalDayToSecondPlusTimestampWithTimeZone(@SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long interval, @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestamp)
    {
        return timestampWithTimeZonePlusIntervalDayToSecond(timestamp, interval);
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.DATE)
    public static long datePlusIntervalYearToMonth(@SqlType(StandardTypes.DATE) long date, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long interval)
    {
        long millis = MONTH_OF_YEAR_UTC.add(TimeUnit.DAYS.toMillis(date), interval);
        return TimeUnit.MILLISECONDS.toDays(millis);
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.DATE)
    public static long intervalYearToMonthPlusDate(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long interval, @SqlType(StandardTypes.DATE) long date)
    {
        return datePlusIntervalYearToMonth(date, interval);
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.TIME)
    public static long timePlusIntervalYearToMonth(@SqlType(StandardTypes.TIME) long time, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long interval)
    {
        return time;
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.TIME)
    public static long intervalYearToMonthPlusTime(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long interval, @SqlType(StandardTypes.TIME) long time)
    {
        return time;
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.TIME_WITH_TIME_ZONE)
    public static long timeWithTimeZonePlusIntervalYearToMonth(@SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long time, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long interval)
    {
        return time;
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.TIME_WITH_TIME_ZONE)
    public static long intervalYearToMonthPlusTimeWithTimeZone(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long interval, @SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long time)
    {
        return time;
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.TIMESTAMP)
    public static long timestampPlusIntervalYearToMonth(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long timestamp, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long interval)
    {
        if (session.isLegacyTimestamp()) {
            return getChronology(session.getTimeZoneKey()).monthOfYear().add(timestamp, interval);
        }
        return MONTH_OF_YEAR_UTC.add(timestamp, interval);
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.TIMESTAMP)
    public static long intervalYearToMonthPlusTimestamp(ConnectorSession session, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long interval, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        return timestampPlusIntervalYearToMonth(session, timestamp, interval);
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long timestampWithTimeZonePlusIntervalYearToMonth(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestamp, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long interval)
    {
        return updateMillisUtc(unpackChronology(timestamp).monthOfYear().add(unpackMillisUtc(timestamp), interval), timestamp);
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long intervalYearToMonthPlusTimestampWithTimeZone(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long interval, @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestamp)
    {
        return timestampWithTimeZonePlusIntervalYearToMonth(timestamp, interval);
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.DATE)
    public static long dateMinusIntervalDayToSecond(@SqlType(StandardTypes.DATE) long date, @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long interval)
    {
        if (MILLIS_OF_DAY.get(interval) != 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Cannot subtract hour, minutes or seconds from a date");
        }
        return date - TimeUnit.MILLISECONDS.toDays(interval);
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.TIME)
    public static long timeMinusIntervalDayToSecond(ConnectorSession session, @SqlType(StandardTypes.TIME) long time, @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long interval)
    {
        return timePlusIntervalDayToSecond(session, time, -interval);
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.TIME_WITH_TIME_ZONE)
    public static long timeWithTimeZoneMinusIntervalDayToSecond(@SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long time, @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long interval)
    {
        return timeWithTimeZonePlusIntervalDayToSecond(time, -interval);
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.TIMESTAMP)
    public static long timestampMinusIntervalDayToSecond(@SqlType(StandardTypes.TIMESTAMP) long timestamp, @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long interval)
    {
        return timestamp - interval;
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long timestampWithTimeZoneMinusIntervalDayToSecond(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestamp, @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long interval)
    {
        return timestampWithTimeZonePlusIntervalDayToSecond(timestamp, -interval);
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.DATE)
    public static long dateMinusIntervalYearToMonth(@SqlType(StandardTypes.DATE) long date, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long interval)
    {
        return datePlusIntervalYearToMonth(date, -interval);
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.TIME)
    public static long timeMinusIntervalYearToMonth(@SqlType(StandardTypes.TIME) long time, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long interval)
    {
        return time;
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.TIME_WITH_TIME_ZONE)
    public static long timeWithTimeZoneMinusIntervalYearToMonth(@SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long time, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long interval)
    {
        return time;
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.TIMESTAMP)
    public static long timestampMinusIntervalYearToMonth(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long timestamp, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long interval)
    {
        return timestampPlusIntervalYearToMonth(session, timestamp, -interval);
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long timestampWithTimeZoneMinusIntervalYearToMonth(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestamp, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long interval)
    {
        return timestampWithTimeZonePlusIntervalYearToMonth(timestamp, -interval);
    }

    public static int modulo24Hour(ISOChronology chronology, long millis)
    {
        return chronology.millisOfDay().get(millis) - chronology.getZone().getOffset(millis);
    }

    public static long modulo24Hour(long millis)
    {
        return MILLIS_OF_DAY.get(millis);
    }
}
