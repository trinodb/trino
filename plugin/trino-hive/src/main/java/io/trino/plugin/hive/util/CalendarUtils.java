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
package io.trino.plugin.hive.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import static java.time.ZoneOffset.UTC;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class CalendarUtils
{
    static final LocalDate GREGORIAN_START_DATE = LocalDate.of(1582, 10, 15);
    static final LocalDate JULIAN_END_DATE = LocalDate.of(1582, 10, 4);

    private static final TimeZone TZ_UTC = TimeZone.getTimeZone(UTC);
    private static final String DATE_FORMAT = "yyyy-MM-dd";
    private static final String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";

    static final ThreadLocal<SimpleDateFormat> HYBRID_CALENDAR_DATE_FORMAT = ThreadLocal.withInitial(() -> {
        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
        format.setCalendar(new GregorianCalendar(TZ_UTC));
        return format;
    });

    static final ThreadLocal<SimpleDateFormat> HYBRID_CALENDAR_DATE_TIME_FORMAT = ThreadLocal.withInitial(() -> {
        SimpleDateFormat format = new SimpleDateFormat(DATE_TIME_FORMAT);
        format.setCalendar(new GregorianCalendar(TZ_UTC));
        return format;
    });

    static final ThreadLocal<SimpleDateFormat> PROLEPTIC_CALENDAR_DATE_FORMAT = ThreadLocal.withInitial(() -> {
        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
        GregorianCalendar prolepticGregorianCalendar = new GregorianCalendar(TZ_UTC);
        prolepticGregorianCalendar.setGregorianChange(new Date(Long.MIN_VALUE));
        format.setCalendar(prolepticGregorianCalendar);
        return format;
    });

    static final ThreadLocal<SimpleDateFormat> PROLEPTIC_CALENDAR_DATE_TIME_FORMAT = ThreadLocal.withInitial(() -> {
        SimpleDateFormat format = new SimpleDateFormat(DATE_TIME_FORMAT);
        GregorianCalendar prolepticGregorianCalendar = new GregorianCalendar(TZ_UTC);
        prolepticGregorianCalendar.setGregorianChange(new Date(Long.MIN_VALUE));
        format.setCalendar(prolepticGregorianCalendar);
        return format;
    });

    private static final long LAST_SWITCH_JULIAN_DAY_MILLIS;
    private static final long LAST_SWITCH_JULIAN_DAY;

    static {
        try {
            LAST_SWITCH_JULIAN_DAY_MILLIS = HYBRID_CALENDAR_DATE_FORMAT.get().parse("1582-10-15").getTime();
            LAST_SWITCH_JULIAN_DAY = MILLISECONDS.toDays(LAST_SWITCH_JULIAN_DAY_MILLIS);
        }
        catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    private CalendarUtils() {}

    public static int convertDaysToProlepticGregorian(int julianDays)
    {
        if (julianDays < LAST_SWITCH_JULIAN_DAY) {
            return convertDaysToProlepticDaysInternal(julianDays);
        }
        return julianDays;
    }

    private static int convertDaysToProlepticDaysInternal(int hybridDays)
    {
        long hybridMillis = DAYS.toMillis(hybridDays);
        String hybridDateInString = HYBRID_CALENDAR_DATE_FORMAT.get().format(new Date(hybridMillis));
        long result;
        try {
            result = PROLEPTIC_CALENDAR_DATE_FORMAT.get().parse(hybridDateInString).getTime();
        }
        catch (ParseException e) {
            throw new RuntimeException(e);
        }
        long prolepticMillis = result;
        return (int) MILLISECONDS.toDays(prolepticMillis);
    }

    public static long convertTimestampToProlepticGregorian(long epochMillis)
    {
        if (epochMillis < LAST_SWITCH_JULIAN_DAY_MILLIS) {
            String dateTimeInString = HYBRID_CALENDAR_DATE_TIME_FORMAT.get().format(new Date(epochMillis));
            try {
                return PROLEPTIC_CALENDAR_DATE_TIME_FORMAT.get().parse(dateTimeInString).getTime();
            }
            catch (ParseException e) {
                throw new RuntimeException(e);
            }
        }
        return epochMillis;
    }
}
