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
package io.trino.parquet.reader;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static java.time.ZoneOffset.UTC;

public final class CalendarUtils
{
    // https://en.wikipedia.org/wiki/Proleptic_Gregorian_calendar#Difference_between_Julian_and_proleptic_Gregorian_calendar_dates
    private static final int[] JULIAN_GREGORIAN_DIFFS = {2, 1, 0, -1, -2, -3, -4, -5, -6, -7, -8, -9, -10, 0};
    private static final int[] JULIAN_GREGORIAN_DIFF_SWITCH_DAY = {
            -719164, -682945, -646420, -609895, -536845,
            -500320, -463795, -390745, -354220, -317695,
            -244645, -208120, -171595, -141427
    };

    // 15-10-1582
    public static final int LAST_SWITCH_JULIAN_DAY = JULIAN_GREGORIAN_DIFF_SWITCH_DAY[JULIAN_GREGORIAN_DIFF_SWITCH_DAY.length - 1];

    // The start day of Common Era (CE) ('0001-01-01') in Julian calendar.
    private static final int JULIAN_COMMON_ERA_START_DAY = JULIAN_GREGORIAN_DIFF_SWITCH_DAY[0];

    private static final int[] GREGORIAN_JULIAN_DIFFS = {-2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0};
    private static final int[] GREGORIAN_JULIAN_DIFF_SWITCH_DAY = {-719162, -682944, -646420, -609896, -536847,
            -500323, -463799, -390750, -354226, -317702, -244653, -208129, -171605, -141436, -141435,
            -141434, -141433, -141432, -141431, -141430, -141429, -141428, -141427};

    // 15-10-1582
    public static final int LAST_SWITCH_GREGORIAN_DAY = GREGORIAN_JULIAN_DIFF_SWITCH_DAY[GREGORIAN_JULIAN_DIFF_SWITCH_DAY.length - 1];

    // The start day of Common Era (CE) ('0001-01-01') in Proleptic Gregorian Calendar.
    private static final int GREGORIAN_COMMON_ERA_START_DAY = GREGORIAN_JULIAN_DIFF_SWITCH_DAY[0];

    private static final LocalDate GREGORIAN_START_DATE = LocalDate.of(1582, 10, 15);
    private static final LocalDate JULIAN_END_DATE = LocalDate.of(1582, 10, 4);
    private static final TimeZone TZ_UTC = TimeZone.getTimeZone(UTC);
    private static final String DATE_FORMAT = "yyyy-MM-dd";

    static final ThreadLocal<SimpleDateFormat> HYBRID_CALENDAR_DATE_FORMAT = ThreadLocal.withInitial(() -> {
        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
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

    private CalendarUtils() {}

    public static int convertDaysToProlepticGregorian(int julianDays)
    {
        if (julianDays < JULIAN_COMMON_ERA_START_DAY) {
            return convertHybridDaysToProlepticDays(julianDays);
        }
        else if (julianDays < LAST_SWITCH_JULIAN_DAY) {
            return rebaseDays(JULIAN_GREGORIAN_DIFF_SWITCH_DAY, JULIAN_GREGORIAN_DIFFS, julianDays);
        }
        return julianDays;
    }

    public static int convertDaysToHybridCalendar(int prolepticDays)
    {
        if (prolepticDays < GREGORIAN_COMMON_ERA_START_DAY) {
            return convertProlepticDaysToHybridDays(prolepticDays);
        }
        else if (prolepticDays < LAST_SWITCH_GREGORIAN_DAY) {
            return rebaseDays(GREGORIAN_JULIAN_DIFF_SWITCH_DAY, GREGORIAN_JULIAN_DIFFS, prolepticDays);
        }
        return prolepticDays;
    }

    static int dateInStringToHybridDays(String date)
    {
        try {
            return (int) TimeUnit.MILLISECONDS.toDays(HYBRID_CALENDAR_DATE_FORMAT.get().parse(date).getTime());
        }
        catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    static int dateInStringToProlepticDays(String hybridDate)
    {
        try {
            return (int) TimeUnit.MILLISECONDS.toDays(PROLEPTIC_CALENDAR_DATE_FORMAT.get().parse(hybridDate).getTime());
        }
        catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    static String formatDaysToStringInProleptic(long epochDays)
    {
        return PROLEPTIC_CALENDAR_DATE_FORMAT.get().format(new Date(TimeUnit.DAYS.toMillis(epochDays)));
    }

    static String formatDaystoStringInHybrid(int hybridDays)
    {
        return HYBRID_CALENDAR_DATE_FORMAT.get().format(new Date(TimeUnit.DAYS.toMillis(hybridDays)));
    }

    private static int convertProlepticDaysToHybridDays(int prolepticDays)
    {
        LocalDate localDate = LocalDate.ofEpochDay(prolepticDays);
        if (localDate.isAfter(JULIAN_END_DATE) && localDate.isBefore(GREGORIAN_START_DATE)) {
            localDate = GREGORIAN_START_DATE;
        }

        return dateInStringToHybridDays(formatDaysToStringInProleptic(localDate.toEpochDay()));
    }

    static int convertHybridDaysToProlepticDays(int hybridDays)
    {
        return dateInStringToProlepticDays(formatDaystoStringInHybrid(hybridDays));
    }

    private static int rebaseDays(int[] switches, int[] diffs, int days)
    {
        int i = switches.length - 1;
        while (i > 0 && days < switches[i]) {
            i--;
        }
        return days + diffs[i];
    }
}
