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
package io.trino.util;

/**
 * Converts epoch-days to civil date components using integer arithmetic only.
 * Based on Howard Hinnant's algorithm (public domain), used in libc++ and abseil.
 * See: https://howardhinnant.github.io/date_algorithms.html
 */
public final class EpochDayUtil
{
    private EpochDayUtil() {}

    private static final int[] DAYS_IN_MONTH = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    /**
     * Extracts year from epoch days.
     */
    public static int extractYear(long epochDay)
    {
        // Shift epoch from 1970-01-01 to 0000-03-01
        long z = epochDay + 719468;
        long era = (z >= 0 ? z : z - 146096) / 146097;
        long doe = z - era * 146097; // day of era [0, 146096]
        long yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // year of era [0, 399]
        long y = yoe + era * 400;
        long doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // day of year [0, 365]
        long mp = (5 * doy + 2) / 153; // month [0, 11] starting from March
        if (mp >= 10) {
            y++;
        }
        return (int) y;
    }

    /**
     * Extracts month (1-12) from epoch days.
     */
    public static int extractMonth(long epochDay)
    {
        long z = epochDay + 719468;
        long era = (z >= 0 ? z : z - 146096) / 146097;
        long doe = z - era * 146097;
        long yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
        long doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        long mp = (5 * doy + 2) / 153;
        return (int) (mp < 10 ? mp + 3 : mp - 9);
    }

    /**
     * Extracts day of month (1-31) from epoch days.
     */
    public static int extractDay(long epochDay)
    {
        long z = epochDay + 719468;
        long era = (z >= 0 ? z : z - 146096) / 146097;
        long doe = z - era * 146097;
        long yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
        long doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        long mp = (5 * doy + 2) / 153;
        return (int) (doy - (153 * mp + 2) / 5 + 1);
    }

    /**
     * Extracts day of year (1-366) from epoch days.
     */
    public static int extractDayOfYear(long epochDay)
    {
        long z = epochDay + 719468;
        long era = (z >= 0 ? z : z - 146096) / 146097;
        long doe = z - era * 146097;
        long yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
        long y = yoe + era * 400;
        long doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        long mp = (5 * doy + 2) / 153;
        if (mp >= 10) {
            y++;
        }
        // Compute Jan 1 of year y as epoch day, then subtract
        long janFirst = daysSinceEpoch((int) y, 1, 1);
        return (int) (epochDay - janFirst + 1);
    }

    /**
     * Extracts quarter (1-4) from epoch days.
     */
    public static int extractQuarter(long epochDay)
    {
        return (extractMonth(epochDay) - 1) / 3 + 1;
    }

    /**
     * Returns the last day of the month as epoch days for the given date epoch days.
     */
    public static long lastDayOfMonth(long epochDay)
    {
        int year = extractYear(epochDay);
        int month = extractMonth(epochDay);
        int daysInMonth = daysInMonth(year, month);
        return daysSinceEpoch(year, month, daysInMonth);
    }

    /**
     * Converts civil date to epoch days (days since 1970-01-01).
     */
    public static long daysSinceEpoch(int year, int month, int day)
    {
        long y = year;
        long m = month;
        if (m <= 2) {
            y--;
        }
        long era = (y >= 0 ? y : y - 399) / 400;
        long yoe = y - era * 400;
        long doy = (153 * (m > 2 ? m - 3 : m + 9) + 2) / 5 + day - 1;
        long doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        return era * 146097 + doe - 719468;
    }

    private static boolean isLeapYear(int year)
    {
        return (year % 4 == 0) && ((year % 100 != 0) || (year % 400 == 0));
    }

    private static int daysInMonth(int year, int month)
    {
        if (month == 2 && isLeapYear(year)) {
            return 29;
        }
        return DAYS_IN_MONTH[month - 1];
    }
}
