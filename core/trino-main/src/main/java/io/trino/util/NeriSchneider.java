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

import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;

// Civil date components (year, month, day, ...) from days-since-1970-01-01,
// using the mul-shift algorithm by Cassio Neri and Lorenz Schneider
// ("Euclidean Affine Functions and Applications to Calendar Algorithms",
// https://arxiv.org/abs/2102.06959). The algorithm operates on a March-based
// calendar shifted by 82 centuries so that any int day-count maps to a
// non-negative value, then unbiases at the end.
public final class NeriSchneider
{
    private NeriSchneider() {}

    private static final int CENTURY_SHIFT = 82;
    private static final long EPOCH_OFFSET_DAYS = 719468L + 146097L * CENTURY_SHIFT;
    private static final int YEAR_OFFSET = 400 * CENTURY_SHIFT;
    private static final int DAYS_PER_400_YEARS = 146097;
    private static final int MARCH_BASED_DOY_THRESHOLD = 306;
    private static final long YEAR_OF_CENTURY_MAGIC = 2939745L;
    private static final long MONTH_DAY_MAGIC = 2141L;
    private static final long MONTH_DAY_OFFSET = 197913L;

    private static final int[] DAYS_IN_MONTH = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    public static int yearFromDays(int daysSinceEpoch)
    {
        long n = daysSinceEpoch + EPOCH_OFFSET_DAYS;
        long n1 = 4 * n + 3;
        long century = n1 / DAYS_PER_400_YEARS;
        long nC = (n1 % DAYS_PER_400_YEARS) / 4;
        long n2 = 4 * nC + 3;
        long p2 = YEAR_OF_CENTURY_MAGIC * n2;
        long yearOfCentury = p2 >>> 32;
        long marchDoy = ((p2 & 0xFFFFFFFFL) / YEAR_OF_CENTURY_MAGIC) / 4;
        long carry = marchDoy >= MARCH_BASED_DOY_THRESHOLD ? 1 : 0;
        return (int) (100 * century + yearOfCentury - YEAR_OFFSET + carry);
    }

    public static int monthFromDays(int daysSinceEpoch)
    {
        long n = daysSinceEpoch + EPOCH_OFFSET_DAYS;
        long n1 = 4 * n + 3;
        long nC = (n1 % DAYS_PER_400_YEARS) / 4;
        long n2 = 4 * nC + 3;
        long p2 = YEAR_OF_CENTURY_MAGIC * n2;
        long marchDoy = ((p2 & 0xFFFFFFFFL) / YEAR_OF_CENTURY_MAGIC) / 4;
        long n3 = MONTH_DAY_MAGIC * marchDoy + MONTH_DAY_OFFSET;
        long marchMonth = n3 >>> 16;
        long carry = marchDoy >= MARCH_BASED_DOY_THRESHOLD ? 1 : 0;
        return (int) (marchMonth - 12 * carry);
    }

    public static int dayOfMonthFromDays(int daysSinceEpoch)
    {
        long n = daysSinceEpoch + EPOCH_OFFSET_DAYS;
        long n1 = 4 * n + 3;
        long nC = (n1 % DAYS_PER_400_YEARS) / 4;
        long n2 = 4 * nC + 3;
        long p2 = YEAR_OF_CENTURY_MAGIC * n2;
        long marchDoy = ((p2 & 0xFFFFFFFFL) / YEAR_OF_CENTURY_MAGIC) / 4;
        long n3 = MONTH_DAY_MAGIC * marchDoy + MONTH_DAY_OFFSET;
        return (int) ((n3 & 0xFFFFL) / MONTH_DAY_MAGIC) + 1;
    }

    public static int dayOfYearFromDays(int daysSinceEpoch)
    {
        long n = daysSinceEpoch + EPOCH_OFFSET_DAYS;
        long n1 = 4 * n + 3;
        long century = n1 / DAYS_PER_400_YEARS;
        long nC = (n1 % DAYS_PER_400_YEARS) / 4;
        long n2 = 4 * nC + 3;
        long p2 = YEAR_OF_CENTURY_MAGIC * n2;
        long yearOfCentury = p2 >>> 32;
        long marchDoy = ((p2 & 0xFFFFFFFFL) / YEAR_OF_CENTURY_MAGIC) / 4;
        if (marchDoy >= MARCH_BASED_DOY_THRESHOLD) {
            return (int) (marchDoy - 305);
        }
        int civilYear = (int) (100 * century + yearOfCentury - YEAR_OFFSET);
        int leap = isLeapYear(civilYear) ? 1 : 0;
        return (int) (marchDoy + 60 + leap);
    }

    public static int dayOfWeekFromDays(int daysSinceEpoch)
    {
        return floorMod(daysSinceEpoch + 3, 7) + 1;
    }

    // ISO 8601 week-of-week-year. The Thursday of an ISO week determines its week-year,
    // so shifting the input to the same week's Thursday lets the year/dayOfYear extractors
    // produce the right values. (daysSinceEpoch + 3) aligns Thursdays to multiples of 7.
    public static int weekFromDays(int daysSinceEpoch)
    {
        int thursdayDays = 7 * floorDiv(daysSinceEpoch + 3, 7);
        return (dayOfYearFromDays(thursdayDays) + 6) / 7;
    }

    public static int yearOfWeekFromDays(int daysSinceEpoch)
    {
        int thursdayDays = 7 * floorDiv(daysSinceEpoch + 3, 7);
        return yearFromDays(thursdayDays);
    }

    public static int quarterFromDays(int daysSinceEpoch)
    {
        return (monthFromDays(daysSinceEpoch) + 2) / 3;
    }

    public static int lastDayOfMonthFromDays(int daysSinceEpoch)
    {
        long n = daysSinceEpoch + EPOCH_OFFSET_DAYS;
        long n1 = 4 * n + 3;
        long century = n1 / DAYS_PER_400_YEARS;
        long nC = (n1 % DAYS_PER_400_YEARS) / 4;
        long n2 = 4 * nC + 3;
        long p2 = YEAR_OF_CENTURY_MAGIC * n2;
        long yearOfCentury = p2 >>> 32;
        long marchDoy = ((p2 & 0xFFFFFFFFL) / YEAR_OF_CENTURY_MAGIC) / 4;
        long n3 = MONTH_DAY_MAGIC * marchDoy + MONTH_DAY_OFFSET;
        int marchMonth = (int) (n3 >>> 16);
        int day = (int) ((n3 & 0xFFFFL) / MONTH_DAY_MAGIC) + 1;
        int carry = marchDoy >= MARCH_BASED_DOY_THRESHOLD ? 1 : 0;
        int month = marchMonth - 12 * carry;
        int civilYear = (int) (100 * century + yearOfCentury - YEAR_OFFSET + carry);
        int daysInMonth = (month == 2 && isLeapYear(civilYear)) ? 29 : DAYS_IN_MONTH[month];
        return daysSinceEpoch + (daysInMonth - day);
    }

    public static boolean isLeapYear(int year)
    {
        return (year & 3) == 0 && (year % 100 != 0 || year % 400 == 0);
    }
}
