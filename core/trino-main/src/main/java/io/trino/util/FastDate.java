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

import java.time.LocalDate;

/**
 * Fast civil-from-days (year/month/day from days-since-1970-01-01) using Ben Joffe's
 * 64-bit algorithm: <a href="https://www.benjoffe.com/fast-date-64">www.benjoffe.com/fast-date-64</a>.
 * Reference C++ implementation (Boost Software License 1.0):
 * <a href="https://github.com/benjoffe/fast-date-benchmarks/blob/main/algorithms/benjoffe_fast64.hpp">benjoffe_fast64.hpp</a>.
 * <p>
 * Math.unsignedMultiplyHigh is intrinsified by HotSpot to {@code mulq}/{@code umulh},
 * so each mul-shift-by-64 step is a single machine instruction. Operates on the x86 SCALE=32
 * variant of the algorithm — the ARM variant differs only in immediate-encoding efficiency.
 */
public final class FastDate
{
    private FastDate() {}

    private static final long ERAS = 14704L;
    private static final long D_SHIFT = 146097L * ERAS - 719469L;
    private static final long Y_SHIFT = 400L * ERAS - 1L;
    private static final long C1 = 505_054_698_555_331L;
    private static final long C2 = 50_504_432_782_230_121L;
    private static final long C3 = 8_619_973_866_219_416L;
    private static final long YPT_MULT = 782_432L;
    private static final long BUMP_THRESHOLD = 126_464L;
    private static final long SHIFT_NO_BUMP = 977_792L;
    private static final long SHIFT_BUMP = 191_360L;

    // The choice of ERAS=14704 maximizes the algorithm's safe range while keeping D_SHIFT
    // within uint32. Below this bound, the (D_SHIFT - days) computation overflows uint32
    // and corrupts every downstream value. Affects only ~7172 days at the very bottom of
    // the int DATE range — dates around year -5,877,660, far outside any realistic use.
    private static final int FAST_MIN_DAYS = (int) (D_SHIFT - 0xFFFFFFFFL);   // = -2_147_476_476

    // Cumulative days before the start of each civil month, indexed 1..12 (0 unused).
    private static final int[] DOY_PREFIX_NON_LEAP = {0, 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334};
    private static final int[] DOY_PREFIX_LEAP = {0, 0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335};

    // Days-in-month, indexed by civil month 1..12 (0 unused).
    private static final int[] DAYS_IN_MONTH_NON_LEAP = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    private static final int[] DAYS_IN_MONTH_LEAP = {0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    /**
     * Returns {@code (year << 32) | (month << 8) | day} where year is signed,
     * month is 1..12, and day is 1..31. Valid for the entire {@code int} range of
     * days-since-epoch (≈ ±5.88M years).
     */
    public static long ymdFromEpochDay(int days)
    {
        if (days < FAST_MIN_DAYS) {
            LocalDate date = LocalDate.ofEpochDay(days);
            return ((long) date.getYear() << 32) | ((long) date.getMonthValue() << 8) | date.getDayOfMonth();
        }
        // Mirror the C++ uint32 wraparound by zero-extending into a long.
        long rev = (D_SHIFT - days) & 0xFFFFFFFFL;
        long cen = Math.unsignedMultiplyHigh(C1, rev);
        long jul = rev + cen - (cen >>> 2);
        long numLow = C2 * jul;
        long numHigh = Math.unsignedMultiplyHigh(C2, jul);
        long yrs = (Y_SHIFT - numHigh) & 0xFFFFFFFFL;
        long ypt = Math.unsignedMultiplyHigh(YPT_MULT, numLow);
        long bump = (ypt < BUMP_THRESHOLD) ? 1L : 0L;
        long shift = (bump != 0) ? SHIFT_BUMP : SHIFT_NO_BUMP;
        long n = (yrs & 3L) * 512L + shift - ypt;
        long month = n >>> 16;
        long day = Math.unsignedMultiplyHigh(C3, n & 0xFFFFL) + 1L;
        int year = (int) (yrs + bump);
        return ((long) year << 32) | (month << 8) | day;
    }

    public static int yearOf(int days)
    {
        return (int) (ymdFromEpochDay(days) >> 32);
    }

    public static int monthOf(int days)
    {
        return (int) ((ymdFromEpochDay(days) >> 8) & 0xFF);
    }

    public static int dayOf(int days)
    {
        return (int) (ymdFromEpochDay(days) & 0xFF);
    }

    public static int quarterOf(int days)
    {
        return (monthOf(days) - 1) / 3 + 1;
    }

    public static int dayOfYearOf(int days)
    {
        long ymd = ymdFromEpochDay(days);
        int year = (int) (ymd >> 32);
        int month = (int) ((ymd >> 8) & 0xFF);
        int day = (int) (ymd & 0xFF);
        return (isLeap(year) ? DOY_PREFIX_LEAP : DOY_PREFIX_NON_LEAP)[month] + day;
    }

    public static int daysInMonthOf(int days)
    {
        long ymd = ymdFromEpochDay(days);
        int year = (int) (ymd >> 32);
        int month = (int) ((ymd >> 8) & 0xFF);
        return (isLeap(year) ? DAYS_IN_MONTH_LEAP : DAYS_IN_MONTH_NON_LEAP)[month];
    }

    /**
     * Civil days in {@code month} of {@code year}. {@code month} is 1..12.
     */
    public static int daysInMonth(int year, int month)
    {
        return (isLeap(year) ? DAYS_IN_MONTH_LEAP : DAYS_IN_MONTH_NON_LEAP)[month];
    }

    public static boolean isLeap(int year)
    {
        // Java % returns same sign as dividend; (year & 3) == 0 is equivalent to
        // year % 4 == 0 for both signs in two's complement.
        return ((year & 3) == 0) && (year % 100 != 0 || year % 400 == 0);
    }

    /**
     * Days-since-1970-01-01 from civil (year, month, day) using Ben Joffe's {@code to_rata_die}.
     * <p>
     * Precondition: the input is a valid civil date. No range or leap-day validation is
     * performed. Safe across the full {@code int} days range (year shift of 5,880,000 covers
     * roughly ±5.88M years centred on the UNIX epoch).
     * <p>
     * The {@code yrs / 100} division lowers to a single mul-shift on HotSpot since 100 is a
     * compile-time constant; all other divisions are by powers of two.
     */
    public static int daysFromYmd(int year, int month, int day)
    {
        long bump = month <= 2 ? 1L : 0L;
        long yrs = (year + 5_880_000L) - bump;
        long cen = yrs / 100L;
        long shift = bump != 0 ? 8829L : -2919L;
        long yearDays = yrs * 365L + (yrs >>> 2) - cen + (cen >>> 2);
        long monthDays = (979L * month + shift) >> 5;
        return (int) (yearDays + monthDays + day - 2_148_345_369L);
    }
}
