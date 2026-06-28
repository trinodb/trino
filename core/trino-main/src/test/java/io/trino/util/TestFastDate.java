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

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

public class TestFastDate
{
    @Test
    public void testPrintDateRoundTrip()
    {
        // Fast path (years 0-9999)
        assertThat(DateTimeUtils.printDate(0)).isEqualTo("1970-01-01");
        assertThat(DateTimeUtils.printDate((int) LocalDate.of(2024, 2, 29).toEpochDay())).isEqualTo("2024-02-29");
        assertThat(DateTimeUtils.printDate((int) LocalDate.of(1, 1, 1).toEpochDay())).isEqualTo("0001-01-01");
        assertThat(DateTimeUtils.printDate((int) LocalDate.of(9999, 12, 31).toEpochDay())).isEqualTo("9999-12-31");
        // Joda-fallback path (negative or 5+ digit years) — ISODateTimeFormat formats with sign and width.
        // Just assert it doesn't blow up and matches Joda's previous behavior, which is what the
        // existing surrounding code expected.
        DateTimeUtils.printDate((int) LocalDate.of(-1, 6, 15).toEpochDay());
        DateTimeUtils.printDate((int) LocalDate.of(12345, 1, 1).toEpochDay());
    }

    @Test
    public void testEpoch()
    {
        long ymd = FastDate.ymdFromEpochDay(0);
        assertThat((int) (ymd >> 32)).isEqualTo(1970);
        assertThat((int) ((ymd >> 8) & 0xFF)).isEqualTo(1);
        assertThat((int) (ymd & 0xFF)).isEqualTo(1);
    }

    @Test
    public void testBoundaryDates()
    {
        int[] interestingYears = {-400, -100, -4, -1, 0, 1, 4, 100, 400, 1900, 1969, 1970, 1971, 2000, 2024, 2100, 2400, 9999};
        for (int year : interestingYears) {
            assertDate(LocalDate.of(year, 1, 1));
            assertDate(LocalDate.of(year, 2, 28));
            if (LocalDate.of(year, 1, 1).isLeapYear()) {
                assertDate(LocalDate.of(year, 2, 29));
            }
            assertDate(LocalDate.of(year, 3, 1));
            assertDate(LocalDate.of(year, 12, 31));
        }
    }

    @Test
    public void testBoundedSweep()
    {
        // ±5_000_000 days ≈ years -11700 .. +15600. Catches every leap-rule edge in Trino's
        // realistic range and runs in a few seconds.
        for (int days = -5_000_000; days <= 5_000_000; days++) {
            check(days);
        }
    }

    @Test
    public void testIntBoundaries()
    {
        // The fast algorithm's safe range is [-2_147_476_476, INT_MAX]. Below the lower bound
        // we fall back to LocalDate, so every int input must round-trip correctly.
        int[] candidates = {
                Integer.MIN_VALUE,
                Integer.MIN_VALUE + 1,
                -2_147_476_477,   // one below safe boundary — falls back
                -2_147_476_476,   // safe boundary — fast path
                -2_000_000_000,
                -1_000_000_000,
                -1,
                0,
                1,
                1_000_000_000,
                2_000_000_000,
                Integer.MAX_VALUE - 1,
                Integer.MAX_VALUE,
        };
        for (int days : candidates) {
            check(days);
        }
    }

    @Test
    @Disabled("Manual: parallel sweep over the entire int range; ~1 minute on a workstation.")
    public void testFullIntSweep()
    {
        // Stay within LocalDate.ofEpochDay's supported range to avoid spurious failures.
        // LocalDate.MIN.toEpochDay() = -365_243_219_162; LocalDate.MAX.toEpochDay() = 365_241_780_471.
        // int range is well inside that.
        IntStream.rangeClosed(Integer.MIN_VALUE, Integer.MAX_VALUE)
                .parallel()
                .forEach(TestFastDate::check);
    }

    @Test
    public void testDaysFromYmdSweep()
    {
        // ±5_000_000 days ≈ years -11700..+15600. For each, take (y,m,d) from LocalDate
        // and assert the round-trip via daysFromYmd matches.
        for (int days = -5_000_000; days <= 5_000_000; days++) {
            LocalDate date = LocalDate.ofEpochDay(days);
            int got = FastDate.daysFromYmd(date.getYear(), date.getMonthValue(), date.getDayOfMonth());
            if (got != days) {
                throw new AssertionError(String.format(
                        "daysFromYmd mismatch at %s: expected %d, got %d",
                        date,
                        days,
                        got));
            }
        }
    }

    @Test
    public void testDaysFromYmdBoundaries()
    {
        int[] interestingYears = {-400, -100, -4, -1, 0, 1, 4, 100, 400, 1900, 1969, 1970, 1971, 2000, 2024, 2100, 2400, 9999};
        for (int year : interestingYears) {
            assertRoundTrip(LocalDate.of(year, 1, 1));
            assertRoundTrip(LocalDate.of(year, 2, 28));
            if (LocalDate.of(year, 1, 1).isLeapYear()) {
                assertRoundTrip(LocalDate.of(year, 2, 29));
            }
            assertRoundTrip(LocalDate.of(year, 3, 1));
            assertRoundTrip(LocalDate.of(year, 12, 31));
        }
    }

    @Test
    public void testDaysInMonth()
    {
        // Jan/Mar/May/Jul/Aug/Oct/Dec = 31; Apr/Jun/Sep/Nov = 30; Feb varies.
        assertThat(FastDate.daysInMonth(2024, 1)).isEqualTo(31);
        assertThat(FastDate.daysInMonth(2024, 4)).isEqualTo(30);
        assertThat(FastDate.daysInMonth(2024, 2)).isEqualTo(29); // leap
        assertThat(FastDate.daysInMonth(2023, 2)).isEqualTo(28);
        assertThat(FastDate.daysInMonth(2000, 2)).isEqualTo(29); // /400 leap
        assertThat(FastDate.daysInMonth(1900, 2)).isEqualTo(28); // /100 non-leap
        assertThat(FastDate.daysInMonth(2400, 2)).isEqualTo(29);
    }

    private static void assertRoundTrip(LocalDate date)
    {
        int got = FastDate.daysFromYmd(date.getYear(), date.getMonthValue(), date.getDayOfMonth());
        long expected = date.toEpochDay();
        if (got != expected) {
            throw new AssertionError(String.format(
                    "daysFromYmd mismatch at %s: expected %d, got %d",
                    date,
                    expected,
                    got));
        }
    }

    private static void assertDate(LocalDate date)
    {
        check((int) date.toEpochDay());
    }

    private static void check(int days)
    {
        LocalDate expected = LocalDate.ofEpochDay(days);
        long ymd = FastDate.ymdFromEpochDay(days);
        int year = (int) (ymd >> 32);
        int month = (int) ((ymd >> 8) & 0xFF);
        int day = (int) (ymd & 0xFF);
        if (year != expected.getYear() || month != expected.getMonthValue() || day != expected.getDayOfMonth()) {
            throw new AssertionError(String.format(
                    "Mismatch at days=%d: expected %04d-%02d-%02d, got %d-%d-%d",
                    days,
                    expected.getYear(),
                    expected.getMonthValue(),
                    expected.getDayOfMonth(),
                    year,
                    month,
                    day));
        }
        int doy = FastDate.dayOfYearOf(days);
        if (doy != expected.getDayOfYear()) {
            throw new AssertionError(String.format(
                    "Day-of-year mismatch at days=%d (%s): expected %d, got %d",
                    days,
                    expected,
                    expected.getDayOfYear(),
                    doy));
        }
        int dim = FastDate.daysInMonthOf(days);
        int expectedDim = expected.lengthOfMonth();
        if (dim != expectedDim) {
            throw new AssertionError(String.format(
                    "Days-in-month mismatch at days=%d (%s): expected %d, got %d",
                    days,
                    expected,
                    expectedDim,
                    dim));
        }
    }
}
