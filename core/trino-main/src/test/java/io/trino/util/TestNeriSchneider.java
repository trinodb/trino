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

import org.joda.time.chrono.ISOChronology;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static java.util.concurrent.TimeUnit.DAYS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestNeriSchneider
{
    private static final ISOChronology UTC = ISOChronology.getInstanceUTC();

    // Day count for 0001-01-01 (proleptic Gregorian) and 9999-12-31, the practical
    // bounds of Trino's DATE type.
    private static final int FIRST_DAY = (int) LocalDate.of(1, 1, 1).toEpochDay();
    private static final int LAST_DAY = (int) LocalDate.of(9999, 12, 31).toEpochDay();

    @Test
    public void testKnownDates()
    {
        assertExtraction(0);                  // 1970-01-01
        assertExtraction(-1);                 // 1969-12-31
        assertExtraction(-719162);            // 0001-01-01
        assertExtraction((int) LocalDate.of(2000, 2, 29).toEpochDay());
        assertExtraction((int) LocalDate.of(2020, 2, 29).toEpochDay());
        assertExtraction((int) LocalDate.of(2100, 3, 1).toEpochDay());
        assertExtraction((int) LocalDate.of(1900, 12, 31).toEpochDay());
        assertExtraction((int) LocalDate.of(9999, 12, 31).toEpochDay());
    }

    @Test
    public void testFullRangeMatchesJoda()
    {
        // Covers every day across the supported Trino DATE range.
        for (int days = FIRST_DAY; days <= LAST_DAY; days++) {
            assertExtraction(days);
        }
    }

    @Test
    public void testDayOfWeek()
    {
        assertThat(NeriSchneider.dayOfWeekFromDays(0)).isEqualTo(4);   // 1970-01-01 Thursday
        assertThat(NeriSchneider.dayOfWeekFromDays(-1)).isEqualTo(3);  // 1969-12-31 Wednesday
        assertThat(NeriSchneider.dayOfWeekFromDays(3)).isEqualTo(7);   // 1970-01-04 Sunday
        assertThat(NeriSchneider.dayOfWeekFromDays(4)).isEqualTo(1);   // 1970-01-05 Monday
        for (int days = FIRST_DAY; days <= LAST_DAY; days += 17) {
            int expected = LocalDate.ofEpochDay(days).getDayOfWeek().getValue();
            assertThat(NeriSchneider.dayOfWeekFromDays(days))
                    .as("days=%d", days)
                    .isEqualTo(expected);
        }
    }

    @Test
    public void testWeekOfWeekyearMatchesJoda()
    {
        for (int days = FIRST_DAY; days <= LAST_DAY; days += 13) {
            int expected = UTC.weekOfWeekyear().get(DAYS.toMillis(days));
            assertThat(NeriSchneider.weekFromDays(days))
                    .as("days=%d", days)
                    .isEqualTo(expected);
        }
    }

    @Test
    public void testYearOfWeekMatchesJoda()
    {
        for (int days = FIRST_DAY; days <= LAST_DAY; days += 13) {
            int expected = UTC.weekyear().get(DAYS.toMillis(days));
            assertThat(NeriSchneider.yearOfWeekFromDays(days))
                    .as("days=%d", days)
                    .isEqualTo(expected);
        }
    }

    @Test
    public void testWeekBoundaries()
    {
        // Year-end boundaries that flip the weekyear; exercises Sun→Mon and Thu→Fri pivots.
        assertWeekAndYearOfWeek(LocalDate.of(2001, 8, 22), 34, 2001);
        assertWeekAndYearOfWeek(LocalDate.of(2005, 1, 2), 53, 2004);
        assertWeekAndYearOfWeek(LocalDate.of(2008, 12, 28), 52, 2008);
        assertWeekAndYearOfWeek(LocalDate.of(2008, 12, 29), 1, 2009);
        assertWeekAndYearOfWeek(LocalDate.of(2009, 12, 31), 53, 2009);
        assertWeekAndYearOfWeek(LocalDate.of(2010, 1, 3), 53, 2009);
        assertWeekAndYearOfWeek(LocalDate.of(2010, 1, 4), 1, 2010);
    }

    private static void assertWeekAndYearOfWeek(LocalDate date, int expectedWeek, int expectedYearOfWeek)
    {
        int days = (int) date.toEpochDay();
        assertThat(NeriSchneider.weekFromDays(days))
                .as("week, date=%s", date)
                .isEqualTo(expectedWeek);
        assertThat(NeriSchneider.yearOfWeekFromDays(days))
                .as("yearOfWeek, date=%s", date)
                .isEqualTo(expectedYearOfWeek);
    }

    @Test
    public void testQuarter()
    {
        for (int days = FIRST_DAY; days <= LAST_DAY; days += 13) {
            int month = LocalDate.ofEpochDay(days).getMonthValue();
            int expected = ((month - 1) / 3) + 1;
            assertThat(NeriSchneider.quarterFromDays(days))
                    .as("days=%d", days)
                    .isEqualTo(expected);
        }
    }

    @Test
    public void testLastDayOfMonth()
    {
        for (int days = FIRST_DAY; days <= LAST_DAY; days += 13) {
            LocalDate date = LocalDate.ofEpochDay(days);
            LocalDate last = date.withDayOfMonth(date.lengthOfMonth());
            assertThat(NeriSchneider.lastDayOfMonthFromDays(days))
                    .as("days=%d", days)
                    .isEqualTo((int) last.toEpochDay());
        }
    }

    @Test
    public void testIsLeapYear()
    {
        for (int year = -400; year <= 9999; year++) {
            assertThat(NeriSchneider.isLeapYear(year))
                    .as("year=%d", year)
                    .isEqualTo(LocalDate.of(year, 1, 1).isLeapYear());
        }
    }

    private static void assertExtraction(int days)
    {
        long millis = DAYS.toMillis(days);
        assertThat(NeriSchneider.yearFromDays(days))
                .as("year, days=%d", days)
                .isEqualTo(UTC.year().get(millis));
        assertThat(NeriSchneider.monthFromDays(days))
                .as("month, days=%d", days)
                .isEqualTo(UTC.monthOfYear().get(millis));
        assertThat(NeriSchneider.dayOfMonthFromDays(days))
                .as("day, days=%d", days)
                .isEqualTo(UTC.dayOfMonth().get(millis));
        assertThat(NeriSchneider.dayOfYearFromDays(days))
                .as("dayOfYear, days=%d", days)
                .isEqualTo(UTC.dayOfYear().get(millis));
    }
}
