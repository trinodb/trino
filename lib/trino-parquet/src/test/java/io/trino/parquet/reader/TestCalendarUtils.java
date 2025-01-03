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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.Map;

import static io.trino.parquet.reader.CalendarUtils.convertDaysToHybridCalendar;
import static io.trino.parquet.reader.CalendarUtils.convertDaysToProlepticGregorian;
import static io.trino.parquet.reader.CalendarUtils.dateInStringToHybridDays;
import static org.assertj.core.api.Assertions.assertThat;

class TestCalendarUtils
{
    @Test
    void testConvertGregorianDaysToAndFromHybridDays()
    {
        ImmutableList<String> dates = ImmutableList.of(
                "0001-01-01",
                "1000-01-01",
                "1582-10-04",
                "1582-10-15",
                "1788-09-10",
                "1888-12-31",
                "1969-12-31",
                "1970-01-01",
                "2024-03-30");

        dates.forEach(date -> {
            int julianDays = dateInStringToHybridDays(date);
            int gregorianDays = (int) LocalDate.parse(date).toEpochDay();

            assertThat(convertDaysToHybridCalendar(gregorianDays)).isEqualTo(julianDays);
            assertThat(convertDaysToProlepticGregorian(julianDays)).isEqualTo(gregorianDays);
        });
    }

    @Test
    void testConvertHybridToProlepticDateForLeapYears()
    {
        ImmutableMap<String, String> dates = ImmutableMap.<String, String>builder()
                .put("0004-02-29", "0004-02-29")
                .put("0100-02-29", "0100-03-01")
                .put("0196-02-29", "0196-02-29")
                .put("0200-02-29", "0200-03-01")
                .put("0204-02-29", "0204-02-29")
                .put("0400-02-29", "0400-02-29")
                .put("1000-02-29", "1000-03-01")
                .put("1200-02-29", "1200-02-29")
                .put("1600-02-29", "1600-02-29")
                .put("1700-02-29", "1700-03-01")
                .put("2000-02-29", "2000-02-29")
                .buildOrThrow();

        dates.forEach((julianDate, gregDate) -> {
            int julianDays = dateInStringToHybridDays(julianDate);
            int gregorianDays = (int) LocalDate.parse(gregDate).toEpochDay();
            assertThat(convertDaysToProlepticGregorian(julianDays)).isEqualTo(gregorianDays);
        });
    }

    @Test
    void testConvertDatesFromSwitchesBoarders()
    {
        ImmutableList<String> dates = ImmutableList.<String>builder()
                .add("0001-01-01")
                .add("0100-03-01")
                .add("0100-03-02")
                .add("0200-02-28")
                .add("0200-03-01")
                .add("0300-02-28")
                .add("0300-03-01")
                .add("0500-02-27")
                .add("0500-02-28")
                .add("0600-02-26")
                .add("0600-02-27")
                .add("0700-02-25")
                .add("0700-02-26")
                .add("0900-02-24")
                .add("0900-02-25")
                .add("1000-02-23")
                .add("1000-02-24")
                .add("1100-02-22")
                .add("1100-02-23")
                .add("1300-02-21")
                .add("1300-02-22")
                .add("1400-02-20")
                .add("1400-02-21")
                .add("1500-02-19")
                .add("1500-02-20")
                .add("1582-02-04")
                .build();

        dates.forEach(date -> {
            int hybridDays = dateInStringToHybridDays(date);
            int gregorianDays = (int) LocalDate.parse(date).toEpochDay();

            assertThat(convertDaysToHybridCalendar(gregorianDays)).isEqualTo(hybridDays);
            assertThat(convertDaysToProlepticGregorian(hybridDays)).isEqualTo(gregorianDays);
        });
    }

    @Test
    void testRebaseNotExistedDatesInHybridCalendar()
    {
        Map<String, String> dates = ImmutableMap.<String, String>builder()
                .put("1582-10-04", "1582-10-04")
                .put("1582-10-05", "1582-10-15")
                .put("1582-10-06", "1582-10-15")
                .put("1582-10-07", "1582-10-15")
                .put("1582-10-08", "1582-10-15")
                .put("1582-10-09", "1582-10-15")
                .put("1582-10-11", "1582-10-15")
                .put("1582-10-12", "1582-10-15")
                .put("1582-10-13", "1582-10-15")
                .put("1582-10-14", "1582-10-15")
                .put("1582-10-15", "1582-10-15")
                .buildOrThrow();

        dates.forEach((gregDate, hybridDate) -> {
            int hybridDays = dateInStringToHybridDays(hybridDate);
            int gregorianDays = (int) LocalDate.parse(gregDate).toEpochDay();
            int actualHybridDays = convertDaysToHybridCalendar(gregorianDays);
            assertThat(actualHybridDays).isEqualTo(hybridDays);
        });
    }
}
