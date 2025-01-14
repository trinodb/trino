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

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.text.ParseException;

import static io.trino.plugin.hive.util.CalendarUtils.HYBRID_CALENDAR_DATE_FORMAT;
import static io.trino.plugin.hive.util.CalendarUtils.HYBRID_CALENDAR_DATE_TIME_FORMAT;
import static io.trino.plugin.hive.util.CalendarUtils.PROLEPTIC_CALENDAR_DATE_FORMAT;
import static io.trino.plugin.hive.util.CalendarUtils.PROLEPTIC_CALENDAR_DATE_TIME_FORMAT;
import static io.trino.plugin.hive.util.CalendarUtils.convertDaysToProlepticGregorian;
import static io.trino.plugin.hive.util.CalendarUtils.convertTimestampToProlepticGregorian;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
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
            int julianDays = toHybridDaysFromString(date);
            int gregorianDays = toProlepticDaysFromString(date);
            assertThat(convertDaysToProlepticGregorian(julianDays)).isEqualTo(gregorianDays);
        });
    }

    @Test
    void testConvertGregorianTimestampToAndFromHybridDays()
    {
        ImmutableList<String> timestamps = ImmutableList.of(
                "0001-01-01 15:15:15.123",
                "1000-01-01 15:15:15.123",
                "1582-10-04 15:15:15.123",
                "1582-10-15 15:15:15.123",
                "1788-09-10 15:15:15.123",
                "1888-12-31 15:15:15.123",
                "1969-12-31 15:15:15.123",
                "1970-01-01 15:15:15.123",
                "2024-03-30 15:15:15.123");

        timestamps.forEach(timestamp -> {
            try {
                long julianMillis = HYBRID_CALENDAR_DATE_TIME_FORMAT.get().parse(timestamp).getTime();
                long gregorianMillis = PROLEPTIC_CALENDAR_DATE_TIME_FORMAT.get().parse(timestamp).getTime();
                assertThat(convertTimestampToProlepticGregorian(julianMillis)).isEqualTo(gregorianMillis);
            }
            catch (ParseException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static int toHybridDaysFromString(String date)
    {
        try {
            return (int) MILLISECONDS.toDays(HYBRID_CALENDAR_DATE_FORMAT.get().parse(date).getTime());
        }
        catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    private static int toProlepticDaysFromString(String date)
    {
        try {
            return (int) MILLISECONDS.toDays(PROLEPTIC_CALENDAR_DATE_FORMAT.get().parse(date).getTime());
        }
        catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }
}
