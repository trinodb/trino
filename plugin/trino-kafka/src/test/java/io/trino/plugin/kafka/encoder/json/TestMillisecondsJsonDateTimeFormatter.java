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
package io.trino.plugin.kafka.encoder.json;

import io.trino.plugin.kafka.encoder.json.format.JsonDateTimeFormatter;
import io.trino.spi.type.SqlTimestampWithTimeZone;
import io.trino.spi.type.TimeZoneKey;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.Optional;

import static io.trino.plugin.kafka.encoder.json.format.DateTimeFormat.MILLISECONDS_SINCE_EPOCH;
import static io.trino.plugin.kafka.encoder.json.format.util.TimeConversions.scaleNanosToMillis;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.testing.DateTimeTestingUtils.sqlTimeOf;
import static io.trino.testing.DateTimeTestingUtils.sqlTimestampOf;
import static java.time.temporal.ChronoField.EPOCH_DAY;
import static java.time.temporal.ChronoField.MILLI_OF_DAY;
import static java.time.temporal.ChronoField.NANO_OF_DAY;
import static java.util.concurrent.TimeUnit.DAYS;
import static org.testng.Assert.assertEquals;

public class TestMillisecondsJsonDateTimeFormatter
{
    private static JsonDateTimeFormatter getFormatter()
    {
        return MILLISECONDS_SINCE_EPOCH.getFormatter(Optional.empty());
    }

    @Test(dataProvider = "testTimeProvider")
    public void testTime(LocalTime time)
    {
        String formatted = getFormatter().formatTime(sqlTimeOf(3, time), 3);
        assertEquals(Long.parseLong(formatted), time.getLong(MILLI_OF_DAY));
    }

    @DataProvider
    public Object[][] testTimeProvider()
    {
        return new Object[][] {
                {LocalTime.of(15, 36, 25, 123000000)},
                {LocalTime.of(15, 36, 25, 0)},
        };
    }

    @Test(dataProvider = "testTimestampProvider")
    public void testTimestamp(LocalDateTime dateTime)
    {
        String formattedStr = getFormatter().formatTimestamp(sqlTimestampOf(3, dateTime));
        assertEquals(Long.parseLong(formattedStr), DAYS.toMillis(dateTime.getLong(EPOCH_DAY)) + scaleNanosToMillis(dateTime.getLong(NANO_OF_DAY)));
    }

    @DataProvider
    public Object[][] testTimestampProvider()
    {
        return new Object[][] {
                {LocalDateTime.of(2020, 8, 18, 12, 38, 29, 123000000)},
                {LocalDateTime.of(1970, 1, 1, 0, 0, 0, 0)},
                {LocalDateTime.of(1800, 8, 18, 12, 38, 29, 123000000)},
        };
    }

    @Test(dataProvider = "testTimestampWithTimeZoneProvider")
    public void testTimestampWithTimeZone(ZonedDateTime zonedDateTime)
    {
        String formattedStr = getFormatter().formatTimestampWithZone(SqlTimestampWithTimeZone.fromInstant(3, zonedDateTime.toInstant(), zonedDateTime.getZone()));
        assertEquals(Long.parseLong(formattedStr), zonedDateTime.toInstant().toEpochMilli());
    }

    @DataProvider
    public Object[][] testTimestampWithTimeZoneProvider()
    {
        return new Object[][] {
                {ZonedDateTime.of(2020, 8, 18, 12, 38, 29, 123000000, UTC_KEY.getZoneId())},
                {ZonedDateTime.of(2020, 8, 18, 12, 38, 29, 123000000, TimeZoneKey.getTimeZoneKey("America/New_York").getZoneId())},
                {ZonedDateTime.of(1800, 8, 18, 12, 38, 29, 123000000, UTC_KEY.getZoneId())},
                {ZonedDateTime.of(2020, 8, 18, 12, 38, 29, 123000000, TimeZoneKey.getTimeZoneKey("Asia/Hong_Kong").getZoneId())},
                {ZonedDateTime.of(2020, 8, 18, 12, 38, 29, 123000000, TimeZoneKey.getTimeZoneKey("Africa/Mogadishu").getZoneId())},
                {ZonedDateTime.of(1970, 1, 1, 0, 0, 0, 0, UTC_KEY.getZoneId())},
        };
    }
}
