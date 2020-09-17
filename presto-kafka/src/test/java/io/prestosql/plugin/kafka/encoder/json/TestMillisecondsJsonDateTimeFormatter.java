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
package io.prestosql.plugin.kafka.encoder.json;

import io.prestosql.plugin.kafka.encoder.json.format.JsonDateTimeFormatter;
import io.prestosql.spi.type.SqlTime;
import io.prestosql.spi.type.SqlTimestamp;
import org.testng.annotations.Test;

import java.time.LocalDateTime;
import java.util.Optional;

import static io.prestosql.plugin.kafka.encoder.json.format.DateTimeFormat.MILLISECONDS_SINCE_EPOCH;
import static io.prestosql.plugin.kafka.encoder.json.format.util.TimeConversions.scaleNanosToMillis;
import static io.prestosql.testing.DateTimeTestingUtils.sqlTimeOf;
import static io.prestosql.testing.DateTimeTestingUtils.sqlTimestampOf;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static java.time.temporal.ChronoField.EPOCH_DAY;
import static java.time.temporal.ChronoField.NANO_OF_DAY;
import static java.util.concurrent.TimeUnit.DAYS;

public class TestMillisecondsJsonDateTimeFormatter
{
    private JsonDateTimeFormatter getFormatter()
    {
        return MILLISECONDS_SINCE_EPOCH.getFormatter(Optional.empty());
    }

    private void testTime(SqlTime value, int precision, long actualMillis)
    {
        String formattedStr = getFormatter().formatTime(value, precision);
        assertEquals(Long.parseLong(formattedStr), actualMillis);
    }

    private void testTimestamp(SqlTimestamp value, long actualMillis)
    {
        String formattedStr = getFormatter().formatTimestamp(value);
        assertEquals(Long.parseLong(formattedStr), actualMillis);
    }

    private long getMillisFromTime(int hour, int minuteOfHour, int secondOfMinute, int nanoOfSecond)
    {
        return getMillisFromDateTime(1970, 1, 1, hour, minuteOfHour, secondOfMinute, nanoOfSecond);
    }

    private long getMillisFromDateTime(int year, int monthOfYear, int dayOfMonth, int hourOfDay, int minuteOfHour, int secondOfMinute, int nanoOfSecond)
    {
        LocalDateTime localDateTime = LocalDateTime.of(year, monthOfYear, dayOfMonth, hourOfDay, minuteOfHour, secondOfMinute, nanoOfSecond);
        return DAYS.toMillis(localDateTime.getLong(EPOCH_DAY)) + scaleNanosToMillis(localDateTime.getLong(NANO_OF_DAY));
    }

    @Test
    public void testMillisecondsDateTimeFunctions()
    {
        testTime(sqlTimeOf(3, 15, 36, 25, 123000000), 3, getMillisFromTime(15, 36, 25, 123000000));
        testTime(sqlTimeOf(3, 15, 36, 25, 0), 3, getMillisFromTime(15, 36, 25, 0));

        testTimestamp(sqlTimestampOf(3, 2020, 8, 18, 12, 38, 29, 123), getMillisFromDateTime(2020, 8, 18, 12, 38, 29, 123000000));
        testTimestamp(sqlTimestampOf(3, 1970, 1, 1, 0, 0, 0, 0), 0);
        testTimestamp(sqlTimestampOf(3, 1800, 8, 18, 12, 38, 29, 123), getMillisFromDateTime(1800, 8, 18, 12, 38, 29, 123000000));
    }
}
