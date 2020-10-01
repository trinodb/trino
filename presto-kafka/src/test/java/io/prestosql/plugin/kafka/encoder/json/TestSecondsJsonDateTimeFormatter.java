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
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Optional;

import static io.prestosql.plugin.kafka.encoder.json.format.DateTimeFormat.SECONDS_SINCE_EPOCH;
import static io.prestosql.testing.DateTimeTestingUtils.sqlTimeOf;
import static io.prestosql.testing.DateTimeTestingUtils.sqlTimestampOf;
import static io.prestosql.testing.assertions.Assert.assertEquals;

public class TestSecondsJsonDateTimeFormatter
{
    private JsonDateTimeFormatter getFormatter()
    {
        return SECONDS_SINCE_EPOCH.getFormatter(Optional.empty());
    }

    private void testTime(SqlTime value, int precision, long actualSeconds)
    {
        String formattedStr = getFormatter().formatTime(value, precision);
        assertEquals(Long.parseLong(formattedStr), actualSeconds);
    }

    private void testTimestamp(SqlTimestamp value, long actualSeconds)
    {
        String formattedStr = getFormatter().formatTimestamp(value);
        assertEquals(Long.parseLong(formattedStr), actualSeconds);
    }

    @Test
    public void testSecondsDateTimeFunctions()
    {
        testTime(sqlTimeOf(3, 15, 36, 25, 0), 3, LocalTime.of(15, 36, 25, 0).toSecondOfDay());
        testTime(sqlTimeOf(3, 0, 0, 0, 0), 3, 0);
        testTime(sqlTimeOf(3, 23, 59, 59, 0), 3, LocalTime.of(23, 59, 59, 0).toSecondOfDay());

        testTimestamp(sqlTimestampOf(3, 2020, 8, 18, 12, 38, 29, 0), LocalDateTime.of(2020, 8, 18, 12, 38, 29, 0).toEpochSecond(ZoneOffset.UTC));
        testTimestamp(sqlTimestampOf(3, 1970, 1, 1, 0, 0, 0, 0), 0);
        testTimestamp(sqlTimestampOf(3, 1800, 8, 18, 12, 38, 29, 0), LocalDateTime.of(1800, 8, 18, 12, 38, 29, 0).toEpochSecond(ZoneOffset.UTC));
    }
}
