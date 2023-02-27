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
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.SqlTimestampWithTimeZone;
import io.trino.spi.type.TimeZoneKey;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.plugin.kafka.encoder.json.format.DateTimeFormat.RFC2822;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.testing.DateTimeTestingUtils.sqlTimestampOf;
import static io.trino.testing.DateTimeTestingUtils.sqlTimestampWithTimeZoneOf;
import static org.testng.Assert.assertEquals;

public class TestRFC2822JsonDateTimeFormatter
{
    private static JsonDateTimeFormatter getFormatter()
    {
        return RFC2822.getFormatter(Optional.empty());
    }

    private static void testTimestamp(SqlTimestamp value, String expectedLiteral)
    {
        String actualLiteral = getFormatter().formatTimestamp(value);
        assertEquals(actualLiteral, expectedLiteral);
    }

    private static void testTimestampWithTZ(SqlTimestampWithTimeZone value, String expectedLiteral)
    {
        String actualLiteral = getFormatter().formatTimestampWithZone(value);
        assertEquals(actualLiteral, expectedLiteral);
    }

    @Test
    public void testRFC2822DateTimeFunctions()
    {
        testTimestamp(sqlTimestampOf(3, 2020, 8, 18, 12, 38, 29, 0), "Tue Aug 18 12:38:29 +0000 2020");
        testTimestamp(sqlTimestampOf(3, 1970, 1, 1, 0, 0, 0, 0), "Thu Jan 01 00:00:00 +0000 1970");
        testTimestamp(sqlTimestampOf(3, 1800, 8, 18, 12, 38, 29, 0), "Mon Aug 18 12:38:29 +0000 1800");

        testTimestampWithTZ(sqlTimestampWithTimeZoneOf(3, 2020, 8, 19, 12, 23, 41, 0, UTC_KEY), "Wed Aug 19 12:23:41 +0000 2020");
        testTimestampWithTZ(sqlTimestampWithTimeZoneOf(3, 2020, 8, 19, 12, 23, 41, 0, TimeZoneKey.getTimeZoneKey("America/New_York")), "Wed Aug 19 12:23:41 -0400 2020");
        testTimestampWithTZ(sqlTimestampWithTimeZoneOf(3, 1800, 8, 19, 12, 23, 41, 0, TimeZoneKey.getTimeZoneKey("America/New_York")), "Tue Aug 19 12:23:41 -0456 1800");
        testTimestampWithTZ(sqlTimestampWithTimeZoneOf(3, 2020, 8, 19, 12, 23, 41, 0, TimeZoneKey.getTimeZoneKey("Asia/Hong_Kong")), "Wed Aug 19 12:23:41 +0800 2020");
        testTimestampWithTZ(sqlTimestampWithTimeZoneOf(3, 2020, 8, 19, 12, 23, 41, 0, TimeZoneKey.getTimeZoneKey("Africa/Mogadishu")), "Wed Aug 19 12:23:41 +0300 2020");
        testTimestampWithTZ(sqlTimestampWithTimeZoneOf(3, 1970, 1, 1, 0, 0, 0, 0, UTC_KEY), "Thu Jan 01 00:00:00 +0000 1970");
    }
}
