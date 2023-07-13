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
import io.trino.spi.type.SqlDate;
import io.trino.spi.type.SqlTime;
import io.trino.spi.type.SqlTimeWithTimeZone;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.SqlTimestampWithTimeZone;
import io.trino.spi.type.TimeZoneKey;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.plugin.kafka.encoder.json.format.DateTimeFormat.ISO8601;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.testing.DateTimeTestingUtils.sqlDateOf;
import static io.trino.testing.DateTimeTestingUtils.sqlTimeOf;
import static io.trino.testing.DateTimeTestingUtils.sqlTimeWithTimeZoneOf;
import static io.trino.testing.DateTimeTestingUtils.sqlTimestampOf;
import static io.trino.testing.DateTimeTestingUtils.sqlTimestampWithTimeZoneOf;
import static org.testng.Assert.assertEquals;

public class TestISO8601JsonDateTimeFormatter
{
    private static JsonDateTimeFormatter getFormatter()
    {
        return ISO8601.getFormatter(Optional.empty());
    }

    private static void testDate(SqlDate value, String expectedLiteral)
    {
        String actualLiteral = getFormatter().formatDate(value);
        assertEquals(actualLiteral, expectedLiteral);
    }

    private static void testTime(SqlTime value, int precision, String expectedLiteral)
    {
        String actualLiteral = getFormatter().formatTime(value, precision);
        assertEquals(actualLiteral, expectedLiteral);
    }

    private static void testTimeWithTZ(SqlTimeWithTimeZone value, String expectedLiteral)
    {
        String actualLiteral = getFormatter().formatTimeWithZone(value);
        assertEquals(actualLiteral, expectedLiteral);
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
    public void testISO8601DateTimeFunctions()
    {
        testDate(sqlDateOf(2020, 8, 14), "2020-08-14");
        testDate(sqlDateOf(1970, 1, 1), "1970-01-01");
        testDate(sqlDateOf(1900, 1, 1), "1900-01-01");
        testDate(sqlDateOf(3001, 1, 1), "3001-01-01");

        testTime(sqlTimeOf(3, 15, 36, 25, 123000000), 3, "15:36:25.123");
        testTime(sqlTimeOf(3, 15, 36, 25, 0), 3, "15:36:25");
        testTime(sqlTimeOf(3, 8, 12, 45, 987000000), 3, "08:12:45.987");
        testTime(sqlTimeOf(3, 0, 0, 0, 0), 3, "00:00");
        testTime(sqlTimeOf(3, 23, 59, 59, 999000000), 3, "23:59:59.999");

        testTimeWithTZ(sqlTimeWithTimeZoneOf(3, 10, 23, 35, 123000000, 0), "10:23:35.123Z");
        testTimeWithTZ(sqlTimeWithTimeZoneOf(3, 0, 0, 0, 0, 0), "00:00:00Z");
        testTimeWithTZ(sqlTimeWithTimeZoneOf(3, 10, 23, 35, 123000000, 2 * 60), "10:23:35.123+02:00");
        testTimeWithTZ(sqlTimeWithTimeZoneOf(3, 10, 23, 35, 123000000, 10 * 60), "10:23:35.123+10:00");
        testTimeWithTZ(sqlTimeWithTimeZoneOf(3, 10, 23, 35, 123000000, -10 * 60), "10:23:35.123-10:00");
        testTimeWithTZ(sqlTimeWithTimeZoneOf(3, 23, 59, 59, 999000000, 0), "23:59:59.999Z");
        testTimeWithTZ(sqlTimeWithTimeZoneOf(3, 12, 34, 56, 789000000, -35), "12:34:56.789-00:35");

        testTimestamp(sqlTimestampOf(3, 2020, 8, 18, 12, 38, 29, 123), "2020-08-18T12:38:29.123");
        testTimestamp(sqlTimestampOf(3, 1970, 1, 1, 0, 0, 0, 0), "1970-01-01T00:00");
        testTimestamp(sqlTimestampOf(3, 1800, 8, 18, 12, 38, 29, 123), "1800-08-18T12:38:29.123");

        testTimestampWithTZ(sqlTimestampWithTimeZoneOf(3, 2020, 8, 19, 12, 23, 41, 123000000, UTC_KEY), "2020-08-19T12:23:41.123Z");
        testTimestampWithTZ(sqlTimestampWithTimeZoneOf(3, 2020, 8, 19, 12, 23, 41, 123000000, TimeZoneKey.getTimeZoneKey("America/New_York")), "2020-08-19T12:23:41.123-04:00");
        testTimestampWithTZ(sqlTimestampWithTimeZoneOf(3, 1800, 8, 19, 12, 23, 41, 123000000, UTC_KEY), "1800-08-19T12:23:41.123Z");
        testTimestampWithTZ(sqlTimestampWithTimeZoneOf(3, 2020, 8, 19, 12, 23, 41, 123000000, TimeZoneKey.getTimeZoneKey("Asia/Hong_Kong")), "2020-08-19T12:23:41.123+08:00");
        testTimestampWithTZ(sqlTimestampWithTimeZoneOf(3, 2020, 8, 19, 12, 23, 41, 123000000, TimeZoneKey.getTimeZoneKey("Africa/Mogadishu")), "2020-08-19T12:23:41.123+03:00");
        testTimestampWithTZ(sqlTimestampWithTimeZoneOf(3, 1970, 1, 1, 0, 0, 0, 0, UTC_KEY), "1970-01-01T00:00:00Z");
    }
}
