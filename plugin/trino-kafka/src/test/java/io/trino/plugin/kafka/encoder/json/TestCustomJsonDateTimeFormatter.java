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

import static io.trino.plugin.kafka.encoder.json.format.DateTimeFormat.CUSTOM_DATE_TIME;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.testing.DateTimeTestingUtils.sqlDateOf;
import static io.trino.testing.DateTimeTestingUtils.sqlTimeOf;
import static io.trino.testing.DateTimeTestingUtils.sqlTimeWithTimeZoneOf;
import static io.trino.testing.DateTimeTestingUtils.sqlTimestampOf;
import static io.trino.testing.DateTimeTestingUtils.sqlTimestampWithTimeZoneOf;
import static org.testng.Assert.assertEquals;

public class TestCustomJsonDateTimeFormatter
{
    private static JsonDateTimeFormatter getFormatter(String formatHint)
    {
        return CUSTOM_DATE_TIME.getFormatter(Optional.of(formatHint));
    }

    private static void testDate(SqlDate value, String formatHint, String expectedLiteral)
    {
        String actualLiteral = getFormatter(formatHint).formatDate(value);
        assertEquals(actualLiteral, expectedLiteral);
    }

    private static void testTime(SqlTime value, String formatHint, int precision, String expectedLiteral)
    {
        String actualLiteral = getFormatter(formatHint).formatTime(value, precision);
        assertEquals(actualLiteral, expectedLiteral);
    }

    private static void testTimeWithTZ(SqlTimeWithTimeZone value, String formatHint, String expectedLiteral)
    {
        String actualLiteral = getFormatter(formatHint).formatTimeWithZone(value);
        assertEquals(actualLiteral, expectedLiteral);
    }

    private static void testTimestamp(SqlTimestamp value, String formatHint, String expectedLiteral)
    {
        String actualLiteral = getFormatter(formatHint).formatTimestamp(value);
        assertEquals(actualLiteral, expectedLiteral);
    }

    private static void testTimestampWithTZ(SqlTimestampWithTimeZone value, String formatHint, String expectedLiteral)
    {
        String actualLiteral = getFormatter(formatHint).formatTimestampWithZone(value);
        assertEquals(actualLiteral, expectedLiteral);
    }

    @Test
    public void testCustomDateTimeFunctions()
    {
        testDate(sqlDateOf(2020, 8, 14), "yyyy-MM-dd", "2020-08-14");
        testDate(sqlDateOf(1970, 1, 1), "yyyy-MM-dd", "1970-01-01");
        testDate(sqlDateOf(1900, 1, 1), "yyyy-MM-dd", "1900-01-01");
        testDate(sqlDateOf(3001, 1, 1), "yyyy-MM-dd", "3001-01-01");

        testTime(sqlTimeOf(3, 15, 36, 25, 123000000), "HH:mm:ss.SSS", 3, "15:36:25.123");
        testTime(sqlTimeOf(3, 15, 36, 25, 0), "HH:mm:ss", 3, "15:36:25");
        testTime(sqlTimeOf(3, 8, 12, 45, 987000000), "HH:mm:ss.SSS", 3, "08:12:45.987");
        testTime(sqlTimeOf(3, 0, 0, 0, 0), "HH:mm:ss.SSS", 3, "00:00:00.000");
        testTime(sqlTimeOf(3, 23, 59, 59, 999000000), "HH:mm:ss.SSS", 3, "23:59:59.999");

        testTimeWithTZ(sqlTimeWithTimeZoneOf(3, 10, 23, 35, 123000000, 0), "HH:mm:ss.SSS Z", "10:23:35.123 +0000");
        testTimeWithTZ(sqlTimeWithTimeZoneOf(3, 0, 0, 0, 0, 0), "HH:mm:ss.SSS Z", "00:00:00.000 +0000");
        testTimeWithTZ(sqlTimeWithTimeZoneOf(3, 10, 23, 35, 123000000, 2 * 60), "HH:mm:ss.SSS Z", "10:23:35.123 +0200");
        testTimeWithTZ(sqlTimeWithTimeZoneOf(3, 10, 23, 35, 123000000, 10 * 60), "HH:mm:ss.SSS Z", "10:23:35.123 +1000");
        testTimeWithTZ(sqlTimeWithTimeZoneOf(3, 10, 23, 35, 123000000, -10 * 60), "HH:mm:ss.SSS Z", "10:23:35.123 -1000");
        testTimeWithTZ(sqlTimeWithTimeZoneOf(3, 23, 59, 59, 999000000, 0), "HH:mm:ss.SSS Z", "23:59:59.999 +0000");
        testTimeWithTZ(sqlTimeWithTimeZoneOf(3, 12, 34, 56, 789000000, -35), "HH:mm:ss.SSS Z", "12:34:56.789 -0035");

        testTimestamp(sqlTimestampOf(3, 2020, 8, 18, 12, 38, 29, 123), "yyyy-dd-MM HH:mm:ss.SSS", "2020-18-08 12:38:29.123");
        testTimestamp(sqlTimestampOf(3, 1970, 1, 1, 0, 0, 0, 0), "yyyy-dd-MM HH:mm:ss.SSS", "1970-01-01 00:00:00.000");
        testTimestamp(sqlTimestampOf(3, 1800, 8, 18, 12, 38, 29, 123), "yyyy-dd-MM HH:mm:ss.SSS", "1800-18-08 12:38:29.123");

        testTimestampWithTZ(sqlTimestampWithTimeZoneOf(3, 2020, 8, 19, 12, 23, 41, 123000000, UTC_KEY), "yyyy-dd-MM HH:mm:ss.SSS Z", "2020-19-08 12:23:41.123 +0000");
        testTimestampWithTZ(sqlTimestampWithTimeZoneOf(3, 2020, 8, 19, 12, 23, 41, 123000000, TimeZoneKey.getTimeZoneKey("America/New_York")), "yyyy-dd-MM HH:mm:ss.SSS Z", "2020-19-08 12:23:41.123 -0400");
        testTimestampWithTZ(sqlTimestampWithTimeZoneOf(3, 1800, 8, 19, 12, 23, 41, 123000000, UTC_KEY), "yyyy-dd-MM HH:mm:ss.SSS Z", "1800-19-08 12:23:41.123 +0000");
        testTimestampWithTZ(sqlTimestampWithTimeZoneOf(3, 2020, 8, 19, 12, 23, 41, 123000000, TimeZoneKey.getTimeZoneKey("Asia/Hong_Kong")), "yyyy-dd-MM HH:mm:ss.SSS Z", "2020-19-08 12:23:41.123 +0800");
        testTimestampWithTZ(sqlTimestampWithTimeZoneOf(3, 2020, 8, 19, 12, 23, 41, 123000000, TimeZoneKey.getTimeZoneKey("Africa/Mogadishu")), "yyyy-dd-MM HH:mm:ss.SSS Z", "2020-19-08 12:23:41.123 +0300");
        testTimestampWithTZ(sqlTimestampWithTimeZoneOf(3, 1970, 1, 1, 0, 0, 0, 0, UTC_KEY), "yyyy-dd-MM HH:mm:ss.SSS Z", "1970-01-01 00:00:00.000 +0000");
    }
}
