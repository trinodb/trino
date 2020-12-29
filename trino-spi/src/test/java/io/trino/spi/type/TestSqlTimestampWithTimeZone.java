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
package io.trino.spi.type;

import org.testng.annotations.Test;

import java.time.ZonedDateTime;

import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static org.testng.Assert.assertEquals;

public class TestSqlTimestampWithTimeZone
{
    @Test
    public void testToZonedDateTime()
    {
        assertEquals(
                new SqlTimestampWithTimeZone(3, 0, 0, UTC_KEY).toZonedDateTime(),
                ZonedDateTime.parse("1970-01-01T00:00Z[UTC]"));

        assertEquals(
                new SqlTimestampWithTimeZone(9, 1234567890123L, 123_000_000, UTC_KEY).toZonedDateTime(),
                ZonedDateTime.parse("2009-02-13T23:31:30.123123Z[UTC]"));

        // non-UTC
        assertEquals(
                new SqlTimestampWithTimeZone(9, 1234567890123L, 123_000_000, TimeZoneKey.getTimeZoneKey("Europe/Warsaw")).toZonedDateTime(),
                ZonedDateTime.parse("2009-02-13T23:31:30.123123Z[Europe/Warsaw]"));

        // nanoseconds
        assertEquals(
                new SqlTimestampWithTimeZone(9, 1234567890123L, 123_456_000, UTC_KEY).toZonedDateTime(),
                ZonedDateTime.parse("2009-02-13T23:31:30.123123456Z[UTC]"));

        // picoseconds, rounding down
        assertEquals(
                new SqlTimestampWithTimeZone(12, 1234567890123L, 123_456_499, UTC_KEY).toZonedDateTime(),
                ZonedDateTime.parse("2009-02-13T23:31:30.123123456Z[UTC]"));

        // picoseconds, rounding up
        assertEquals(
                new SqlTimestampWithTimeZone(12, 1234567890123L, 123_456_500, UTC_KEY).toZonedDateTime(),
                ZonedDateTime.parse("2009-02-13T23:31:30.123123457Z[UTC]"));

        // rounding to next millisecond
        assertEquals(
                new SqlTimestampWithTimeZone(12, 1234567890123L, 999_999_999, UTC_KEY).toZonedDateTime(),
                ZonedDateTime.parse("2009-02-13T23:31:30.124Z[UTC]"));

        // rounding to next second
        assertEquals(
                new SqlTimestampWithTimeZone(12, 1234567890999L, 999_999_999, UTC_KEY).toZonedDateTime(),
                ZonedDateTime.parse("2009-02-13T23:31:31Z[UTC]"));

        // negative epoch
        assertEquals(
                new SqlTimestampWithTimeZone(9, -1234567890123L, 123_000_000, UTC_KEY).toZonedDateTime(),
                ZonedDateTime.parse("1930-11-18T00:28:29.877123Z[UTC]"));

        // negative epoch, nanoseconds
        assertEquals(
                new SqlTimestampWithTimeZone(9, -1234567890123L, 123_456_000, UTC_KEY).toZonedDateTime(),
                ZonedDateTime.parse("1930-11-18T00:28:29.877123456Z[UTC]"));

        // negative epoch, picoseconds, rounding down
        assertEquals(
                new SqlTimestampWithTimeZone(12, -1234567890123L, 123_456_499, UTC_KEY).toZonedDateTime(),
                ZonedDateTime.parse("1930-11-18T00:28:29.877123456Z[UTC]"));

        // negative epoch, picoseconds, rounding up
        assertEquals(
                new SqlTimestampWithTimeZone(12, -1234567890123L, 123_456_500, UTC_KEY).toZonedDateTime(),
                ZonedDateTime.parse("1930-11-18T00:28:29.877123457Z[UTC]"));

        // negative epoch, rounding to next millisecond
        assertEquals(
                new SqlTimestampWithTimeZone(12, -1234567890123L, 999_999_999, UTC_KEY).toZonedDateTime(),
                ZonedDateTime.parse("1930-11-18T00:28:29.878Z[UTC]"));

        // negative epoch, rounding to next second
        assertEquals(
                new SqlTimestampWithTimeZone(12, 1234567890999L, 999_999_999, UTC_KEY).toZonedDateTime(),
                ZonedDateTime.parse("2009-02-13T23:31:31Z[UTC]"));
    }
}
