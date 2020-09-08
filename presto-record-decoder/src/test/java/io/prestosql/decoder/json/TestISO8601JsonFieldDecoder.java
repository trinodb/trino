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
package io.prestosql.decoder.json;

import io.prestosql.spi.type.Type;
import org.testng.annotations.Test;

import static io.prestosql.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.prestosql.spi.type.DateTimeEncoding.packTimeWithTimeZone;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TimeZoneKey.UTC_KEY;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static java.util.Arrays.asList;

public class TestISO8601JsonFieldDecoder
{
    private JsonFieldDecoderTester tester = new JsonFieldDecoderTester("iso8601");

    @Test
    public void testDecode()
    {
        tester.assertDecodedAs("\"2018-02-19T09:20:11\"", TIMESTAMP_MILLIS, 1_519_032_011_000_000L);
        tester.assertDecodedAs("\"2018-02-19T09:20:11Z\"", TIMESTAMP_MILLIS, 1_519_032_011_000_000L);
        tester.assertDecodedAs("\"2018-02-19T09:20:11+10:00\"", TIMESTAMP_MILLIS, 1_519_032_011_000_000L);
        tester.assertDecodedAs("\"13:15:18\"", TIME, 47_718_000_000_000_000L);
        tester.assertDecodedAs("\"13:15\"", TIME, 47_700_000_000_000_000L);
        tester.assertDecodedAs("\"13:15:18Z\"", TIME, 47_718_000_000_000_000L);
        tester.assertDecodedAs("\"13:15Z\"", TIME, 47_700_000_000_000_000L);
        tester.assertDecodedAs("\"13:15:18+10:00\"", TIME, 47_718_000_000_000_000L);
        tester.assertDecodedAs("\"13:15+10:00\"", TIME, 47_700_000_000_000_000L);
        tester.assertDecodedAs("\"2018-02-11\"", DATE, 17573);
        tester.assertDecodedAs("\"2018-02-19T09:20:11Z\"", TIMESTAMP_WITH_TIME_ZONE, packDateTimeWithZone(1519032011000L, UTC_KEY));
        tester.assertDecodedAs("\"2018-02-19T12:20:11+03:00\"", TIMESTAMP_WITH_TIME_ZONE, packDateTimeWithZone(1519032011000L, "+03:00"));
        tester.assertDecodedAs("\"13:15:18Z\"", TIME_WITH_TIME_ZONE, packTimeWithTimeZone(47_718_000_000_000L, 0));
        tester.assertDecodedAs("\"13:15:18+10:00\"", TIME_WITH_TIME_ZONE, packTimeWithTimeZone(47_718_000_000_000L, 10 * 60));
        tester.assertDecodedAs("\"15:13:18.123-04:00\"", TIME_WITH_TIME_ZONE, packTimeWithTimeZone(54_798_123_000_000L, -4 * 60));
        tester.assertDecodedAs("\"15:13:18.123+08:00\"", TIME_WITH_TIME_ZONE, packTimeWithTimeZone(54_798_123_000_000L, 8 * 60));
    }

    @Test
    public void testDecodeNulls()
    {
        for (Type type : asList(DATE, TIME, TIME_WITH_TIME_ZONE, TIMESTAMP_MILLIS, TIMESTAMP_WITH_TIME_ZONE)) {
            tester.assertDecodedAsNull("null", type);
            tester.assertMissingDecodedAsNull(type);
        }
    }

    @Test
    public void testDecodeInvalid()
    {
        tester.assertInvalidInput("1", TIMESTAMP_MILLIS, "\\Qcould not parse value '1' as 'timestamp(3)' for column 'some_column'\\E");
        tester.assertInvalidInput("{}", TIMESTAMP_MILLIS, "\\Qcould not parse non-value node as 'timestamp(3)' for column 'some_column'\\E");
        tester.assertInvalidInput("\"a\"", TIMESTAMP_MILLIS, "\\Qcould not parse value 'a' as 'timestamp(3)' for column 'some_column'\\E");
        tester.assertInvalidInput("1", TIMESTAMP_MILLIS, "\\Qcould not parse value '1' as 'timestamp(3)' for column 'some_column'\\E");

        tester.assertInvalidInput("\"2018-02-19T09:20:11\"", DATE, "could not parse value '2018-02-19T09:20:11' as 'date' for column 'some_column'");
        tester.assertInvalidInput("\"2018-02-19T09:20:11Z\"", DATE, "could not parse value '2018-02-19T09:20:11Z' as 'date' for column 'some_column'");
        tester.assertInvalidInput("\"09:20:11Z\"", DATE, "could not parse value '09:20:11Z' as 'date' for column 'some_column'");
        tester.assertInvalidInput("\"09:20:11\"", DATE, "could not parse value '09:20:11' as 'date' for column 'some_column'");

        tester.assertInvalidInput("\"2018-02-19T09:20:11\"", TIMESTAMP_WITH_TIME_ZONE, "\\Qcould not parse value '2018-02-19T09:20:11' as 'timestamp(3) with time zone' for column 'some_column'\\E");
        tester.assertInvalidInput("\"09:20:11\"", TIMESTAMP_WITH_TIME_ZONE, "\\Qcould not parse value '09:20:11' as 'timestamp(3) with time zone' for column 'some_column'\\E");
        tester.assertInvalidInput("\"09:20:11Z\"", TIMESTAMP_WITH_TIME_ZONE, "\\Qcould not parse value '09:20:11Z' as 'timestamp(3) with time zone' for column 'some_column'\\E");
        tester.assertInvalidInput("\"2018-02-19\"", TIMESTAMP_WITH_TIME_ZONE, "\\Qcould not parse value '2018-02-19' as 'timestamp(3) with time zone' for column 'some_column'\\E");

        tester.assertInvalidInput("\"2018-02-19T09:20:11\"", TIME, "\\Qcould not parse value '2018-02-19T09:20:11' as 'time(3)' for column 'some_column'\\E");
        tester.assertInvalidInput("\"2018-02-19T09:20:11Z\"", TIME, "\\Qcould not parse value '2018-02-19T09:20:11Z' as 'time(3)' for column 'some_column'\\E");
        tester.assertInvalidInput("\"2018-02-19\"", TIME, "\\Qcould not parse value '2018-02-19' as 'time(3)' for column 'some_column'\\E");
        tester.assertInvalidInput("\"2018-02-19Z\"", TIME, "\\Qcould not parse value '2018-02-19Z' as 'time(3)' for column 'some_column'\\E");

        tester.assertInvalidInput("\"2018-02-19T09:20:11\"", TIME_WITH_TIME_ZONE, "\\Qcould not parse value '2018-02-19T09:20:11' as 'time(3) with time zone' for column 'some_column'\\E");
        tester.assertInvalidInput("\"2018-02-19T09:20:11Z\"", TIME_WITH_TIME_ZONE, "\\Qcould not parse value '2018-02-19T09:20:11Z' as 'time(3) with time zone' for column 'some_column'\\E");
        tester.assertInvalidInput("\"09:20:11\"", TIME_WITH_TIME_ZONE, "\\Qcould not parse value '09:20:11' as 'time(3) with time zone' for column 'some_column'\\E");
        tester.assertInvalidInput("\"2018-02-19\"", TIME_WITH_TIME_ZONE, "\\Qcould not parse value '2018-02-19' as 'time(3) with time zone' for column 'some_column'\\E");
    }
}
