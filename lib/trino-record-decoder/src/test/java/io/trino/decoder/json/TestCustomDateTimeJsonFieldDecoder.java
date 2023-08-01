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
package io.trino.decoder.json;

import com.google.common.collect.ImmutableSet;
import io.airlift.json.ObjectMapperProvider;
import io.trino.decoder.DecoderTestColumnHandle;
import io.trino.decoder.RowDecoderSpec;
import io.trino.spi.TrinoException;
import org.testng.annotations.Test;

import static io.trino.decoder.util.DecoderTestUtil.TESTING_SESSION;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.packTimeWithTimeZone;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimeWithTimeZoneType.TIME_TZ_MILLIS;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKeyForOffset;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestCustomDateTimeJsonFieldDecoder
{
    private final JsonFieldDecoderTester timestampTester = new JsonFieldDecoderTester("custom-date-time", "MM/yyyy/dd H:m:s");
    private final JsonFieldDecoderTester timestampWithTimeZoneTester = new JsonFieldDecoderTester("custom-date-time", "MM/yyyy/dd H:m:s Z");
    private final JsonFieldDecoderTester timeTester = new JsonFieldDecoderTester("custom-date-time", "mm:HH:ss");
    private final JsonFieldDecoderTester timeWTZTester = new JsonFieldDecoderTester("custom-date-time", "HH:mm:ss.SSSZ");
    private final JsonFieldDecoderTester dateTester = new JsonFieldDecoderTester("custom-date-time", "MM/yyyy/dd");
    private final JsonFieldDecoderTester timeJustHourTester = new JsonFieldDecoderTester("custom-date-time", "HH");

    @Test
    public void testDecode()
    {
        timestampTester.assertDecodedAs("\"02/2018/19 9:20:11\"", TIMESTAMP_MILLIS, 1_519_032_011_000_000L);
        timestampWithTimeZoneTester.assertDecodedAs("\"02/2018/19 11:20:11 +02:00\"", TIMESTAMP_MILLIS, 1_519_032_011_000_000L);
        timestampTester.assertDecodedAs("\"02/2018/19 9:20:11\"", TIMESTAMP_TZ_MILLIS, packDateTimeWithZone(1519032011000L, UTC_KEY));
        timestampWithTimeZoneTester.assertDecodedAs("\"02/2018/19 11:20:11 +02:00\"", TIMESTAMP_TZ_MILLIS, packDateTimeWithZone(1519032011000L, getTimeZoneKeyForOffset(120))); // TODO: extract TZ from pattern
        timeTester.assertDecodedAs("\"15:13:18\"", TIME_MILLIS, 47_718_000_000_000_000L);
        timeJustHourTester.assertDecodedAs("\"15\"", TIME_MILLIS, 54_000_000_000_000_000L);
        timeJustHourTester.assertDecodedAs("15", TIME_MILLIS, 54_000_000_000_000_000L);
        timeTester.assertDecodedAs("\"15:13:18\"", TIME_TZ_MILLIS, packTimeWithTimeZone(47_718_000_000_000L, 0));
        timeWTZTester.assertDecodedAs("\"15:13:18.123-04:00\"", TIME_TZ_MILLIS, packTimeWithTimeZone(54_798_123_000_000L, -4 * 60));
        timeWTZTester.assertDecodedAs("\"15:13:18.123+08:00\"", TIME_TZ_MILLIS, packTimeWithTimeZone(54_798_123_000_000L, 8 * 60));
        dateTester.assertDecodedAs("\"02/2018/11\"", DATE, 17573);
    }

    @Test
    public void testDecodeNulls()
    {
        dateTester.assertDecodedAsNull("null", DATE);
        dateTester.assertMissingDecodedAsNull(DATE);

        timeTester.assertDecodedAsNull("null", TIME_MILLIS);
        timeTester.assertMissingDecodedAsNull(TIME_MILLIS);

        timeTester.assertDecodedAsNull("null", TIME_TZ_MILLIS);
        timeTester.assertMissingDecodedAsNull(TIME_TZ_MILLIS);

        timestampTester.assertDecodedAsNull("null", TIMESTAMP_MILLIS);
        timestampTester.assertMissingDecodedAsNull(TIMESTAMP_MILLIS);

        timestampTester.assertDecodedAsNull("null", TIMESTAMP_TZ_MILLIS);
        timestampTester.assertMissingDecodedAsNull(TIMESTAMP_TZ_MILLIS);
    }

    @Test
    public void testDecodeInvalid()
    {
        timestampTester.assertInvalidInput("1", TIMESTAMP_MILLIS, "\\Qcould not parse value '1' as 'timestamp(3)' for column 'some_column'\\E");
        timestampTester.assertInvalidInput("{}", TIMESTAMP_MILLIS, "\\Qcould not parse non-value node as 'timestamp(3)' for column 'some_column'\\E");
        timestampTester.assertInvalidInput("\"a\"", TIMESTAMP_MILLIS, "\\Qcould not parse value 'a' as 'timestamp(3)' for column 'some_column'\\E");
        timestampTester.assertInvalidInput("\"15:13:18\"", TIMESTAMP_MILLIS, "\\Qcould not parse value '15:13:18' as 'timestamp(3)' for column 'some_column'\\E");
        timestampTester.assertInvalidInput("\"02/2018/11\"", TIMESTAMP_MILLIS, "\\Qcould not parse value '02/2018/11' as 'timestamp(3)' for column 'some_column'\\E");
    }

    @Test
    public void testInvalidFormatHint()
    {
        DecoderTestColumnHandle columnHandle = new DecoderTestColumnHandle(
                0,
                "some_column",
                TIMESTAMP_MILLIS,
                "mappedField",
                "custom-date-time",
                "XXMM/yyyy/dd H:m:sXX",
                false,
                false,
                false);
        assertThatThrownBy(() -> new JsonRowDecoderFactory(new ObjectMapperProvider().get()).create(TESTING_SESSION, new RowDecoderSpec(JsonRowDecoder.NAME, emptyMap(), ImmutableSet.of(columnHandle))))
                .isInstanceOf(TrinoException.class)
                .hasMessageMatching("invalid Joda Time pattern 'XXMM/yyyy/dd H:m:sXX' passed as format hint for column 'some_column'");
    }
}
