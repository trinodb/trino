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
package io.trino.plugin.bigquery;

import io.trino.spi.type.TimeZoneKey;
import org.testng.annotations.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.type.Decimals.encodeScaledValue;
import static io.trino.spi.type.LongTimestampWithTimeZone.fromEpochSecondsAndFraction;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static java.math.BigDecimal.ONE;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Use TestBigQueryTypeMapping
 */
@Deprecated
public class TestBigQueryType
{
    @Test
    public void testTimeToStringConverter()
    {
        assertThat(BigQueryType.timeToStringConverter(
                Long.valueOf(303497217825L)))
                .isEqualTo("'00:00:00.303497'");
    }

    @Test
    public void testTimestampToStringConverter()
    {
        assertThat(BigQueryType.timestampToStringConverter(
                fromEpochSecondsAndFraction(1585658096, 123_456_000_000L, UTC_KEY)))
                .isEqualTo("2020-03-31 12:34:56.123456");
        assertThat(BigQueryType.timestampToStringConverter(
                fromEpochSecondsAndFraction(1585658096, 123_456_000_000L, TimeZoneKey.getTimeZoneKey("Asia/Kathmandu"))))
                .isEqualTo("2020-03-31 12:34:56.123456");
    }

    @Test
    public void testDateToStringConverter()
    {
        assertThat(BigQueryType.dateToStringConverter(
                Long.valueOf(18352)))
                .isEqualTo("'2020-03-31'");
    }

    @Test
    public void testStringToStringConverter()
    {
        assertThat(BigQueryType.stringToStringConverter(
                utf8Slice("test")))
                .isEqualTo("'test'");

        assertThat(BigQueryType.stringToStringConverter(
                utf8Slice("test's test")))
                .isEqualTo("'test\\'s test'");
    }

    @Test
    public void testNumericToStringConverter()
    {
        assertThat(BigQueryType.numericToStringConverter(
                encodeScaledValue(ONE, 9)))
                .isEqualTo("1.000000000");
    }

    @Test
    public void testBytesToStringConverter()
    {
        assertThat(BigQueryType.bytesToStringConverter(
                wrappedBuffer((byte) 1, (byte) 2, (byte) 3, (byte) 4)))
                .isEqualTo("FROM_BASE64('AQIDBA==')");
    }
}
