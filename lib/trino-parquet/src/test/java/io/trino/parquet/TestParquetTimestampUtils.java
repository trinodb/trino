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
package io.trino.parquet;

import io.trino.plugin.base.type.DecodedTimestamp;
import io.trino.spi.TrinoException;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.parquet.io.api.Binary;
import org.testng.annotations.Test;

import java.time.LocalDateTime;

import static io.trino.parquet.ParquetTimestampUtils.decodeInt96Timestamp;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.time.ZoneOffset.UTC;
import static org.apache.hadoop.hive.ql.io.parquet.timestamp.NanoTimeUtils.getNanoTime;
import static org.testng.Assert.assertEquals;

public class TestParquetTimestampUtils
{
    @Test
    public void testGetTimestampMillis()
    {
        assertTimestampCorrect(LocalDateTime.parse("2011-01-01T00:00:00.000000000"));
        assertTimestampCorrect(LocalDateTime.parse("2001-01-01T01:01:01.000000001"));
        assertTimestampCorrect(LocalDateTime.parse("2015-12-31T23:59:59.999999999"));
    }

    @Test
    public void testInvalidBinaryLength()
    {
        try {
            byte[] invalidLengthBinaryTimestamp = new byte[8];
            decodeInt96Timestamp(Binary.fromConstantByteArray(invalidLengthBinaryTimestamp));
        }
        catch (TrinoException e) {
            assertEquals(e.getErrorCode(), NOT_SUPPORTED.toErrorCode());
            assertEquals(e.getMessage(), "Parquet timestamp must be 12 bytes, actual 8");
        }
    }

    private static void assertTimestampCorrect(LocalDateTime dateTime)
    {
        Timestamp timestamp = Timestamp.ofEpochSecond(dateTime.toEpochSecond(UTC), dateTime.getNano());
        Binary timestampBytes = getNanoTime(timestamp, false).toBinary();
        DecodedTimestamp decodedTimestamp = decodeInt96Timestamp(timestampBytes);
        assertEquals(decodedTimestamp.epochSeconds(), dateTime.toEpochSecond(UTC));
        assertEquals(decodedTimestamp.nanosOfSecond(), dateTime.getNano());
    }
}
