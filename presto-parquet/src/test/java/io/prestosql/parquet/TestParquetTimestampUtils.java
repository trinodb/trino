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
package io.prestosql.parquet;

import io.prestosql.spi.PrestoException;
import io.prestosql.spi.type.TimestampType;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.parquet.io.api.Binary;
import org.testng.annotations.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static io.prestosql.parquet.ParquetTimestampUtils.getTimestampMillis;
import static io.prestosql.parquet.ParquetTimestampUtils.scaleParquetTimestamp;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static org.apache.hadoop.hive.ql.io.parquet.timestamp.NanoTimeUtils.getNanoTime;
import static org.apache.parquet.schema.OriginalType.TIMESTAMP_MICROS;
import static org.apache.parquet.schema.OriginalType.TIMESTAMP_MILLIS;
import static org.testng.Assert.assertEquals;

public class TestParquetTimestampUtils
{
    @Test
    public void testGetTimestampMillis()
    {
        assertTimestampCorrect("2011-01-01 00:00:00.000000000");
        assertTimestampCorrect("2001-01-01 01:01:01.000000001");
        assertTimestampCorrect("2015-12-31 23:59:59.999999999");
    }

    @Test
    public void testInvalidBinaryLength()
    {
        try {
            byte[] invalidLengthBinaryTimestamp = new byte[8];
            getTimestampMillis(Binary.fromByteArray(invalidLengthBinaryTimestamp));
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), NOT_SUPPORTED.toErrorCode());
            assertEquals(e.getMessage(), "Parquet timestamp must be 12 bytes, actual 8");
        }
    }

    private static void assertTimestampCorrect(String timestampString)
    {
        Timestamp timestamp = Timestamp.valueOf(timestampString);
        Binary timestampBytes = getNanoTime(timestamp, false).toBinary();
        long decodedTimestampMillis = getTimestampMillis(timestampBytes);
        assertEquals(decodedTimestampMillis, timestamp.toEpochMilli());
    }

    @Test
    public void testScaleParquetTimestamps()
    {
        long epochMillis = LocalDateTime.of(2012, 11, 10, 9, 8, 7, 6).toInstant(ZoneOffset.UTC).toEpochMilli();
        long epochMicros1 = (epochMillis * 1000) + 345;
        long epochMicros2 = (epochMillis * 1000) + 543;
        TimestampType prestoMilliType = TimestampType.createTimestampType(3);
        assertEquals(scaleParquetTimestamp(TIMESTAMP_MILLIS, prestoMilliType, epochMillis), epochMillis);
        assertEquals(scaleParquetTimestamp(TIMESTAMP_MICROS, prestoMilliType, epochMicros1), epochMillis); // Round down
        assertEquals(scaleParquetTimestamp(TIMESTAMP_MICROS, prestoMilliType, epochMicros2), epochMillis + 1); // Round up

        TimestampType prestoMicroType = TimestampType.createTimestampType(6);
        assertEquals(scaleParquetTimestamp(TIMESTAMP_MILLIS, prestoMicroType, epochMillis), epochMillis * 1000);
        assertEquals(scaleParquetTimestamp(TIMESTAMP_MICROS, prestoMicroType, epochMicros1), epochMicros1);
        assertEquals(scaleParquetTimestamp(TIMESTAMP_MICROS, prestoMicroType, epochMicros2), epochMicros2);
    }
}
