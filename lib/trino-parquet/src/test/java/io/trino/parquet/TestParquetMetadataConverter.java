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

import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.format.Statistics;
import org.apache.parquet.io.api.Binary;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static io.trino.parquet.ParquetMetadataConverter.toParquetStatistics;
import static org.assertj.core.api.Assertions.assertThat;

class TestParquetMetadataConverter
{
    private static final int TRUNCATE_LENGTH = 64;

    @Test
    void testBinaryMaxStatisticsOmittedForAllFFBytes()
    {
        // 128 bytes of 0xFF cannot be truncated for max (incrementing would overflow)
        byte[] allFF = new byte[128];
        Arrays.fill(allFF, (byte) 0xFF);

        // min is a normal value
        byte[] normalMin = new byte[] {0x00, 0x01, 0x02};

        BinaryStatistics binaryStats = new BinaryStatistics();
        binaryStats.setMinMax(Binary.fromConstantByteArray(normalMin), Binary.fromConstantByteArray(allFF));
        binaryStats.setNumNulls(5);

        Statistics formatStats = toParquetStatistics(binaryStats, TRUNCATE_LENGTH);

        // Null count should be preserved
        assertThat(formatStats.isSetNull_count()).isTrue();
        assertThat(formatStats.getNull_count()).isEqualTo(5);

        // Max should be omitted because all-0xFF cannot be truncated and exceeds limit
        assertThat(formatStats.isSetMax()).isFalse();
        assertThat(formatStats.isSetMax_value()).isFalse();

        // Min should still be present (it's small enough)
        assertThat(formatStats.isSetMin_value()).isTrue();
        assertThat(formatStats.getMin_value()).isEqualTo(normalMin);
    }

    @Test
    void testBinaryMinStatisticsPreservedForAllZerosBytes()
    {
        // 128 bytes of 0x00 CAN be truncated for min (first 64 bytes of 0x00 <= 128 bytes of 0x00)
        byte[] allZeros = new byte[128];
        Arrays.fill(allZeros, (byte) 0x00);

        // max is a normal value
        byte[] normalMax = new byte[] {0x01, 0x02, 0x03};

        BinaryStatistics binaryStats = new BinaryStatistics();
        binaryStats.setMinMax(Binary.fromConstantByteArray(allZeros), Binary.fromConstantByteArray(normalMax));
        binaryStats.setNumNulls(3);

        Statistics formatStats = toParquetStatistics(binaryStats, TRUNCATE_LENGTH);

        // Null count should be preserved
        assertThat(formatStats.isSetNull_count()).isTrue();
        assertThat(formatStats.getNull_count()).isEqualTo(3);

        // Min should be present (all-0x00 can be truncated to TRUNCATE_LENGTH bytes)
        assertThat(formatStats.isSetMin_value()).isTrue();
        assertThat(formatStats.getMin_value().length).isEqualTo(TRUNCATE_LENGTH);

        // Max should still be present (it's small enough)
        assertThat(formatStats.isSetMax_value()).isTrue();
        assertThat(formatStats.getMax_value()).isEqualTo(normalMax);
    }

    @Test
    void testNormalBinaryStatisticsPreserved()
    {
        // Normal values that fit within the limit
        byte[] minValue = new byte[32];
        byte[] maxValue = new byte[32];
        Arrays.fill(minValue, (byte) 0x10);
        Arrays.fill(maxValue, (byte) 0x20);

        BinaryStatistics binaryStats = new BinaryStatistics();
        binaryStats.setMinMax(Binary.fromConstantByteArray(minValue), Binary.fromConstantByteArray(maxValue));
        binaryStats.setNumNulls(7);

        Statistics formatStats = toParquetStatistics(binaryStats, TRUNCATE_LENGTH);

        // Everything should be preserved
        assertThat(formatStats.isSetNull_count()).isTrue();
        assertThat(formatStats.getNull_count()).isEqualTo(7);
        assertThat(formatStats.isSetMin_value()).isTrue();
        assertThat(formatStats.isSetMax_value()).isTrue();
        assertThat(formatStats.getMin_value()).isEqualTo(minValue);
        assertThat(formatStats.getMax_value()).isEqualTo(maxValue);
    }

    @Test
    void testBinaryStatisticsTruncationWorks()
    {
        // Values longer than truncation length that can be truncated
        byte[] minValue = new byte[100];
        byte[] maxValue = new byte[100];
        Arrays.fill(minValue, (byte) 0x10);
        Arrays.fill(maxValue, (byte) 0x20);

        BinaryStatistics binaryStats = new BinaryStatistics();
        binaryStats.setMinMax(Binary.fromConstantByteArray(minValue), Binary.fromConstantByteArray(maxValue));
        binaryStats.setNumNulls(2);

        Statistics formatStats = toParquetStatistics(binaryStats, TRUNCATE_LENGTH);

        // Statistics should be present and truncated
        assertThat(formatStats.isSetNull_count()).isTrue();
        assertThat(formatStats.getNull_count()).isEqualTo(2);
        assertThat(formatStats.isSetMin_value()).isTrue();
        assertThat(formatStats.isSetMax_value()).isTrue();

        // Truncated values should be at most TRUNCATE_LENGTH bytes
        assertThat(formatStats.getMin_value().length).isLessThanOrEqualTo(TRUNCATE_LENGTH);
        assertThat(formatStats.getMax_value().length).isLessThanOrEqualTo(TRUNCATE_LENGTH);
    }

    @Test
    void testOnlyMaxOmittedWhenOnlyMaxExceedsLimit()
    {
        // Min can be truncated (all-0x00), but max cannot (all-0xFF)
        byte[] allZeros = new byte[128];
        byte[] allOnes = new byte[128];
        Arrays.fill(allZeros, (byte) 0x00);
        Arrays.fill(allOnes, (byte) 0xFF);

        BinaryStatistics binaryStats = new BinaryStatistics();
        binaryStats.setMinMax(Binary.fromConstantByteArray(allZeros), Binary.fromConstantByteArray(allOnes));
        binaryStats.setNumNulls(10);

        Statistics formatStats = toParquetStatistics(binaryStats, TRUNCATE_LENGTH);

        // Null count should be preserved
        assertThat(formatStats.isSetNull_count()).isTrue();
        assertThat(formatStats.getNull_count()).isEqualTo(10);

        // Min should be preserved (can be truncated to TRUNCATE_LENGTH bytes of 0x00)
        assertThat(formatStats.isSetMin_value()).isTrue();
        assertThat(formatStats.getMin_value().length).isEqualTo(TRUNCATE_LENGTH);

        // Max should be omitted (all-0xFF cannot be truncated)
        assertThat(formatStats.isSetMax()).isFalse();
        assertThat(formatStats.isSetMax_value()).isFalse();
    }
}
