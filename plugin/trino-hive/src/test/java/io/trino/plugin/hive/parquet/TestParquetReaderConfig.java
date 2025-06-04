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
package io.trino.plugin.hive.parquet;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class TestParquetReaderConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(ParquetReaderConfig.class)
                .setIgnoreStatistics(false)
                .setMaxReadBlockSize(DataSize.of(16, MEGABYTE))
                .setMaxReadBlockRowCount(8 * 1024)
                .setMaxMergeDistance(DataSize.of(1, MEGABYTE))
                .setMaxBufferSize(DataSize.of(8, MEGABYTE))
                .setUseColumnIndex(true)
                .setUseBloomFilter(true)
                .setSmallFileThreshold(DataSize.of(3, MEGABYTE))
                .setVectorizedDecodingEnabled(true));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("parquet.ignore-statistics", "true")
                .put("parquet.max-read-block-size", "66kB")
                .put("parquet.max-read-block-row-count", "500")
                .put("parquet.max-buffer-size", "1431kB")
                .put("parquet.max-merge-distance", "342kB")
                .put("parquet.use-column-index", "false")
                .put("parquet.use-bloom-filter", "false")
                .put("parquet.small-file-threshold", "1kB")
                .put("parquet.experimental.vectorized-decoding.enabled", "false")
                .buildOrThrow();

        ParquetReaderConfig expected = new ParquetReaderConfig()
                .setIgnoreStatistics(true)
                .setMaxReadBlockSize(DataSize.of(66, KILOBYTE))
                .setMaxReadBlockRowCount(500)
                .setMaxBufferSize(DataSize.of(1431, KILOBYTE))
                .setMaxMergeDistance(DataSize.of(342, KILOBYTE))
                .setUseColumnIndex(false)
                .setUseBloomFilter(false)
                .setSmallFileThreshold(DataSize.of(1, KILOBYTE))
                .setVectorizedDecodingEnabled(false);

        assertFullMapping(properties, expected);
    }
}
