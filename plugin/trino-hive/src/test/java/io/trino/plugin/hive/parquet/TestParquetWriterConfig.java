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

import io.airlift.units.DataSize;
import io.trino.parquet.writer.ParquetWriterOptions;
import org.apache.parquet.hadoop.ParquetWriter;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertDeprecatedEquivalence;
import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class TestParquetWriterConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(ParquetWriterConfig.class)
                .setParquetOptimizedWriterEnabled(true)
                .setBlockSize(DataSize.ofBytes(ParquetWriter.DEFAULT_BLOCK_SIZE))
                .setPageSize(DataSize.ofBytes(ParquetWriter.DEFAULT_PAGE_SIZE))
                .setBatchSize(ParquetWriterOptions.DEFAULT_BATCH_SIZE)
                .setValidationPercentage(5));
    }

    @Test
    public void testLegacyProperties()
    {
        assertDeprecatedEquivalence(
                ParquetWriterConfig.class,
                Map.of(
                        "parquet.optimized-writer.enabled", "true",
                        "parquet.writer.block-size", "33MB",
                        "parquet.writer.page-size", "7MB"),
                Map.of(
                        "parquet.experimental-optimized-writer.enabled", "true",
                        "hive.parquet.writer.block-size", "33MB",
                        "hive.parquet.writer.page-size", "7MB"),
                Map.of(
                        "hive.parquet.optimized-writer.enabled", "true",
                        "hive.parquet.writer.block-size", "33MB",
                        "hive.parquet.writer.page-size", "7MB"));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = Map.of(
                "parquet.optimized-writer.enabled", "false",
                "parquet.writer.block-size", "234MB",
                "parquet.writer.page-size", "6MB",
                "parquet.writer.batch-size", "100",
                "parquet.optimized-writer.validation-percentage", "10");

        ParquetWriterConfig expected = new ParquetWriterConfig()
                .setParquetOptimizedWriterEnabled(false)
                .setBlockSize(DataSize.of(234, MEGABYTE))
                .setPageSize(DataSize.of(6, MEGABYTE))
                .setBatchSize(100)
                .setValidationPercentage(10);

        assertFullMapping(properties, expected);
    }
}
