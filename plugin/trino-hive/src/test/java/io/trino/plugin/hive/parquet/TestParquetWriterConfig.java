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
import org.apache.parquet.column.ParquetProperties;
import org.junit.jupiter.api.Test;

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
                .setRowGroupSize(DataSize.of(128, MEGABYTE))
                .setRowGroupMaxRowCount(ParquetWriterOptions.DEFAULT_MAX_ROW_GROUP_ROW_COUNT)
                .setPageSize(DataSize.ofBytes(ParquetProperties.DEFAULT_PAGE_SIZE))
                .setPageValueCount(ParquetWriterOptions.DEFAULT_MAX_PAGE_VALUE_COUNT)
                .setBatchSize(ParquetWriterOptions.DEFAULT_BATCH_SIZE)
                .setValidationPercentage(5)
                .setDeltaLengthByteArrayEncodingEnabled(true));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = Map.of(
                "parquet.writer.row-group-size", "234MB",
                "parquet.writer.row-group-max-row-count", "50000",
                "parquet.writer.page-size", "6MB",
                "parquet.writer.page-value-count", "10000",
                "parquet.writer.batch-size", "100",
                "parquet.writer.validation-percentage", "10",
                "parquet.writer.delta-length-byte-array-encoding-enabled", "false");

        ParquetWriterConfig expected = new ParquetWriterConfig()
                .setRowGroupSize(DataSize.of(234, MEGABYTE))
                .setRowGroupMaxRowCount(50_000)
                .setPageSize(DataSize.of(6, MEGABYTE))
                .setPageValueCount(10_000)
                .setBatchSize(100)
                .setValidationPercentage(10)
                .setDeltaLengthByteArrayEncodingEnabled(false);

        assertFullMapping(properties, expected);

        Map<String, String> oldProperties = Map.of(
                "parquet.writer.block-size", "234MB",
                "parquet.writer.row-group-max-row-count", "50000",
                "parquet.writer.page-size", "6MB",
                "parquet.writer.page-value-count", "10000",
                "parquet.writer.batch-size", "100",
                "parquet.writer.validation-percentage", "10",
                "parquet.writer.delta-length-byte-array-encoding-enabled", "false");
        assertDeprecatedEquivalence(ParquetWriterConfig.class, properties, oldProperties);
    }
}
