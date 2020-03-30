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
package com.starburstdata.presto.plugin.snowflake;

import com.google.common.collect.ImmutableMap;
import com.google.inject.ConfigurationException;
import com.starburstdata.presto.plugin.snowflake.distributed.SnowflakeDistributedConfig;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.units.DataSize;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSnowflakeDistributedConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(SnowflakeDistributedConfig.class)
                .setStageSchema(null)
                .setMaxInitialSplitSize(new DataSize(32, DataSize.Unit.MEGABYTE))
                .setMaxSplitSize(new DataSize(64, DataSize.Unit.MEGABYTE))
                .setParquetMaxReadBlockSize(new DataSize(16, DataSize.Unit.MEGABYTE))
                .setExportFileMaxSize(new DataSize(16, DataSize.Unit.MEGABYTE))
                .setMaxExportRetries(3));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("snowflake.stage-schema", "test_schema")
                .put("snowflake.max-initial-split-size", "16MB")
                .put("snowflake.max-split-size", "256MB")
                .put("snowflake.parquet.max-read-block-size", "66kB")
                .put("snowflake.export-file-max-size", "256MB")
                .put("snowflake.max-export-retries", "2")
                .build();

        SnowflakeDistributedConfig expected = new SnowflakeDistributedConfig()
                .setStageSchema("test_schema")
                .setMaxInitialSplitSize(new DataSize(16, DataSize.Unit.MEGABYTE))
                .setMaxSplitSize(new DataSize(256, DataSize.Unit.MEGABYTE))
                .setParquetMaxReadBlockSize(new DataSize(66, DataSize.Unit.KILOBYTE))
                .setExportFileMaxSize(new DataSize(256, DataSize.Unit.MEGABYTE))
                .setMaxExportRetries(2);
        assertFullMapping(properties, expected);
    }

    @DataProvider
    public Object[][] invalidSizes()
    {
        return new Object[][] {
                {new DataSize(16, DataSize.Unit.KILOBYTE)},
                {new DataSize(6, DataSize.Unit.GIGABYTE)}
        };
    }

    @Test(dataProvider = "invalidSizes")
    public void testInvalidExportFileSize(DataSize size)
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("snowflake.stage-schema", "test_schema")
                .put("snowflake.export-file-max-size", size.toString())
                .build();
        ConfigurationFactory configurationFactory = new ConfigurationFactory(properties);
        assertThatThrownBy(() -> configurationFactory.build(SnowflakeDistributedConfig.class))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("Invalid configuration property snowflake.export-file-max-size");
    }
}
