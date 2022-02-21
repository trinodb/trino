/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
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
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSnowflakeDistributedConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(SnowflakeDistributedConfig.class)
                .setStageSchema(null)
                .setMaxInitialSplitSize(DataSize.of(32, MEGABYTE))
                .setMaxSplitSize(DataSize.of(64, MEGABYTE))
                .setParquetMaxReadBlockSize(DataSize.of(16, MEGABYTE))
                .setUseColumnIndex(true)
                .setExportFileMaxSize(DataSize.of(5, GIGABYTE))
                .setMaxExportRetries(3)
                .setRetryCanceledQueries(false));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("snowflake.stage-schema", "test_schema_2")
                .put("snowflake.max-initial-split-size", "31MB")
                .put("snowflake.max-split-size", "222MB")
                .put("snowflake.parquet.max-read-block-size", "66kB")
                .put("snowflake.parquet.use-column-index", "false")
                .put("snowflake.export-file-max-size", "333MB")
                .put("snowflake.max-export-retries", "42")
                .put("snowflake.retry-canceled-queries", "true")
                .buildOrThrow();

        SnowflakeDistributedConfig expected = new SnowflakeDistributedConfig()
                .setStageSchema("test_schema_2")
                .setMaxInitialSplitSize(DataSize.of(31, MEGABYTE))
                .setMaxSplitSize(DataSize.of(222, MEGABYTE))
                .setParquetMaxReadBlockSize(DataSize.of(66, KILOBYTE))
                .setUseColumnIndex(false)
                .setExportFileMaxSize(DataSize.of(333, MEGABYTE))
                .setMaxExportRetries(42)
                .setRetryCanceledQueries(true);
        assertFullMapping(properties, expected);
    }

    @DataProvider
    public Object[][] invalidSizes()
    {
        return new Object[][] {
                {DataSize.of(16, KILOBYTE)},
                {DataSize.of(6, GIGABYTE)}
        };
    }

    @Test(dataProvider = "invalidSizes")
    public void testInvalidExportFileSize(DataSize size)
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("snowflake.stage-schema", "test_schema_2")
                .put("snowflake.export-file-max-size", size.toString())
                .buildOrThrow();
        ConfigurationFactory configurationFactory = new ConfigurationFactory(properties);
        assertThatThrownBy(() -> configurationFactory.build(SnowflakeDistributedConfig.class))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("Invalid configuration property snowflake.export-file-max-size");
    }
}
