/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.snowflake.distributed;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;
import io.airlift.units.MaxDataSize;
import io.airlift.units.MinDataSize;
import io.trino.plugin.hive.HiveConfig;

import javax.validation.constraints.NotNull;

import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class SnowflakeDistributedConfig
{
    private String stageSchema;

    // defaults copied from io.trino.plugin.hive.HiveConfig
    private DataSize maxInitialSplitSize;
    private DataSize maxSplitSize = DataSize.of(64, MEGABYTE);
    private DataSize parquetMaxReadBlockSize = DataSize.of(16, MEGABYTE);
    private boolean useColumnIndex = true;
    private DataSize exportFileMaxSize = DataSize.of(5, GIGABYTE);
    private int maxExportRetries = 3;
    private boolean retryCanceledQueries;

    @Config("snowflake.stage-schema")
    @ConfigDescription("Schema to use for data staging")
    public SnowflakeDistributedConfig setStageSchema(String stageSchema)
    {
        this.stageSchema = stageSchema;
        return this;
    }

    @NotNull
    public String getStageSchema()
    {
        return stageSchema;
    }

    public DataSize getMaxInitialSplitSize()
    {
        if (maxInitialSplitSize == null) {
            return DataSize.ofBytes(maxSplitSize.toBytes() / 2).to(maxSplitSize.getUnit());
        }
        return maxInitialSplitSize;
    }

    @Config("snowflake.max-initial-split-size")
    public SnowflakeDistributedConfig setMaxInitialSplitSize(DataSize maxInitialSplitSize)
    {
        this.maxInitialSplitSize = maxInitialSplitSize;
        return this;
    }

    @NotNull
    public DataSize getMaxSplitSize()
    {
        return maxSplitSize;
    }

    @Config("snowflake.max-split-size")
    public SnowflakeDistributedConfig setMaxSplitSize(DataSize maxSplitSize)
    {
        this.maxSplitSize = maxSplitSize;
        return this;
    }

    @NotNull
    public DataSize getParquetMaxReadBlockSize()
    {
        return parquetMaxReadBlockSize;
    }

    @Config("snowflake.parquet.max-read-block-size")
    public SnowflakeDistributedConfig setParquetMaxReadBlockSize(DataSize parquetMaxReadBlockSize)
    {
        this.parquetMaxReadBlockSize = parquetMaxReadBlockSize;
        return this;
    }

    @Config("snowflake.parquet.use-column-index")
    @ConfigDescription("Enable using Parquet column indexes")
    public SnowflakeDistributedConfig setUseColumnIndex(boolean useColumnIndex)
    {
        this.useColumnIndex = useColumnIndex;
        return this;
    }

    public boolean isUseColumnIndex()
    {
        return useColumnIndex;
    }

    @MinDataSize(value = "1MB", message = "The export file max size must at least 1MB")
    @MaxDataSize(value = "5GB", message = "The export file max size can be at most 5GB")
    @NotNull
    public DataSize getExportFileMaxSize()
    {
        return exportFileMaxSize;
    }

    @Config("snowflake.export-file-max-size")
    @ConfigDescription("Maximum size of files to create when exporting data")
    public SnowflakeDistributedConfig setExportFileMaxSize(DataSize exportFileMaxSize)
    {
        this.exportFileMaxSize = exportFileMaxSize;
        return this;
    }

    public int getMaxExportRetries()
    {
        return maxExportRetries;
    }

    @Config("snowflake.max-export-retries")
    @ConfigDescription("Number of export retries")
    public SnowflakeDistributedConfig setMaxExportRetries(int maxExportRetries)
    {
        this.maxExportRetries = maxExportRetries;
        return this;
    }

    public boolean isRetryCanceledQueries()
    {
        return retryCanceledQueries;
    }

    @Config("snowflake.retry-canceled-queries")
    @ConfigDescription("Should Presto retry queries that failed due to being canceled")
    public SnowflakeDistributedConfig setRetryCanceledQueries(boolean retryCanceledQueries)
    {
        this.retryCanceledQueries = retryCanceledQueries;
        return this;
    }

    HiveConfig getHiveConfig()
    {
        return new HiveConfig()
                .setMaxInitialSplitSize(maxInitialSplitSize)
                .setMaxSplitSize(maxSplitSize);
    }
}
