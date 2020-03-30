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
package com.starburstdata.presto.plugin.snowflake.distributed;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;
import io.airlift.units.MaxDataSize;
import io.airlift.units.MinDataSize;
import io.prestosql.plugin.hive.HiveConfig;

import javax.validation.constraints.NotNull;

import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class SnowflakeDistributedConfig
{
    private String stageSchema;

    // defaults copied from io.prestosql.plugin.hive.HiveConfig
    private DataSize maxInitialSplitSize;
    private DataSize maxSplitSize = new DataSize(64, MEGABYTE);
    private DataSize parquetMaxReadBlockSize = new DataSize(16, MEGABYTE);
    private DataSize exportFileMaxSize = new DataSize(5, GIGABYTE);
    private int maxExportRetries = 3;

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
            return new DataSize(maxSplitSize.getValue() / 2, maxSplitSize.getUnit());
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

    HiveConfig getHiveConfig()
    {
        return new HiveConfig()
                .setMaxInitialSplitSize(maxInitialSplitSize)
                .setMaxSplitSize(maxSplitSize);
    }
}
