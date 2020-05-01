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
package io.prestosql.plugin.hive.parquet;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.LegacyConfig;
import io.airlift.units.DataSize;
import io.prestosql.parquet.ParquetReaderOptions;

import javax.validation.constraints.NotNull;

public class ParquetReaderConfig
{
    private ParquetReaderOptions options = new ParquetReaderOptions();

    @Deprecated
    public boolean isFailOnCorruptedStatistics()
    {
        return options.isFailOnCorruptedStatistics();
    }

    @Deprecated
    @Config("parquet.fail-on-corrupted-statistics")
    @LegacyConfig("hive.parquet.fail-on-corrupted-statistics")
    @ConfigDescription("Fail when scanning Parquet files with corrupted statistics")
    public ParquetReaderConfig setFailOnCorruptedStatistics(boolean failOnCorruptedStatistics)
    {
        options = options.withFailOnCorruptedStatistics(failOnCorruptedStatistics);
        return this;
    }

    @NotNull
    public DataSize getMaxReadBlockSize()
    {
        return options.getMaxReadBlockSize();
    }

    @Config("parquet.max-read-block-size")
    @LegacyConfig("hive.parquet.max-read-block-size")
    public ParquetReaderConfig setMaxReadBlockSize(DataSize maxReadBlockSize)
    {
        options = options.withMaxReadBlockSize(maxReadBlockSize);
        return this;
    }

    @NotNull
    public DataSize getMaxMergeDistance()
    {
        return options.getMaxMergeDistance();
    }

    @Config("parquet.max-merge-distance")
    public ParquetReaderConfig setMaxMergeDistance(DataSize distance)
    {
        options = options.withMaxMergeDistance(distance);
        return this;
    }

    @NotNull
    public DataSize getMaxBufferSize()
    {
        return options.getMaxBufferSize();
    }

    @Config("parquet.max-buffer-size")
    public ParquetReaderConfig setMaxBufferSize(DataSize size)
    {
        options = options.withMaxBufferSize(size);
        return this;
    }

    public ParquetReaderOptions toParquetReaderOptions()
    {
        return options;
    }
}
