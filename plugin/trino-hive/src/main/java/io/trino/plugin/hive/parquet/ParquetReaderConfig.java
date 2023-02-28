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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;
import io.airlift.configuration.LegacyConfig;
import io.airlift.units.DataSize;
import io.airlift.units.MinDataSize;
import io.trino.parquet.ParquetReaderOptions;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

@DefunctConfig({
        "hive.parquet.fail-on-corrupted-statistics",
        "parquet.fail-on-corrupted-statistics",
})
public class ParquetReaderConfig
{
    private ParquetReaderOptions options = new ParquetReaderOptions();

    @Deprecated
    public boolean isIgnoreStatistics()
    {
        return options.isIgnoreStatistics();
    }

    @Deprecated
    @Config("parquet.ignore-statistics")
    @ConfigDescription("Ignore statistics from Parquet to allow querying files with corrupted or incorrect statistics")
    public ParquetReaderConfig setIgnoreStatistics(boolean ignoreStatistics)
    {
        options = options.withIgnoreStatistics(ignoreStatistics);
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

    @Min(128)
    @Max(65536)
    public int getMaxReadBlockRowCount()
    {
        return options.getMaxReadBlockRowCount();
    }

    @Config("parquet.max-read-block-row-count")
    @ConfigDescription("Maximum number of rows read in a batch")
    public ParquetReaderConfig setMaxReadBlockRowCount(int length)
    {
        options = options.withMaxReadBlockRowCount(length);
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
    @MinDataSize("1MB")
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

    @Config("parquet.use-column-index")
    @ConfigDescription("Enable using Parquet column indexes")
    public ParquetReaderConfig setUseColumnIndex(boolean useColumnIndex)
    {
        options = options.withUseColumnIndex(useColumnIndex);
        return this;
    }

    public boolean isUseColumnIndex()
    {
        return options.isUseColumnIndex();
    }

    @Config("parquet.optimized-reader.enabled")
    @ConfigDescription("Use optimized Parquet reader")
    public ParquetReaderConfig setOptimizedReaderEnabled(boolean optimizedReaderEnabled)
    {
        options = options.withBatchColumnReaders(optimizedReaderEnabled);
        return this;
    }

    public boolean isOptimizedReaderEnabled()
    {
        return options.useBatchColumnReaders();
    }

    @Config("parquet.optimized-nested-reader.enabled")
    @ConfigDescription("Use optimized Parquet reader for nested columns")
    public ParquetReaderConfig setOptimizedNestedReaderEnabled(boolean optimizedNestedReaderEnabled)
    {
        options = options.withBatchNestedColumnReaders(optimizedNestedReaderEnabled);
        return this;
    }

    public boolean isOptimizedNestedReaderEnabled()
    {
        return options.useBatchNestedColumnReaders();
    }

    @Config("parquet.use-bloom-filter")
    @ConfigDescription("Enable using Parquet bloom filter")
    public ParquetReaderConfig setUseBloomFilter(boolean useBloomFilter)
    {
        options = options.withBloomFilter(useBloomFilter);
        return this;
    }

    public boolean isUseBloomFilter()
    {
        return options.useBloomFilter();
    }

    public ParquetReaderOptions toParquetReaderOptions()
    {
        return options;
    }
}
