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
import io.airlift.units.DataSize;
import io.airlift.units.MaxDataSize;
import io.airlift.units.MinDataSize;
import io.trino.parquet.ParquetReaderOptions;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

@DefunctConfig({
        "hive.parquet.fail-on-corrupted-statistics",
        "parquet.fail-on-corrupted-statistics",
        "parquet.optimized-reader.enabled",
        "parquet.optimized-nested-reader.enabled",
        "hive.parquet.max-read-block-size",
})
public class ParquetReaderConfig
{
    public static final String PARQUET_READER_MAX_SMALL_FILE_THRESHOLD = "15MB";

    private ParquetReaderOptions options = ParquetReaderOptions.defaultOptions();

    public boolean isIgnoreStatistics()
    {
        return options.isIgnoreStatistics();
    }

    @Config("parquet.ignore-statistics")
    @ConfigDescription("Ignore statistics from Parquet to allow querying files with corrupted or incorrect statistics")
    public ParquetReaderConfig setIgnoreStatistics(boolean ignoreStatistics)
    {
        options = ParquetReaderOptions.builder(options)
                .withIgnoreStatistics(ignoreStatistics)
                .build();
        return this;
    }

    @NotNull
    public DataSize getMaxReadBlockSize()
    {
        return options.getMaxReadBlockSize();
    }

    @Config("parquet.max-read-block-size")
    public ParquetReaderConfig setMaxReadBlockSize(DataSize maxReadBlockSize)
    {
        options = ParquetReaderOptions.builder(options)
                .withMaxReadBlockSize(maxReadBlockSize)
                .build();
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
        options = ParquetReaderOptions.builder(options).withMaxReadBlockRowCount(length).build();
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
        options = ParquetReaderOptions.builder(options)
                .withMaxMergeDistance(distance)
                .build();
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
        options = ParquetReaderOptions.builder(options)
                .withMaxBufferSize(size)
                .build();
        return this;
    }

    @Config("parquet.use-column-index")
    @ConfigDescription("Enable using Parquet column indexes")
    public ParquetReaderConfig setUseColumnIndex(boolean useColumnIndex)
    {
        options = ParquetReaderOptions.builder(options)
                .withUseColumnIndex(useColumnIndex)
                .build();
        return this;
    }

    public boolean isUseColumnIndex()
    {
        return options.isUseColumnIndex();
    }

    @Config("parquet.use-bloom-filter")
    @ConfigDescription("Use Parquet Bloom filters")
    public ParquetReaderConfig setUseBloomFilter(boolean useBloomFilter)
    {
        options = ParquetReaderOptions.builder(options)
                .withBloomFilter(useBloomFilter)
                .build();
        return this;
    }

    public boolean isUseBloomFilter()
    {
        return options.useBloomFilter();
    }

    @Config("parquet.small-file-threshold")
    @ConfigDescription("Size below which a parquet file will be read entirely")
    public ParquetReaderConfig setSmallFileThreshold(DataSize smallFileThreshold)
    {
        options = ParquetReaderOptions.builder(options)
                .withSmallFileThreshold(smallFileThreshold)
                .build();
        return this;
    }

    @NotNull
    @MaxDataSize(PARQUET_READER_MAX_SMALL_FILE_THRESHOLD)
    public DataSize getSmallFileThreshold()
    {
        return options.getSmallFileThreshold();
    }

    @Config("parquet.experimental.vectorized-decoding.enabled")
    @ConfigDescription("Enable using Java Vector API for faster decoding of parquet files")
    public ParquetReaderConfig setVectorizedDecodingEnabled(boolean vectorizedDecodingEnabled)
    {
        options = ParquetReaderOptions.builder(options)
                .withVectorizedDecodingEnabled(vectorizedDecodingEnabled)
                .build();
        return this;
    }

    public boolean isVectorizedDecodingEnabled()
    {
        return options.isVectorizedDecodingEnabled();
    }

    @Config("parquet.max-footer-read-size")
    @ConfigDescription("Maximum allowed size of the parquet footer. Files with footers larger than this will generate an exception on read")
    public ParquetReaderConfig setMaxFooterReadSize(DataSize maxFooterReadSize)
    {
        options = ParquetReaderOptions.builder(options)
                .withMaxFooterReadSize(maxFooterReadSize)
                .build();
        return this;
    }

    @NotNull
    public DataSize getMaxFooterReadSize()
    {
        return options.getMaxFooterReadSize();
    }

    public ParquetReaderOptions toParquetReaderOptions()
    {
        return options;
    }
}
