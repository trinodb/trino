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
package io.trino.parquet;

import io.airlift.units.DataSize;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;

public class ParquetReaderOptions
{
    private static final DataSize DEFAULT_MAX_READ_BLOCK_SIZE = DataSize.of(16, MEGABYTE);
    private static final int DEFAULT_MAX_READ_BLOCK_ROW_COUNT = 8 * 1024;
    private static final DataSize DEFAULT_MAX_MERGE_DISTANCE = DataSize.of(1, MEGABYTE);
    private static final DataSize DEFAULT_MAX_BUFFER_SIZE = DataSize.of(8, MEGABYTE);
    private static final DataSize DEFAULT_SMALL_FILE_THRESHOLD = DataSize.of(3, MEGABYTE);
    private static final DataSize DEFAULT_MAX_FOOTER_READ_SIZE = DataSize.of(15, MEGABYTE);
    private static final DataSize DEFAULT_MAX_PAGE_READ_SIZE = DataSize.of(500, MEGABYTE);

    private final boolean ignoreStatistics;
    private final DataSize maxReadBlockSize;
    private final int maxReadBlockRowCount;
    private final DataSize maxMergeDistance;
    private final DataSize maxBufferSize;
    private final boolean useColumnIndex;
    private final boolean useBloomFilter;
    private final DataSize smallFileThreshold;
    private final boolean vectorizedDecodingEnabled;
    private final DataSize maxFooterReadSize;
    private final DataSize maxPageReadSize;

    private ParquetReaderOptions()
    {
        ignoreStatistics = false;
        maxReadBlockSize = DEFAULT_MAX_READ_BLOCK_SIZE;
        maxReadBlockRowCount = DEFAULT_MAX_READ_BLOCK_ROW_COUNT;
        maxMergeDistance = DEFAULT_MAX_MERGE_DISTANCE;
        maxBufferSize = DEFAULT_MAX_BUFFER_SIZE;
        useColumnIndex = true;
        useBloomFilter = true;
        smallFileThreshold = DEFAULT_SMALL_FILE_THRESHOLD;
        vectorizedDecodingEnabled = true;
        maxFooterReadSize = DEFAULT_MAX_FOOTER_READ_SIZE;
        maxPageReadSize = DEFAULT_MAX_PAGE_READ_SIZE;
    }

    private ParquetReaderOptions(
            boolean ignoreStatistics,
            DataSize maxReadBlockSize,
            int maxReadBlockRowCount,
            DataSize maxMergeDistance,
            DataSize maxBufferSize,
            boolean useColumnIndex,
            boolean useBloomFilter,
            DataSize smallFileThreshold,
            boolean vectorizedDecodingEnabled,
            DataSize maxFooterReadSize,
            DataSize maxPageReadSize)
    {
        this.ignoreStatistics = ignoreStatistics;
        this.maxReadBlockSize = requireNonNull(maxReadBlockSize, "maxReadBlockSize is null");
        checkArgument(maxReadBlockRowCount > 0, "maxReadBlockRowCount must be greater than 0");
        this.maxReadBlockRowCount = maxReadBlockRowCount;
        this.maxMergeDistance = requireNonNull(maxMergeDistance, "maxMergeDistance is null");
        this.maxBufferSize = requireNonNull(maxBufferSize, "maxBufferSize is null");
        this.useColumnIndex = useColumnIndex;
        this.useBloomFilter = useBloomFilter;
        this.smallFileThreshold = requireNonNull(smallFileThreshold, "smallFileThreshold is null");
        this.vectorizedDecodingEnabled = vectorizedDecodingEnabled;
        this.maxFooterReadSize = requireNonNull(maxFooterReadSize, "maxFooterReadSize is null");
        this.maxPageReadSize = requireNonNull(maxPageReadSize, "maxPageReadSize is null");
    }

    public static Builder builder()
    {
        return new Builder(new ParquetReaderOptions());
    }

    public static Builder builder(ParquetReaderOptions parquetReaderOptions)
    {
        return new Builder(parquetReaderOptions);
    }

    public static ParquetReaderOptions defaultOptions()
    {
        return new ParquetReaderOptions();
    }

    public boolean isIgnoreStatistics()
    {
        return ignoreStatistics;
    }

    public DataSize getMaxReadBlockSize()
    {
        return maxReadBlockSize;
    }

    public DataSize getMaxMergeDistance()
    {
        return maxMergeDistance;
    }

    public boolean isUseColumnIndex()
    {
        return useColumnIndex;
    }

    public boolean useBloomFilter()
    {
        return useBloomFilter;
    }

    public boolean isVectorizedDecodingEnabled()
    {
        return vectorizedDecodingEnabled;
    }

    public DataSize getMaxBufferSize()
    {
        return maxBufferSize;
    }

    public int getMaxReadBlockRowCount()
    {
        return maxReadBlockRowCount;
    }

    public DataSize getSmallFileThreshold()
    {
        return smallFileThreshold;
    }

    public DataSize getMaxFooterReadSize()
    {
        return maxFooterReadSize;
    }

    public DataSize getMaxPageReadSize()
    {
        return maxPageReadSize;
    }

    public static class Builder
    {
        private boolean ignoreStatistics;
        private DataSize maxReadBlockSize;
        private int maxReadBlockRowCount;
        private DataSize maxMergeDistance;
        private DataSize maxBufferSize;
        private boolean useColumnIndex;
        private boolean useBloomFilter;
        private DataSize smallFileThreshold;
        private boolean vectorizedDecodingEnabled;
        private DataSize maxFooterReadSize;
        private DataSize maxPageReadSize;

        private Builder(ParquetReaderOptions parquetReaderOptions)
        {
            requireNonNull(parquetReaderOptions, "parquetReaderOptions is null");
            this.ignoreStatistics = parquetReaderOptions.ignoreStatistics;
            this.maxReadBlockSize = parquetReaderOptions.maxReadBlockSize;
            this.maxReadBlockRowCount = parquetReaderOptions.maxReadBlockRowCount;
            this.maxMergeDistance = parquetReaderOptions.maxMergeDistance;
            this.maxBufferSize = parquetReaderOptions.maxBufferSize;
            this.useColumnIndex = parquetReaderOptions.useColumnIndex;
            this.useBloomFilter = parquetReaderOptions.useBloomFilter;
            this.smallFileThreshold = parquetReaderOptions.smallFileThreshold;
            this.vectorizedDecodingEnabled = parquetReaderOptions.vectorizedDecodingEnabled;
            this.maxFooterReadSize = parquetReaderOptions.maxFooterReadSize;
            this.maxPageReadSize = parquetReaderOptions.maxPageReadSize;
        }

        public Builder withIgnoreStatistics(boolean ignoreStatistics)
        {
            this.ignoreStatistics = ignoreStatistics;
            return this;
        }

        public Builder withMaxReadBlockSize(DataSize maxReadBlockSize)
        {
            this.maxReadBlockSize = requireNonNull(maxReadBlockSize, "maxReadBlockSize is null");
            return this;
        }

        public Builder withMaxReadBlockRowCount(int maxReadBlockRowCount)
        {
            this.maxReadBlockRowCount = maxReadBlockRowCount;
            return this;
        }

        public Builder withMaxMergeDistance(DataSize maxMergeDistance)
        {
            this.maxMergeDistance = requireNonNull(maxMergeDistance, "maxMergeDistance is null");
            return this;
        }

        public Builder withMaxBufferSize(DataSize maxBufferSize)
        {
            this.maxBufferSize = requireNonNull(maxBufferSize, "maxBufferSize is null");
            return this;
        }

        public Builder withUseColumnIndex(boolean useColumnIndex)
        {
            this.useColumnIndex = useColumnIndex;
            return this;
        }

        public Builder withBloomFilter(boolean useBloomFilter)
        {
            this.useBloomFilter = useBloomFilter;
            return this;
        }

        public Builder withSmallFileThreshold(DataSize smallFileThreshold)
        {
            this.smallFileThreshold = requireNonNull(smallFileThreshold, "smallFileThreshold is null");
            return this;
        }

        public Builder withVectorizedDecodingEnabled(boolean vectorizedDecodingEnabled)
        {
            this.vectorizedDecodingEnabled = vectorizedDecodingEnabled;
            return this;
        }

        public Builder withMaxFooterReadSize(DataSize maxFooterReadSize)
        {
            this.maxFooterReadSize = requireNonNull(maxFooterReadSize, "maxFooterReadSize is null");
            return this;
        }

        public Builder withMaxPageReadSize(DataSize maxPageReadSize)
        {
            this.maxPageReadSize = requireNonNull(maxPageReadSize, "maxPageSize is null");
            return this;
        }

        public ParquetReaderOptions build()
        {
            return new ParquetReaderOptions(
                    ignoreStatistics,
                    maxReadBlockSize,
                    maxReadBlockRowCount,
                    maxMergeDistance,
                    maxBufferSize,
                    useColumnIndex,
                    useBloomFilter,
                    smallFileThreshold,
                    vectorizedDecodingEnabled,
                    maxFooterReadSize,
                    maxPageReadSize);
        }
    }
}
