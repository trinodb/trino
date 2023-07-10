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

    private final boolean ignoreStatistics;
    private final DataSize maxReadBlockSize;
    private final int maxReadBlockRowCount;
    private final DataSize maxMergeDistance;
    private final DataSize maxBufferSize;
    private final boolean useColumnIndex;
    private final boolean useBatchColumnReaders;
    private final boolean useBatchNestedColumnReaders;
    private final boolean useBloomFilter;

    public ParquetReaderOptions()
    {
        ignoreStatistics = false;
        maxReadBlockSize = DEFAULT_MAX_READ_BLOCK_SIZE;
        maxReadBlockRowCount = DEFAULT_MAX_READ_BLOCK_ROW_COUNT;
        maxMergeDistance = DEFAULT_MAX_MERGE_DISTANCE;
        maxBufferSize = DEFAULT_MAX_BUFFER_SIZE;
        useColumnIndex = true;
        useBatchColumnReaders = true;
        useBatchNestedColumnReaders = true;
        useBloomFilter = true;
    }

    private ParquetReaderOptions(
            boolean ignoreStatistics,
            DataSize maxReadBlockSize,
            int maxReadBlockRowCount,
            DataSize maxMergeDistance,
            DataSize maxBufferSize,
            boolean useColumnIndex,
            boolean useBatchColumnReaders,
            boolean useBatchNestedColumnReaders,
            boolean useBloomFilter)
    {
        this.ignoreStatistics = ignoreStatistics;
        this.maxReadBlockSize = requireNonNull(maxReadBlockSize, "maxReadBlockSize is null");
        checkArgument(maxReadBlockRowCount > 0, "maxReadBlockRowCount must be greater than 0");
        this.maxReadBlockRowCount = maxReadBlockRowCount;
        this.maxMergeDistance = requireNonNull(maxMergeDistance, "maxMergeDistance is null");
        this.maxBufferSize = requireNonNull(maxBufferSize, "maxBufferSize is null");
        this.useColumnIndex = useColumnIndex;
        this.useBatchColumnReaders = useBatchColumnReaders;
        this.useBatchNestedColumnReaders = useBatchNestedColumnReaders;
        this.useBloomFilter = useBloomFilter;
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

    public boolean useBatchColumnReaders()
    {
        return useBatchColumnReaders;
    }

    public boolean useBatchNestedColumnReaders()
    {
        return useBatchNestedColumnReaders;
    }

    public boolean useBloomFilter()
    {
        return useBloomFilter;
    }

    public DataSize getMaxBufferSize()
    {
        return maxBufferSize;
    }

    public int getMaxReadBlockRowCount()
    {
        return maxReadBlockRowCount;
    }

    public ParquetReaderOptions withIgnoreStatistics(boolean ignoreStatistics)
    {
        return new ParquetReaderOptions(
                ignoreStatistics,
                maxReadBlockSize,
                maxReadBlockRowCount,
                maxMergeDistance,
                maxBufferSize,
                useColumnIndex,
                useBatchColumnReaders,
                useBatchNestedColumnReaders,
                useBloomFilter);
    }

    public ParquetReaderOptions withMaxReadBlockSize(DataSize maxReadBlockSize)
    {
        return new ParquetReaderOptions(
                ignoreStatistics,
                maxReadBlockSize,
                maxReadBlockRowCount,
                maxMergeDistance,
                maxBufferSize,
                useColumnIndex,
                useBatchColumnReaders,
                useBatchNestedColumnReaders,
                useBloomFilter);
    }

    public ParquetReaderOptions withMaxReadBlockRowCount(int maxReadBlockRowCount)
    {
        return new ParquetReaderOptions(
                ignoreStatistics,
                maxReadBlockSize,
                maxReadBlockRowCount,
                maxMergeDistance,
                maxBufferSize,
                useColumnIndex,
                useBatchColumnReaders,
                useBatchNestedColumnReaders,
                useBloomFilter);
    }

    public ParquetReaderOptions withMaxMergeDistance(DataSize maxMergeDistance)
    {
        return new ParquetReaderOptions(
                ignoreStatistics,
                maxReadBlockSize,
                maxReadBlockRowCount,
                maxMergeDistance,
                maxBufferSize,
                useColumnIndex,
                useBatchColumnReaders,
                useBatchNestedColumnReaders,
                useBloomFilter);
    }

    public ParquetReaderOptions withMaxBufferSize(DataSize maxBufferSize)
    {
        return new ParquetReaderOptions(
                ignoreStatistics,
                maxReadBlockSize,
                maxReadBlockRowCount,
                maxMergeDistance,
                maxBufferSize,
                useColumnIndex,
                useBatchColumnReaders,
                useBatchNestedColumnReaders,
                useBloomFilter);
    }

    public ParquetReaderOptions withUseColumnIndex(boolean useColumnIndex)
    {
        return new ParquetReaderOptions(
                ignoreStatistics,
                maxReadBlockSize,
                maxReadBlockRowCount,
                maxMergeDistance,
                maxBufferSize,
                useColumnIndex,
                useBatchColumnReaders,
                useBatchNestedColumnReaders,
                useBloomFilter);
    }

    public ParquetReaderOptions withBatchColumnReaders(boolean useBatchColumnReaders)
    {
        return new ParquetReaderOptions(
                ignoreStatistics,
                maxReadBlockSize,
                maxReadBlockRowCount,
                maxMergeDistance,
                maxBufferSize,
                useColumnIndex,
                useBatchColumnReaders,
                useBatchNestedColumnReaders,
                useBloomFilter);
    }

    public ParquetReaderOptions withBatchNestedColumnReaders(boolean useBatchNestedColumnReaders)
    {
        return new ParquetReaderOptions(
                ignoreStatistics,
                maxReadBlockSize,
                maxReadBlockRowCount,
                maxMergeDistance,
                maxBufferSize,
                useColumnIndex,
                useBatchColumnReaders,
                useBatchNestedColumnReaders,
                useBloomFilter);
    }

    public ParquetReaderOptions withBloomFilter(boolean useBloomFilter)
    {
        return new ParquetReaderOptions(
                ignoreStatistics,
                maxReadBlockSize,
                maxReadBlockRowCount,
                maxMergeDistance,
                maxBufferSize,
                useColumnIndex,
                useBatchColumnReaders,
                useBatchNestedColumnReaders,
                useBloomFilter);
    }
}
