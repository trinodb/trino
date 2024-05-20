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
package io.trino.parquet.writer;

import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;
import org.apache.parquet.column.ParquetProperties;

import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class ParquetWriterOptions
{
    private static final DataSize DEFAULT_MAX_ROW_GROUP_SIZE = DataSize.of(128, MEGABYTE);
    private static final DataSize DEFAULT_MAX_PAGE_SIZE = DataSize.ofBytes(ParquetProperties.DEFAULT_PAGE_SIZE);
    // org.apache.parquet.column.DEFAULT_PAGE_ROW_COUNT_LIMIT is 20_000 to improve selectivity of page indexes
    // This value should be revisited when TODO https://github.com/trinodb/trino/issues/9359 is implemented
    public static final int DEFAULT_MAX_PAGE_VALUE_COUNT = 60_000;
    public static final int DEFAULT_BATCH_SIZE = 10_000;
    public static final DataSize DEFAULT_MAX_BLOOM_FILTER_SIZE = DataSize.of(1, MEGABYTE);
    public static final double DEFAULT_BLOOM_FILTER_FPP = 0.05;

    public static ParquetWriterOptions.Builder builder()
    {
        return new ParquetWriterOptions.Builder();
    }

    private final int maxRowGroupSize;
    private final int maxPageSize;
    private final int maxPageValueCount;
    private final int batchSize;
    private final int maxBloomFilterSize;
    private final double bloomFilterFpp;
    // Set of column dot paths to columns with bloom filters
    private final Set<String> bloomFilterColumns;

    private ParquetWriterOptions(
            DataSize maxBlockSize,
            DataSize maxPageSize,
            int maxPageValueCount,
            int batchSize,
            DataSize maxBloomFilterSize,
            double bloomFilterFpp,
            Set<String> bloomFilterColumns)
    {
        this.maxRowGroupSize = Ints.saturatedCast(maxBlockSize.toBytes());
        this.maxPageSize = Ints.saturatedCast(maxPageSize.toBytes());
        this.maxPageValueCount = maxPageValueCount;
        this.batchSize = batchSize;
        this.maxBloomFilterSize = Ints.saturatedCast(maxBloomFilterSize.toBytes());
        this.bloomFilterFpp = bloomFilterFpp;
        this.bloomFilterColumns = ImmutableSet.copyOf(bloomFilterColumns);
        checkArgument(this.bloomFilterFpp > 0.0 && this.bloomFilterFpp < 1.0, "bloomFilterFpp should be > 0.0 & < 1.0");
    }

    public int getMaxRowGroupSize()
    {
        return maxRowGroupSize;
    }

    public int getMaxPageSize()
    {
        return maxPageSize;
    }

    public int getMaxPageValueCount()
    {
        return maxPageValueCount;
    }

    public int getBatchSize()
    {
        return batchSize;
    }

    public int getMaxBloomFilterSize()
    {
        return maxBloomFilterSize;
    }

    public Set<String> getBloomFilterColumns()
    {
        return bloomFilterColumns;
    }

    public double getBLoomFilterFpp()
    {
        return bloomFilterFpp;
    }

    public static class Builder
    {
        private DataSize maxBlockSize = DEFAULT_MAX_ROW_GROUP_SIZE;
        private DataSize maxPageSize = DEFAULT_MAX_PAGE_SIZE;
        private int maxPageValueCount = DEFAULT_MAX_PAGE_VALUE_COUNT;
        private int batchSize = DEFAULT_BATCH_SIZE;
        private DataSize maxBloomFilterSize = DEFAULT_MAX_BLOOM_FILTER_SIZE;
        private Set<String> bloomFilterColumns = ImmutableSet.of();
        private double bloomFilterFpp = DEFAULT_BLOOM_FILTER_FPP;

        public Builder setMaxBlockSize(DataSize maxBlockSize)
        {
            this.maxBlockSize = maxBlockSize;
            return this;
        }

        public Builder setMaxPageSize(DataSize maxPageSize)
        {
            this.maxPageSize = maxPageSize;
            return this;
        }

        public Builder setMaxPageValueCount(int maxPageValueCount)
        {
            this.maxPageValueCount = maxPageValueCount;
            return this;
        }

        public Builder setBatchSize(int batchSize)
        {
            this.batchSize = batchSize;
            return this;
        }

        public Builder setMaxBloomFilterSize(DataSize maxBloomFilterSize)
        {
            this.maxBloomFilterSize = maxBloomFilterSize;
            return this;
        }

        public Builder setBloomFilterFpp(double bloomFilterFpp)
        {
            this.bloomFilterFpp = bloomFilterFpp;
            return this;
        }

        public Builder setBloomFilterColumns(Set<String> columns)
        {
            this.bloomFilterColumns = columns;
            return this;
        }

        public ParquetWriterOptions build()
        {
            return new ParquetWriterOptions(
                    maxBlockSize,
                    maxPageSize,
                    maxPageValueCount,
                    batchSize,
                    maxBloomFilterSize,
                    bloomFilterFpp,
                    bloomFilterColumns);
        }
    }
}
