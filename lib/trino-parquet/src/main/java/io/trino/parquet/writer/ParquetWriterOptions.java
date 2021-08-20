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
import io.airlift.units.DataSize;
import org.apache.parquet.hadoop.ParquetWriter;

import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class ParquetWriterOptions
{
    private static final double DEFAULT_BLOOM_FILTER_FPP = 0.01;
    private static final DataSize DEFAULT_MAX_ROW_GROUP_SIZE = DataSize.ofBytes(ParquetWriter.DEFAULT_BLOCK_SIZE);
    private static final DataSize DEFAULT_MAX_PAGE_SIZE = DataSize.ofBytes(ParquetWriter.DEFAULT_PAGE_SIZE);

    public static ParquetWriterOptions.Builder builder()
    {
        return new ParquetWriterOptions.Builder();
    }

    private final int maxRowGroupSize;
    private final int maxPageSize;
    private final double bloomFilterFpp;
    private final Set<String> bloomFilterColumns;

    private ParquetWriterOptions(DataSize maxBlockSize, DataSize maxPageSize, Set<String> bloomFilterColumns, double bloomFilterFpp)
    {
        this.maxRowGroupSize = toIntExact(requireNonNull(maxBlockSize, "maxBlockSize is null").toBytes());
        this.maxPageSize = toIntExact(requireNonNull(maxPageSize, "maxPageSize is null").toBytes());
        this.bloomFilterFpp = bloomFilterFpp;
        this.bloomFilterColumns = ImmutableSet.copyOf(bloomFilterColumns);
    }

    public long getMaxRowGroupSize()
    {
        return maxRowGroupSize;
    }

    public int getMaxPageSize()
    {
        return maxPageSize;
    }

    public boolean isBloomFilterEnabled()
    {
        return !bloomFilterColumns.isEmpty();
    }

    public boolean isBloomFilterColumn(String columnName)
    {
        return bloomFilterColumns.contains(columnName);
    }

    public double getBloomFilterFpp()
    {
        return this.bloomFilterFpp;
    }

    public Set<String> getBloomFilterColumns()
    {
        return this.bloomFilterColumns;
    }

    public static class Builder
    {
        private DataSize maxBlockSize = DEFAULT_MAX_ROW_GROUP_SIZE;
        private DataSize maxPageSize = DEFAULT_MAX_PAGE_SIZE;
        private double bloomFilterFpp = DEFAULT_BLOOM_FILTER_FPP;
        private Set<String> bloomFilterColumns = ImmutableSet.of();

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

        public Builder setBloomFilterColumns(Set<String> columns)
        {
            this.bloomFilterColumns = columns;
            return this;
        }

        public Builder setBloomFilterFpp(double value)
        {
            checkArgument(value > 0.0 && value < 1.0, "False positive probability should be between 0 and 1");
            this.bloomFilterFpp = value;
            return this;
        }

        public ParquetWriterOptions build()
        {
            return new ParquetWriterOptions(maxBlockSize, maxPageSize, bloomFilterColumns, bloomFilterFpp);
        }
    }
}
