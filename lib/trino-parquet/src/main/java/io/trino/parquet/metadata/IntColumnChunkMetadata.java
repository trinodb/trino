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
package io.trino.parquet.metadata;

import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.PrimitiveType;

import java.util.Set;

class IntColumnChunkMetadata
        extends ColumnChunkMetadata
{
    private final int firstDataPage;
    private final int dictionaryPageOffset;
    private final int valueCount;
    private final int totalSize;
    private final int totalUncompressedSize;
    private final Statistics statistics;

    IntColumnChunkMetadata(
            ColumnPath path,
            PrimitiveType type,
            CompressionCodecName codec,
            EncodingStats encodingStats,
            Set<Encoding> encodings,
            Statistics statistics,
            long firstDataPage,
            long dictionaryPageOffset,
            long valueCount,
            long totalSize,
            long totalUncompressedSize)
    {
        super(encodingStats, ColumnChunkProperties.get(path, type, codec, encodings));
        this.firstDataPage = positiveLongToInt(firstDataPage);
        this.dictionaryPageOffset = positiveLongToInt(dictionaryPageOffset);
        this.valueCount = positiveLongToInt(valueCount);
        this.totalSize = positiveLongToInt(totalSize);
        this.totalUncompressedSize = positiveLongToInt(totalUncompressedSize);
        this.statistics = statistics;
    }

    private int positiveLongToInt(long value)
    {
        if (!ColumnChunkMetadata.positiveLongFitsInAnInt(value)) {
            throw new IllegalArgumentException("value should be positive and fit in an int: " + value);
        }
        return (int) (value + Integer.MIN_VALUE);
    }

    private long intToPositiveLong(int value)
    {
        return (long) value - Integer.MIN_VALUE;
    }

    @Override
    public long getFirstDataPageOffset()
    {
        return intToPositiveLong(firstDataPage);
    }

    @Override
    public long getDictionaryPageOffset()
    {
        return intToPositiveLong(dictionaryPageOffset);
    }

    @Override
    public long getValueCount()
    {
        return intToPositiveLong(valueCount);
    }

    @Override
    public long getTotalUncompressedSize()
    {
        return intToPositiveLong(totalUncompressedSize);
    }

    @Override
    public long getTotalSize()
    {
        return intToPositiveLong(totalSize);
    }

    @Override
    public Statistics getStatistics()
    {
        return statistics;
    }
}
