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
package org.apache.parquet.hadoop.metadata;

import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.schema.PrimitiveType;

import java.util.Set;

class LongColumnChunkMetaData
        extends ColumnChunkMetaData
{
    private final long firstDataPageOffset;
    private final long dictionaryPageOffset;
    private final long valueCount;
    private final long totalSize;
    private final long totalUncompressedSize;
    private final Statistics statistics;

    LongColumnChunkMetaData(
            ColumnPath path,
            PrimitiveType type,
            CompressionCodecName codec,
            EncodingStats encodingStats,
            Set<Encoding> encodings,
            Statistics statistics,
            long firstDataPageOffset,
            long dictionaryPageOffset,
            long valueCount,
            long totalSize,
            long totalUncompressedSize)
    {
        super(encodingStats, ColumnChunkProperties.get(path, type, codec, encodings));
        this.firstDataPageOffset = firstDataPageOffset;
        this.dictionaryPageOffset = dictionaryPageOffset;
        this.valueCount = valueCount;
        this.totalSize = totalSize;
        this.totalUncompressedSize = totalUncompressedSize;
        this.statistics = statistics;
    }

    @Override
    public long getFirstDataPageOffset()
    {
        return firstDataPageOffset;
    }

    @Override
    public long getDictionaryPageOffset()
    {
        return dictionaryPageOffset;
    }

    @Override
    public long getValueCount()
    {
        return valueCount;
    }

    @Override
    public long getTotalUncompressedSize()
    {
        return totalUncompressedSize;
    }

    @Override
    public long getTotalSize()
    {
        return totalSize;
    }

    @Override
    public Statistics getStatistics()
    {
        return statistics;
    }
}
