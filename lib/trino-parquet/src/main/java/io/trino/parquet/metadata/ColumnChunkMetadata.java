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
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

import java.util.Set;

public abstract class ColumnChunkMetadata
{
    protected int rowGroupOrdinal = -1;

    public static ColumnChunkMetadata get(
            ColumnPath path,
            PrimitiveType type,
            CompressionCodecName codec,
            EncodingStats encodingStats,
            Set<Encoding> encodings,
            Statistics<?> statistics,
            long firstDataPage,
            long dictionaryPageOffset,
            long valueCount,
            long totalSize,
            long totalUncompressedSize)
    {
        if (positiveLongFitsInAnInt(firstDataPage)
                && positiveLongFitsInAnInt(dictionaryPageOffset)
                && positiveLongFitsInAnInt(valueCount)
                && positiveLongFitsInAnInt(totalSize)
                && positiveLongFitsInAnInt(totalUncompressedSize)) {
            return new IntColumnChunkMetadata(
                    path, type, codec,
                    encodingStats, encodings,
                    statistics,
                    firstDataPage,
                    dictionaryPageOffset,
                    valueCount,
                    totalSize,
                    totalUncompressedSize);
        }
        return new LongColumnChunkMetadata(
                path, type, codec,
                encodingStats, encodings,
                statistics,
                firstDataPage,
                dictionaryPageOffset,
                valueCount,
                totalSize,
                totalUncompressedSize);
    }

    public void setRowGroupOrdinal(int rowGroupOrdinal)
    {
        this.rowGroupOrdinal = rowGroupOrdinal;
    }

    public int getRowGroupOrdinal()
    {
        return rowGroupOrdinal;
    }

    public long getStartingPos()
    {
        decryptIfNeeded();
        long dictionaryPageOffset = getDictionaryPageOffset();
        long firstDataPageOffset = getFirstDataPageOffset();
        if (dictionaryPageOffset > 0 && dictionaryPageOffset < firstDataPageOffset) {
            return dictionaryPageOffset;
        }
        return firstDataPageOffset;
    }

    protected static boolean positiveLongFitsInAnInt(long value)
    {
        return (value >= 0) && (value + Integer.MIN_VALUE <= Integer.MAX_VALUE);
    }

    EncodingStats encodingStats;

    ColumnChunkProperties properties;

    private IndexReference columnIndexReference;
    private IndexReference offsetIndexReference;

    private long bloomFilterOffset = -1;

    protected ColumnChunkMetadata(ColumnChunkProperties columnChunkProperties)
    {
        this(null, columnChunkProperties);
    }

    protected ColumnChunkMetadata(EncodingStats encodingStats, ColumnChunkProperties columnChunkProperties)
    {
        this.encodingStats = encodingStats;
        this.properties = columnChunkProperties;
    }

    protected void decryptIfNeeded() {}

    public CompressionCodecName getCodec()
    {
        decryptIfNeeded();
        return properties.codec();
    }

    public ColumnPath getPath()
    {
        return properties.path();
    }

    public PrimitiveTypeName getType()
    {
        decryptIfNeeded();
        return properties.type().getPrimitiveTypeName();
    }

    public PrimitiveType getPrimitiveType()
    {
        decryptIfNeeded();
        return properties.type();
    }

    public abstract long getFirstDataPageOffset();

    public abstract long getDictionaryPageOffset();

    public abstract long getValueCount();

    public abstract long getTotalUncompressedSize();

    public abstract long getTotalSize();

    public abstract Statistics<?> getStatistics();

    public IndexReference getColumnIndexReference()
    {
        decryptIfNeeded();
        return columnIndexReference;
    }

    public void setColumnIndexReference(IndexReference indexReference)
    {
        this.columnIndexReference = indexReference;
    }

    public IndexReference getOffsetIndexReference()
    {
        decryptIfNeeded();
        return offsetIndexReference;
    }

    public void setOffsetIndexReference(IndexReference offsetIndexReference)
    {
        this.offsetIndexReference = offsetIndexReference;
    }

    public void setBloomFilterOffset(long bloomFilterOffset)
    {
        this.bloomFilterOffset = bloomFilterOffset;
    }

    public long getBloomFilterOffset()
    {
        decryptIfNeeded();
        return bloomFilterOffset;
    }

    public Set<Encoding> getEncodings()
    {
        decryptIfNeeded();
        return properties.encodings();
    }

    public EncodingStats getEncodingStats()
    {
        decryptIfNeeded();
        return encodingStats;
    }

    @Override
    public String toString()
    {
        decryptIfNeeded();
        return "ColumnMetaData{" + properties.toString() + ", " + getFirstDataPageOffset() + "}";
    }
}
