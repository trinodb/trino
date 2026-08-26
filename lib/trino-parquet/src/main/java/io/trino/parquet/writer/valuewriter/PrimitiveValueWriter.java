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
package io.trino.parquet.writer.valuewriter;

import io.trino.parquet.ParquetMetadataConverter;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.ValueBlock;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.schema.PrimitiveType;

import static java.util.Objects.requireNonNull;

public abstract class PrimitiveValueWriter
        implements AutoCloseable
{
    private Statistics<?> statistics;
    private final PrimitiveType parquetType;
    // Type with the column order under which statistics are computed, see ParquetMetadataConverter#toTypeDefinedOrder
    private final PrimitiveType statisticsType;
    private final ValuesWriter valuesWriter;

    public PrimitiveValueWriter(PrimitiveType parquetType, ValuesWriter valuesWriter)
    {
        this.parquetType = requireNonNull(parquetType, "parquetType is null");
        this.statisticsType = ParquetMetadataConverter.toTypeDefinedOrder(parquetType);
        this.valuesWriter = requireNonNull(valuesWriter, "valuesWriter is null");
        this.statistics = Statistics.createStats(statisticsType);
    }

    /// Creates statistics computed with the type-defined column order semantics, see ParquetMetadataConverter#toTypeDefinedOrder
    public static Statistics<?> createStatistics(PrimitiveType type)
    {
        return Statistics.createStats(ParquetMetadataConverter.toTypeDefinedOrder(type));
    }

    ValuesWriter getValuesWriter()
    {
        return valuesWriter;
    }

    public Statistics<?> getStatistics()
    {
        return statistics;
    }

    protected int getTypeLength()
    {
        return parquetType.getTypeLength();
    }

    public long getBufferedSize()
    {
        return valuesWriter.getBufferedSize();
    }

    public long getEstimatedBufferedSize()
    {
        return switch (valuesWriter) {
            case DictionaryFallbackValuesWriter dictionaryFallbackValuesWriter -> dictionaryFallbackValuesWriter.getEstimatedBufferedSize();
            case BloomFilterValuesWriter bloomFilterValuesWriter -> bloomFilterValuesWriter.getEstimatedBufferedSize();
            default -> valuesWriter.getBufferedSize();
        };
    }

    public BytesInput getBytes()
    {
        return valuesWriter.getBytes();
    }

    public Encoding getEncoding()
    {
        return valuesWriter.getEncoding();
    }

    public void reset()
    {
        valuesWriter.reset();
        this.statistics = Statistics.createStats(statisticsType);
    }

    @Override
    public void close()
    {
        valuesWriter.close();
    }

    public DictionaryPage toDictPageAndClose()
    {
        return valuesWriter.toDictPageAndClose();
    }

    public void resetDictionary()
    {
        valuesWriter.resetDictionary();
    }

    public long getAllocatedSize()
    {
        return valuesWriter.getAllocatedSize();
    }

    public final void write(Block rawBlock)
    {
        switch (rawBlock) {
            case RunLengthEncodedBlock rleBlock -> {
                ValueBlock valueBlock = rleBlock.getValue();
                if (!valueBlock.isNull(0)) {
                    writeRepeated(valueBlock, rleBlock.getPositionCount());
                }
            }
            case DictionaryBlock dictionaryBlock -> writePositions(dictionaryBlock.getDictionary(), dictionaryBlock.getRawIds(), dictionaryBlock.getRawIdsOffset(), dictionaryBlock.getPositionCount());
            case ValueBlock valueBlock -> writeValueBlock(valueBlock);
        }
    }

    protected abstract void writeValueBlock(ValueBlock block);

    protected abstract void writeRepeated(ValueBlock block, int count);

    protected abstract void writePositions(ValueBlock block, int[] positions, int offset, int length);
}
