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
import jakarta.annotation.Nullable;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;

import java.util.Arrays;

import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Double.longBitsToDouble;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public abstract class PrimitiveValueWriter
        implements AutoCloseable
{
    // number of values gathered into a buffer before they are handed to the values writer
    private static final int BATCH_SIZE = 1024;

    private Statistics<?> statistics;
    private final PrimitiveType parquetType;
    // Type with the column order under which statistics are computed, see ParquetMetadataConverter#toTypeDefinedOrder
    private final PrimitiveType statisticsType;
    private final ValuesWriter valuesWriter;
    // statistics of unsigned integer types are not ordered like the Java values, so they are updated value by value
    private final boolean signedIntegerOrder;
    @Nullable
    private int[] intBuffer;
    @Nullable
    private long[] longBuffer;

    public PrimitiveValueWriter(PrimitiveType parquetType, ValuesWriter valuesWriter)
    {
        this.parquetType = requireNonNull(parquetType, "parquetType is null");
        this.statisticsType = ParquetMetadataConverter.toTypeDefinedOrder(parquetType);
        this.valuesWriter = requireNonNull(valuesWriter, "valuesWriter is null");
        this.statistics = Statistics.createStats(statisticsType);
        this.signedIntegerOrder = !(statisticsType.getLogicalTypeAnnotation() instanceof IntLogicalTypeAnnotation intType) || intType.isSigned();
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

    Statistics<?> getStatistics()
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

    public long getEstimatedDataPageSize()
    {
        return valuesWriter.getEstimatedDataPageSize();
    }

    public long getEstimatedDictionaryPageSize()
    {
        return valuesWriter.getEstimatedDictionaryPageSize();
    }

    public BytesInput getBytes()
    {
        return valuesWriter.getBytes();
    }

    public Encoding getEncoding()
    {
        return valuesWriter.getEncoding();
    }

    /// Resets the writer for the next page and returns the statistics of the values written to the page that was reset
    public Statistics<?> resetAndGetStatistics()
    {
        valuesWriter.reset();
        Statistics<?> finished = statistics;
        statistics = Statistics.createStats(statisticsType);
        return finished;
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
        return valuesWriter.getAllocatedSize() + sizeOf(intBuffer) + sizeOf(longBuffer);
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

    /// Returns a reusable buffer for gathering values before they are written in a batch
    protected final int[] intBuffer()
    {
        if (intBuffer == null) {
            intBuffer = new int[BATCH_SIZE];
        }
        return intBuffer;
    }

    /// Returns a reusable buffer for gathering values before they are written in a batch
    protected final long[] longBuffer()
    {
        if (longBuffer == null) {
            longBuffer = new long[BATCH_SIZE];
        }
        return longBuffer;
    }

    protected final void writeIntegers(int[] values, int offset, int length)
    {
        if (length == 0) {
            return;
        }
        valuesWriter.writeIntegers(values, offset, length);
        if (!signedIntegerOrder) {
            for (int i = offset; i < offset + length; i++) {
                statistics.updateStats(values[i]);
            }
            return;
        }
        int minValue = values[offset];
        int maxValue = minValue;
        for (int i = offset + 1; i < offset + length; i++) {
            minValue = min(minValue, values[i]);
            maxValue = max(maxValue, values[i]);
        }
        statistics.updateStats(minValue);
        statistics.updateStats(maxValue);
    }

    protected final void writeLongs(long[] values, int offset, int length)
    {
        if (length == 0) {
            return;
        }
        valuesWriter.writeLongs(values, offset, length);
        if (!signedIntegerOrder) {
            for (int i = offset; i < offset + length; i++) {
                statistics.updateStats(values[i]);
            }
            return;
        }
        long minValue = values[offset];
        long maxValue = minValue;
        for (int i = offset + 1; i < offset + length; i++) {
            minValue = min(minValue, values[i]);
            maxValue = max(maxValue, values[i]);
        }
        statistics.updateStats(minValue);
        statistics.updateStats(maxValue);
    }

    /// Writes doubles given as their raw long bits
    protected final void writeDoubleBits(long[] values, int offset, int length)
    {
        if (length == 0) {
            return;
        }
        valuesWriter.writeDoubleBits(values, offset, length);
        // statistics count NaN values and normalize zeros, so they see every value
        for (int i = offset; i < offset + length; i++) {
            statistics.updateStats(longBitsToDouble(values[i]));
        }
    }

    /// Writes floats given as their raw int bits
    protected final void writeFloatBits(int[] values, int offset, int length)
    {
        if (length == 0) {
            return;
        }
        valuesWriter.writeFloatBits(values, offset, length);
        // statistics count NaN values and normalize zeros, so they see every value
        for (int i = offset; i < offset + length; i++) {
            statistics.updateStats(intBitsToFloat(values[i]));
        }
    }

    protected final void writeRepeatedInteger(int value, int count)
    {
        int[] buffer = intBuffer();
        Arrays.fill(buffer, 0, min(count, buffer.length), value);
        for (int written = 0; written < count; written += buffer.length) {
            valuesWriter.writeIntegers(buffer, 0, min(count - written, buffer.length));
        }
        statistics.updateStats(value);
    }

    protected final void writeRepeatedLong(long value, int count)
    {
        long[] buffer = longBuffer();
        Arrays.fill(buffer, 0, min(count, buffer.length), value);
        for (int written = 0; written < count; written += buffer.length) {
            valuesWriter.writeLongs(buffer, 0, min(count - written, buffer.length));
        }
        statistics.updateStats(value);
    }

    protected final void writeRepeatedDoubleBits(long value, int count)
    {
        long[] buffer = longBuffer();
        Arrays.fill(buffer, 0, min(count, buffer.length), value);
        for (int written = 0; written < count; written += buffer.length) {
            valuesWriter.writeDoubleBits(buffer, 0, min(count - written, buffer.length));
        }
        statistics.updateStats(longBitsToDouble(value));
    }

    protected final void writeRepeatedFloatBits(int value, int count)
    {
        int[] buffer = intBuffer();
        Arrays.fill(buffer, 0, min(count, buffer.length), value);
        for (int written = 0; written < count; written += buffer.length) {
            valuesWriter.writeFloatBits(buffer, 0, min(count - written, buffer.length));
        }
        statistics.updateStats(intBitsToFloat(value));
    }

    protected abstract void writeValueBlock(ValueBlock block);

    protected abstract void writeRepeated(ValueBlock block, int count);

    protected abstract void writePositions(ValueBlock block, int[] positions, int offset, int length);
}
