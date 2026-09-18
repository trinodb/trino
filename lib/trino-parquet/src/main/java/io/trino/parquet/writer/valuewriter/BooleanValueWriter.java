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

import io.trino.spi.block.BitArrayBlock;
import io.trino.spi.block.ValueBlock;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.schema.PrimitiveType;

import static io.trino.spi.block.Bitmap.getBits;

public class BooleanValueWriter
        extends PrimitiveValueWriter
{
    public BooleanValueWriter(ValuesWriter valuesWriter, PrimitiveType parquetType)
    {
        super(parquetType, valuesWriter);
    }

    @Override
    protected void writeRepeated(ValueBlock block, int count)
    {
        ValuesWriter valuesWriter = getValuesWriter();
        Statistics<?> statistics = getStatistics();
        boolean value = ((BitArrayBlock) block).getBoolean(0);
        if (valuesWriter instanceof TrinoBooleanPlainValuesWriter packedWriter) {
            long bits = 0;
            if (value) {
                bits = -1L;
            }
            for (int written = 0; written < count; written += Long.SIZE) {
                packedWriter.writeBits(bits, Math.min(Long.SIZE, count - written));
            }
        }
        else {
            for (int i = 0; i < count; i++) {
                valuesWriter.writeBoolean(value);
            }
        }
        statistics.updateStats(value);
    }

    @Override
    protected void writeValueBlock(ValueBlock block)
    {
        ValuesWriter valuesWriter = getValuesWriter();
        Statistics<?> statistics = getStatistics();
        BitArrayBlock bitArrayBlock = (BitArrayBlock) block;
        if (valuesWriter instanceof TrinoBooleanPlainValuesWriter packedWriter) {
            writeBitArrayBlock(bitArrayBlock, packedWriter, statistics);
            return;
        }

        boolean mayHaveNull = block.mayHaveNull();
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (!mayHaveNull || !block.isNull(i)) {
                boolean value = bitArrayBlock.getBoolean(i);
                valuesWriter.writeBoolean(value);
                statistics.updateStats(value);
            }
        }
    }

    @Override
    protected void writePositions(ValueBlock block, int[] positions, int offset, int length)
    {
        ValuesWriter valuesWriter = getValuesWriter();
        Statistics<?> statistics = getStatistics();
        BitArrayBlock bitArrayBlock = (BitArrayBlock) block;
        boolean mayHaveNull = block.mayHaveNull();
        for (int i = 0; i < length; i++) {
            int position = positions[offset + i];
            if (!mayHaveNull || !block.isNull(position)) {
                boolean value = bitArrayBlock.getBoolean(position);
                valuesWriter.writeBoolean(value);
                statistics.updateStats(value);
            }
        }
    }

    private static void writeBitArrayBlock(BitArrayBlock block, TrinoBooleanPlainValuesWriter valuesWriter, Statistics<?> statistics)
    {
        int rawOffset = block.getRawValuesOffset();
        long[] values = block.getRawValues();
        long[] validity = block.getRawValueIsValid();
        boolean hasTrue = false;
        boolean hasFalse = false;
        for (int position = 0; position < block.getPositionCount(); position += Long.SIZE) {
            int positionCount = Math.min(Long.SIZE, block.getPositionCount() - position);
            long bits = getBits(values, rawOffset, position, positionCount);
            int valueCount = positionCount;
            if (validity != null) {
                long validBits = getBits(validity, rawOffset, position, positionCount);
                valueCount = Long.bitCount(validBits);
                bits = Long.compress(bits, validBits);
            }
            valuesWriter.writeBits(bits, valueCount);
            hasTrue |= bits != 0;
            hasFalse |= Long.bitCount(bits) < valueCount;
        }
        if (hasFalse) {
            statistics.updateStats(false);
        }
        if (hasTrue) {
            statistics.updateStats(true);
        }
    }
}
