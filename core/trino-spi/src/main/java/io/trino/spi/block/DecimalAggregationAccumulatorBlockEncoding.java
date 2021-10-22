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
package io.trino.spi.block;

import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import static io.trino.spi.block.EncoderUtil.decodeNullBits;
import static io.trino.spi.block.EncoderUtil.encodeNullsAsBits;

public class DecimalAggregationAccumulatorBlockEncoding
        implements BlockEncoding
{
    public static final String NAME = "DECIMAL_AGGREGATION_ACCUMULATOR";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
    {
        int positionCount = block.getPositionCount();
        sliceOutput.appendInt(positionCount);
        int numberOfNoOverflowValuesPerEntry = getNumberOfNoOverflowValuesPerEntry(block);
        sliceOutput.appendInt(numberOfNoOverflowValuesPerEntry);

        encodeNullsAsBits(sliceOutput, block);

        if (!block.mayHaveNull()) {
            sliceOutput.writeBytes(getValuesSlice(block));
        }
        else {
            long[] valuesWithoutNull = new long[positionCount * numberOfNoOverflowValuesPerEntry];
            int nonNullPositionCount = 0;
            for (int i = 0; i < positionCount; i++) {
                for (int j = 0; j < numberOfNoOverflowValuesPerEntry; j++) {
                    valuesWithoutNull[nonNullPositionCount + j] = block.getLong(i, j * Long.BYTES);
                }
                int isNotNull = !block.isNull(i) ? 1 : 0;
                nonNullPositionCount += numberOfNoOverflowValuesPerEntry * isNotNull;
            }

            sliceOutput.writeInt(nonNullPositionCount / numberOfNoOverflowValuesPerEntry);
            sliceOutput.writeBytes(Slices.wrappedLongArray(valuesWithoutNull, 0, nonNullPositionCount));
        }
        Slice overflow = getOverflowSlice(block);
        sliceOutput.writeBoolean(overflow != null);
        if (overflow != null) {
            sliceOutput.writeBytes(overflow);
        }
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();
        int numberOfNoOverflowValuesPerEntry = sliceInput.readInt();

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);

        long[] values = new long[positionCount * numberOfNoOverflowValuesPerEntry];
        if (valueIsNull == null) {
            sliceInput.readBytes(Slices.wrappedLongArray(values));
        }
        else {
            int nonNullPositionCount = sliceInput.readInt();
            sliceInput.readBytes(Slices.wrappedLongArray(values, 0, nonNullPositionCount * numberOfNoOverflowValuesPerEntry));
            int position = numberOfNoOverflowValuesPerEntry * (nonNullPositionCount - 1);
            for (int i = positionCount - 1; i >= 0 && position >= 0; i--) {
                System.arraycopy(values, position, values, numberOfNoOverflowValuesPerEntry * i, numberOfNoOverflowValuesPerEntry);
                if (!valueIsNull[i]) {
                    position -= numberOfNoOverflowValuesPerEntry;
                }
            }
        }
        boolean nonZeroOverflow = sliceInput.readBoolean();
        long[] overflow = null;
        if (nonZeroOverflow) {
            overflow = new long[positionCount];
            sliceInput.readBytes(Slices.wrappedLongArray(overflow));
        }

        return new DecimalAggregationAccumulatorBlock(
                0,
                positionCount,
                valueIsNull,
                values,
                overflow,
                numberOfNoOverflowValuesPerEntry);
    }

    private Slice getValuesSlice(Block block)
    {
        if (block instanceof DecimalAggregationAccumulatorBlock) {
            return ((DecimalAggregationAccumulatorBlock) block).getValuesSlice();
        }
        else if (block instanceof DecimalAggregationAccumulatorBlockBuilder) {
            return ((DecimalAggregationAccumulatorBlockBuilder) block).getValuesSlice();
        }

        throw new IllegalArgumentException("Unexpected block type " + block.getClass().getSimpleName());
    }

    private Slice getOverflowSlice(Block block)
    {
        if (block instanceof DecimalAggregationAccumulatorBlock) {
            return ((DecimalAggregationAccumulatorBlock) block).getOverflowSlice();
        }
        else if (block instanceof DecimalAggregationAccumulatorBlockBuilder) {
            return ((DecimalAggregationAccumulatorBlockBuilder) block).getOverflowSlice();
        }

        throw new IllegalArgumentException("Unexpected block type " + block.getClass().getSimpleName());
    }

    private int getNumberOfNoOverflowValuesPerEntry(Block block)
    {
        if (block instanceof DecimalAggregationAccumulatorBlock) {
            return ((DecimalAggregationAccumulatorBlock) block).getNumberOfNoOverflowValuesPerEntry();
        }
        else if (block instanceof DecimalAggregationAccumulatorBlockBuilder) {
            return ((DecimalAggregationAccumulatorBlockBuilder) block).getNumberOfNoOverflowValuesPerEntry();
        }

        throw new IllegalArgumentException("Unexpected block type " + block.getClass().getSimpleName());
    }
}
