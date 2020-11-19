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
package io.prestosql.spi.block;

import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import static io.prestosql.spi.block.EncoderUtil.decodeNullBits;
import static io.prestosql.spi.block.EncoderUtil.encodeNullsAsBits;

public class ShortArrayBlockEncoding
        implements BlockEncoding
{
    public static final String NAME = "SHORT_ARRAY";

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

        encodeNullsAsBits(sliceOutput, block);

        if (!block.mayHaveNull()) {
            sliceOutput.writeBytes(getValuesSlice(block));
        }
        else {
            short[] valuesWithoutNull = new short[positionCount];
            int nonNullPositionCount = 0;
            for (int i = 0; i < positionCount; i++) {
                valuesWithoutNull[nonNullPositionCount] = block.getShort(i, 0);
                if (!block.isNull(i)) {
                    nonNullPositionCount++;
                }
            }

            sliceOutput.writeInt(nonNullPositionCount);
            sliceOutput.writeBytes(Slices.wrappedShortArray(valuesWithoutNull, 0, nonNullPositionCount));
        }
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);

        short[] values = new short[positionCount];
        if (valueIsNull == null) {
            sliceInput.readBytes(Slices.wrappedShortArray(values));
        }
        else {
            int nonNullPositionCount = sliceInput.readInt();
            sliceInput.readBytes(Slices.wrappedShortArray(values, 0, nonNullPositionCount));
            int position = nonNullPositionCount - 1;
            for (int i = positionCount - 1; i >= 0 && position >= 0; i--) {
                values[i] = values[position];
                if (!valueIsNull[i]) {
                    position--;
                }
            }
        }

        return new ShortArrayBlock(0, positionCount, valueIsNull, values);
    }

    private Slice getValuesSlice(Block block)
    {
        if (block instanceof ShortArrayBlock) {
            return ((ShortArrayBlock) block).getValuesSlice();
        }
        else if (block instanceof ShortArrayBlockBuilder) {
            return ((ShortArrayBlockBuilder) block).getValuesSlice();
        }

        throw new IllegalArgumentException("Unexpected block type " + block.getClass().getSimpleName());
    }
}
