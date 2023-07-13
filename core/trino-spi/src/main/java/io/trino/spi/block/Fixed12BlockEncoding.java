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

public class Fixed12BlockEncoding
        implements BlockEncoding
{
    public static final String NAME = "FIXED12";

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
            int[] valuesWithoutNull = new int[positionCount * 3];
            int nonNullPositionCount = 0;
            for (int i = 0; i < positionCount; i++) {
                valuesWithoutNull[nonNullPositionCount] = block.getInt(i, 0);
                valuesWithoutNull[nonNullPositionCount + 1] = block.getInt(i, 4);
                valuesWithoutNull[nonNullPositionCount + 2] = block.getInt(i, 8);
                if (!block.isNull(i)) {
                    nonNullPositionCount += 3;
                }
            }

            sliceOutput.writeInt(nonNullPositionCount / 3);
            sliceOutput.writeBytes(Slices.wrappedIntArray(valuesWithoutNull, 0, nonNullPositionCount));
        }
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);

        int[] values = new int[positionCount * 3];
        if (valueIsNull == null) {
            sliceInput.readBytes(Slices.wrappedIntArray(values));
        }
        else {
            int nonNullPositionCount = sliceInput.readInt();
            sliceInput.readBytes(Slices.wrappedIntArray(values, 0, nonNullPositionCount * 3));
            int position = 3 * (nonNullPositionCount - 1);
            for (int i = positionCount - 1; i >= 0 && position >= 0; i--) {
                System.arraycopy(values, position, values, 3 * i, 3);
                if (!valueIsNull[i]) {
                    position -= 3;
                }
            }
        }
        return new Fixed12Block(0, positionCount, valueIsNull, values);
    }

    private static Slice getValuesSlice(Block block)
    {
        if (block instanceof Fixed12Block) {
            return ((Fixed12Block) block).getValuesSlice();
        }
        if (block instanceof Fixed12BlockBuilder) {
            return ((Fixed12BlockBuilder) block).getValuesSlice();
        }

        throw new IllegalArgumentException("Unexpected block type " + block.getClass().getSimpleName());
    }
}
