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

import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import static io.trino.spi.block.EncoderUtil.decodeNullBits;
import static io.trino.spi.block.EncoderUtil.encodeNullsAsBits;

public class Int128ArrayBlockEncoding
        implements BlockEncoding
{
    public static final String NAME = "INT128_ARRAY";

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
            if (block instanceof Int128ArrayBlock valueBlock) {
                sliceOutput.writeLongs(valueBlock.getRawValues(), valueBlock.getPositionOffset() * 2, valueBlock.getPositionCount() * 2);
            }
            else if (block instanceof Int128ArrayBlockBuilder blockBuilder) {
                sliceOutput.writeLongs(blockBuilder.getRawValues(), 0, blockBuilder.getPositionCount() * 2);
            }
            else {
                throw new IllegalArgumentException("Unexpected block type " + block.getClass().getSimpleName());
            }
        }
        else {
            long[] valuesWithoutNull = new long[positionCount * 2];
            int nonNullPositionCount = 0;
            for (int i = 0; i < positionCount; i++) {
                valuesWithoutNull[nonNullPositionCount] = block.getLong(i, 0);
                valuesWithoutNull[nonNullPositionCount + 1] = block.getLong(i, 8);
                if (!block.isNull(i)) {
                    nonNullPositionCount += 2;
                }
            }

            sliceOutput.writeInt(nonNullPositionCount / 2);
            sliceOutput.writeLongs(valuesWithoutNull, 0, nonNullPositionCount);
        }
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);

        long[] values = new long[positionCount * 2];
        if (valueIsNull == null) {
            sliceInput.readLongs(values);
        }
        else {
            int nonNullPositionCount = sliceInput.readInt();
            sliceInput.readLongs(values, 0, nonNullPositionCount * 2);
            int position = 2 * (nonNullPositionCount - 1);
            for (int i = positionCount - 1; i >= 0 && position >= 0; i--) {
                System.arraycopy(values, position, values, 2 * i, 2);
                if (!valueIsNull[i]) {
                    position -= 2;
                }
            }
        }

        return new Int128ArrayBlock(0, positionCount, valueIsNull, values);
    }
}
