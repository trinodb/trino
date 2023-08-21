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
import static io.trino.spi.block.EncoderUtil.retrieveNullBits;
import static java.lang.System.arraycopy;

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
            if (block instanceof ShortArrayBlock valueBlock) {
                sliceOutput.writeShorts(valueBlock.getRawValues(), valueBlock.getRawValuesOffset(), valueBlock.getPositionCount());
            }
            else if (block instanceof ShortArrayBlockBuilder blockBuilder) {
                sliceOutput.writeShorts(blockBuilder.getRawValues(), 0, blockBuilder.getPositionCount());
            }
            else {
                throw new IllegalArgumentException("Unexpected block type " + block.getClass().getSimpleName());
            }
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
            sliceOutput.writeShorts(valuesWithoutNull, 0, nonNullPositionCount);
        }
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();

        byte[] valueIsNullPacked = retrieveNullBits(sliceInput, positionCount);
        short[] values = new short[positionCount];

        if (valueIsNullPacked == null) {
            sliceInput.readShorts(values);
            return new ShortArrayBlock(0, positionCount, null, values);
        }
        boolean[] valueIsNull = decodeNullBits(valueIsNullPacked, positionCount);

        int nonNullPositionCount = sliceInput.readInt();
        sliceInput.readShorts(values, 0, nonNullPositionCount);
        int position = nonNullPositionCount - 1;

        // Handle Last (positionCount % 8) values
        for (int i = positionCount - 1; i >= (positionCount & ~0b111) && position >= 0; i--) {
            values[i] = values[position];
            if (!valueIsNull[i]) {
                position--;
            }
        }

        // Handle the remaining positions.
        for (int i = (positionCount & ~0b111) - 8; i >= 0 && position >= 0; i -= 8) {
            byte packed = valueIsNullPacked[i >>> 3];
            if (packed == 0) { // Only values
                arraycopy(values, position - 7, values, i, 8);
                position -= 8;
            }
            else if (packed != -1) { // At least one non-null
                for (int j = i + 7; j >= i && position >= 0; j--) {
                    values[j] = values[position];
                    if (!valueIsNull[j]) {
                        position--;
                    }
                }
            }
            // Do nothing if there are only nulls
        }
        return new ShortArrayBlock(0, positionCount, valueIsNull, values);
    }
}
