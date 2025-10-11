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
import jakarta.annotation.Nullable;

import static io.trino.spi.block.EncoderUtil.decodeNullBits;
import static io.trino.spi.block.EncoderUtil.encodeNullsAsBits;
import static io.trino.spi.block.EncoderUtil.retrieveNullBits;
import static java.util.Objects.checkFromIndexSize;

public class VariableWidthBlockEncoding
        implements BlockEncoding
{
    public static final String NAME = "VARIABLE_WIDTH";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public Class<? extends Block> getBlockClass()
    {
        return VariableWidthBlock.class;
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
    {
        VariableWidthBlock variableWidthBlock = (VariableWidthBlock) block;

        int positionCount = variableWidthBlock.getPositionCount();
        sliceOutput.appendInt(positionCount);

        int arrayBaseOffset = variableWidthBlock.getRawArrayBase();
        @Nullable
        boolean[] isNull = variableWidthBlock.getRawValueIsNull();
        int[] rawOffsets = variableWidthBlock.getRawOffsets();
        checkFromIndexSize(arrayBaseOffset, positionCount + 1, rawOffsets.length);

        int totalLength = 0;
        int firstValueOffset = rawOffsets[arrayBaseOffset];

        int[] adjustedOffsets = new int[positionCount + 1];

        for (int position = 0; position < positionCount; position++) {
            totalLength += rawOffsets[position + arrayBaseOffset + 1] - rawOffsets[position + arrayBaseOffset];
            adjustedOffsets[position] = rawOffsets[position + arrayBaseOffset] - firstValueOffset;
        }
        adjustedOffsets[positionCount] = rawOffsets[positionCount + arrayBaseOffset] - firstValueOffset;

        sliceOutput.writeInts(adjustedOffsets);

        encodeNullsAsBits(sliceOutput, isNull, arrayBaseOffset, positionCount);

        sliceOutput
                .appendInt(totalLength)
                .writeBytes(variableWidthBlock.getRawSlice(), variableWidthBlock.getPositionOffset(0), totalLength);
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();

        int[] offsets = new int[positionCount + 1];
        // Read the lengths array into the end of the offsets array, since nonNullsCount <= positionCount
        sliceInput.readInts(offsets);

        byte[] valueIsNullPacked = retrieveNullBits(sliceInput, positionCount);

        int blockSize = sliceInput.readInt();
        Slice slice = Slices.allocate(blockSize);
        sliceInput.readBytes(slice);

        if (valueIsNullPacked == null) {
            return new VariableWidthBlock(0, positionCount, slice, offsets, null);
        }

        boolean[] valueIsNull = decodeNullBits(valueIsNullPacked, positionCount);

        return new VariableWidthBlock(0, positionCount, slice, offsets, valueIsNull);
    }
}
