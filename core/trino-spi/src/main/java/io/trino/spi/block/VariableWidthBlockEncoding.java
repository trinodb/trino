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
import static java.lang.String.format;
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
        encodeNullsAsBits(sliceOutput, isNull, arrayBaseOffset, positionCount);

        int[] rawOffsets = variableWidthBlock.getRawOffsets();
        writeOffsetsWithNullsCompacted(sliceOutput, rawOffsets, isNull, arrayBaseOffset, positionCount);

        int startingOffset = rawOffsets[arrayBaseOffset];
        int totalLength = rawOffsets[positionCount + arrayBaseOffset] - startingOffset;

        sliceOutput
                .appendInt(totalLength)
                .writeBytes(variableWidthBlock.getRawSlice(), startingOffset, totalLength);
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();
        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);

        int[] offsets = readOffsetsWithNullsCompacted(sliceInput, valueIsNull, positionCount);

        int blockSize = sliceInput.readInt();
        Slice slice = Slices.allocate(blockSize);
        sliceInput.readBytes(slice);

        return new VariableWidthBlock(0, positionCount, slice, offsets, valueIsNull);
    }

    private static void writeOffsetsWithNullsCompacted(SliceOutput sliceOutput, int[] rawOffsets, @Nullable boolean[] valueIsNull, int baseOffset, int positionCount)
    {
        checkFromIndexSize(baseOffset, positionCount + 1, rawOffsets.length);

        int startingOffset = rawOffsets[baseOffset];
        if (valueIsNull == null && startingOffset == 0) {
            // No translation of offsets required, write the range of raw offsets directly to the output
            sliceOutput
                    .appendInt(positionCount)
                    .writeInts(rawOffsets, baseOffset + 1, positionCount);
        }
        else if (valueIsNull == null) {
            // Subtract starting offset from each offset value to translate them to start from zero, no null suppression required
            int[] nonNullEndingOffsets = new int[positionCount];
            for (int i = 0; i < nonNullEndingOffsets.length; i++) {
                nonNullEndingOffsets[i] = rawOffsets[i + baseOffset + 1] - startingOffset;
            }
            sliceOutput
                    .appendInt(nonNullEndingOffsets.length)
                    .writeInts(nonNullEndingOffsets, 0, nonNullEndingOffsets.length);
        }
        else {
            // Translate offsets and suppress null values from the output
            int[] nonNullEndingOffsets = new int[positionCount];
            int nonNullEndingOffsetCount = 0;
            for (int i = 0; i < positionCount; i++) {
                nonNullEndingOffsets[nonNullEndingOffsetCount] = rawOffsets[i + baseOffset + 1] - startingOffset;
                nonNullEndingOffsetCount += valueIsNull[i + baseOffset] ? 0 : 1;
            }
            sliceOutput
                    .appendInt(nonNullEndingOffsetCount)
                    .writeInts(nonNullEndingOffsets, 0, nonNullEndingOffsetCount);
        }
    }

    private static int[] readOffsetsWithNullsCompacted(SliceInput sliceInput, @Nullable boolean[] valueIsNull, int positionCount)
    {
        int nonNullEndingOffsets = sliceInput.readInt();
        if (nonNullEndingOffsets > positionCount) {
            throw new IllegalArgumentException(format("nonNullEndingOffsets must be <= positionCount, found: %s > %s", nonNullEndingOffsets, positionCount));
        }
        // Offsets are read into the end of the array, expansion will pull values down into the lower range until null positions are expanded in place
        int[] offsets = new int[positionCount + 1];
        int compactIndex = offsets.length - nonNullEndingOffsets;
        sliceInput.readInts(offsets, compactIndex, nonNullEndingOffsets);
        if (valueIsNull == null) {
            if (positionCount != nonNullEndingOffsets) {
                throw new IllegalArgumentException(format("nonNullEndingOffsets must match positionCount, found %s <> %s", nonNullEndingOffsets, positionCount));
            }
            return offsets;
        }
        if (valueIsNull.length != positionCount) {
            throw new IllegalArgumentException(format("valueIsNull length must match positionCount, found %s <> %s", valueIsNull.length, positionCount));
        }
        // Shift the offsets from the end of the offsets array downwards, repeating the previous offset when nulls are encountered
        // until no more nulls are present
        int position = 0;
        int copyFrom = compactIndex - 1;
        for (; position < copyFrom; position++) {
            offsets[position] = offsets[copyFrom];
            copyFrom += valueIsNull[position] ? 0 : 1;
        }
        for (; position < positionCount; position++) {
            if (valueIsNull[position]) {
                throw new IllegalArgumentException("Invalid valueIsNull mask for offsets received");
            }
        }
        return offsets;
    }
}
