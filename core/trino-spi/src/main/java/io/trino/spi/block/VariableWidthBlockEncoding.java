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

        sliceOutput.writeBytes(variableWidthBlock.getRawSlice(), startingOffset, totalLength);
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();
        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);

        int[] offsets = readOffsetsWithNullsCompacted(sliceInput, valueIsNull, positionCount);

        int sliceSize = offsets[offsets.length - 1];
        Slice slice = Slices.allocate(sliceSize);
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
        else {
            int[] nonNullOffsets;
            int nonNullOffsetsCount;
            if (valueIsNull == null) {
                // Subtract starting offset from each ending offset to translate them to start from zero, no null suppression required
                nonNullOffsets = new int[positionCount];
                for (int i = 0; i < nonNullOffsets.length; i++) {
                    nonNullOffsets[i] = rawOffsets[i + baseOffset + 1] - startingOffset;
                }
                nonNullOffsetsCount = nonNullOffsets.length;
            }
            else {
                // Translate ending offsets and suppress null values from the output
                nonNullOffsets = new int[positionCount];
                nonNullOffsetsCount = 0;
                for (int i = 0; i < positionCount; i++) {
                    nonNullOffsets[nonNullOffsetsCount] = rawOffsets[i + baseOffset + 1] - startingOffset;
                    nonNullOffsetsCount += valueIsNull[i + baseOffset] ? 0 : 1;
                }
            }
            sliceOutput
                    .appendInt(nonNullOffsetsCount)
                    .writeInts(nonNullOffsets, 0, nonNullOffsetsCount);
        }
    }

    private static int[] readOffsetsWithNullsCompacted(SliceInput sliceInput, @Nullable boolean[] valueIsNull, int positionCount)
    {
        if (valueIsNull != null && valueIsNull.length != positionCount) {
            throw new IllegalArgumentException(format("valueIsNull length must match positionCount, found %s <> %s", valueIsNull.length, positionCount));
        }
        int nonNullOffsetCount = sliceInput.readInt();
        if (nonNullOffsetCount > positionCount) {
            throw new IllegalArgumentException(format("nonNullOffsetCount must be <= positionCount, found: %s > %s", nonNullOffsetCount, positionCount));
        }
        // Offsets are read into the end of the array, expansion will pull values down into the lower range until null positions are expanded in place
        int[] offsets = new int[positionCount + 1];
        int compactIndex = offsets.length - nonNullOffsetCount;
        sliceInput.readInts(offsets, compactIndex, nonNullOffsetCount);
        if (valueIsNull == null || compactIndex == 1) {
            if (positionCount != nonNullOffsetCount) {
                throw new IllegalArgumentException(format("nonNullOffsetCount must match positionCount, found %s <> %s", nonNullOffsetCount, positionCount));
            }
            return offsets;
        }
        // Shift the offsets from the end of the offsets array downwards, repeating the previous offset when nulls are encountered
        // until no more nulls are present
        int readFrom = compactIndex - 1;
        for (int position = 0; position < readFrom; position++) {
            offsets[position] = offsets[readFrom];
            readFrom += valueIsNull[position] ? 0 : 1;
        }
        return offsets;
    }
}
