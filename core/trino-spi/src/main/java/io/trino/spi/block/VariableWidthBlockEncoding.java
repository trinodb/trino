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

import java.util.Arrays;

import static io.trino.spi.block.EncoderUtil.decodeNullBits;
import static io.trino.spi.block.EncoderUtil.encodeNullsAsBits;
import static java.lang.String.format;

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
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
    {
        // The down casts here are safe because it is the block itself the provides this encoding implementation.
        AbstractVariableWidthBlock variableWidthBlock = (AbstractVariableWidthBlock) block;

        int positionCount = variableWidthBlock.getPositionCount();
        sliceOutput.appendInt(positionCount);

        // lengths
        int[] lengths = new int[positionCount];
        int totalLength = 0;
        int nonNullsCount = 0;

        for (int position = 0; position < positionCount; position++) {
            int length = variableWidthBlock.getSliceLength(position);
            totalLength += length;
            lengths[nonNullsCount] = length;
            nonNullsCount += variableWidthBlock.isNull(position) ? 0 : 1;
        }

        sliceOutput
                .appendInt(nonNullsCount)
                .writeBytes(Slices.wrappedIntArray(lengths, 0, nonNullsCount));

        encodeNullsAsBits(sliceOutput, variableWidthBlock);

        sliceOutput
                .appendInt(totalLength)
                .writeBytes(variableWidthBlock.getRawSlice(0), variableWidthBlock.getPositionOffset(0), totalLength);
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();
        int nonNullsCount = sliceInput.readInt();

        if (nonNullsCount > positionCount) {
            throw new IllegalArgumentException(format("nonNullsCount must be <= positionCount, found: %s > %s", nonNullsCount, positionCount));
        }

        int[] offsets = new int[positionCount + 1];
        // Read the lengths array into the end of the offsets array, since nonNullsCount <= positionCount
        int lengthIndex = offsets.length - nonNullsCount;
        sliceInput.readBytes(Slices.wrappedIntArray(offsets, lengthIndex, nonNullsCount));

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);
        // Transform lengths back to offsets
        if (valueIsNull == null) {
            if (positionCount != nonNullsCount || lengthIndex != 1) {
                throw new IllegalArgumentException(format("nonNullsCount must equal positionCount, found: %s <> %s", nonNullsCount, positionCount));
            }
            // Simplified loop for no nulls present
            for (int i = 1; i < offsets.length; i++) {
                offsets[i] += offsets[i - 1];
            }
        }
        else {
            computeOffsetsFromLengths(offsets, valueIsNull, lengthIndex);
        }

        int blockSize = sliceInput.readInt();
        Slice slice = Slices.allocate(blockSize);
        sliceInput.readBytes(slice);

        return new VariableWidthBlock(0, positionCount, slice, offsets, valueIsNull);
    }

    private static void computeOffsetsFromLengths(int[] offsets, boolean[] valueIsNull, int lengthIndex)
    {
        if (lengthIndex < 0 || lengthIndex > offsets.length) {
            throw new IllegalArgumentException(format("Invalid lengthIndex %s for offsets %s", lengthIndex, offsets.length));
        }
        int currentOffset = 0;
        for (int i = 1; i < offsets.length; i++) {
            if (lengthIndex == offsets.length) {
                // Populate remaining null elements
                Arrays.fill(offsets, i, offsets.length, currentOffset);
                break;
            }
            boolean isNull = valueIsNull[i - 1];
            // must be accessed unconditionally, otherwise CMOV optimization isn't applied due to
            // ArrayIndexOutOfBoundsException checks
            int length = offsets[lengthIndex];
            lengthIndex += isNull ? 0 : 1;
            currentOffset += isNull ? 0 : length;
            offsets[i] = currentOffset;
        }
        if (lengthIndex != offsets.length) {
            throw new IllegalArgumentException(format("Failed to consume all length entries, found %s <> %s", lengthIndex, offsets.length));
        }
    }
}
