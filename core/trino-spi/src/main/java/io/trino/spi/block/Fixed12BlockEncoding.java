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
import jakarta.annotation.Nullable;

import static io.trino.spi.block.Bitmap.isSet;
import static io.trino.spi.block.EncoderUtil.decodeValidityAsLongs;
import static io.trino.spi.block.EncoderUtil.encodeValidityAsLongs;
import static java.util.Objects.checkFromIndexSize;

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
    public Class<? extends Block> getBlockClass()
    {
        return Fixed12Block.class;
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
    {
        Fixed12Block fixed12Block = (Fixed12Block) block;
        int positionCount = fixed12Block.getPositionCount();
        sliceOutput.appendInt(positionCount);

        int rawOffset = fixed12Block.getRawOffset();
        @Nullable
        long[] valueIsValid = fixed12Block.getRawValueIsValid();
        int[] rawValues = fixed12Block.getRawValues();

        encodeValidityAsLongs(sliceOutput, valueIsValid, rawOffset, positionCount);

        if (valueIsValid == null) {
            sliceOutput.writeInts(rawValues, rawOffset * 3, positionCount * 3);
        }
        else {
            compactFixed12WithNulls(sliceOutput, rawValues, valueIsValid, rawOffset, positionCount);
        }
    }

    @Override
    public Fixed12Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();

        long[] valueIsValid = decodeValidityAsLongs(sliceInput, positionCount);
        if (valueIsValid == null) {
            int[] values = new int[positionCount * 3];
            sliceInput.readInts(values);
            return new Fixed12Block(0, positionCount, null, values);
        }

        return expandFixed12WithNulls(sliceInput, positionCount, valueIsValid);
    }

    static void compactFixed12WithNulls(SliceOutput sliceOutput, int[] values, long[] valueIsValid, int offset, int length)
    {
        checkFromIndexSize(offset * 3, length * 3, values.length);
        int[] compacted = new int[length * 3];
        int compactedIndex = 0;
        for (int position = 0; position < length; position++) {
            if (isSet(valueIsValid, offset, position)) {
                int rawValuesIndex = (position + offset) * 3;
                compacted[compactedIndex] = values[rawValuesIndex];
                compacted[compactedIndex + 1] = values[rawValuesIndex + 1];
                compacted[compactedIndex + 2] = values[rawValuesIndex + 2];
                compactedIndex += 3;
            }
        }

        sliceOutput.writeInt(compactedIndex / 3);
        sliceOutput.writeInts(compacted, 0, compactedIndex);
    }

    static Fixed12Block expandFixed12WithNulls(SliceInput sliceInput, int positionCount, long[] valueIsValid)
    {
        int[] values = new int[positionCount * 3];
        int nonNullPositionCount = sliceInput.readInt();
        sliceInput.readInts(values, 0, nonNullPositionCount * 3);
        int compactedIndex = 3 * (nonNullPositionCount - 1);
        for (int position = positionCount - 1; position >= 0 && compactedIndex >= 0; position--) {
            if (isSet(valueIsValid, 0, position)) {
                int valuesIndex = position * 3;
                values[valuesIndex] = values[compactedIndex];
                values[valuesIndex + 1] = values[compactedIndex + 1];
                values[valuesIndex + 2] = values[compactedIndex + 2];
                compactedIndex -= 3;
            }
        }
        return new Fixed12Block(0, positionCount, valueIsValid, values);
    }
}
