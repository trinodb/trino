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
    public Class<? extends Block> getBlockClass()
    {
        return Int128ArrayBlock.class;
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
    {
        Int128ArrayBlock int128ArrayBlock = (Int128ArrayBlock) block;
        int positionCount = int128ArrayBlock.getPositionCount();
        sliceOutput.appendInt(positionCount);

        int rawOffset = int128ArrayBlock.getRawOffset();
        @Nullable
        long[] valueIsValid = int128ArrayBlock.getRawValueIsValid();
        long[] rawValues = int128ArrayBlock.getRawValues();

        encodeValidityAsLongs(sliceOutput, valueIsValid, rawOffset, positionCount);

        if (valueIsValid == null) {
            sliceOutput.writeLongs(rawValues, rawOffset * 2, positionCount * 2);
        }
        else {
            compactInt128WithNulls(sliceOutput, rawValues, valueIsValid, rawOffset, positionCount);
        }
    }

    @Override
    public Int128ArrayBlock readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();

        long[] valueIsValid = decodeValidityAsLongs(sliceInput, positionCount);
        if (valueIsValid == null) {
            long[] values = new long[positionCount * 2];
            sliceInput.readLongs(values);
            return new Int128ArrayBlock(0, positionCount, null, values);
        }

        return expandInt128WithNulls(sliceInput, positionCount, valueIsValid);
    }

    static void compactInt128WithNulls(SliceOutput sliceOutput, long[] values, long[] valueIsValid, int offset, int length)
    {
        checkFromIndexSize(offset * 2, length * 2, values.length);
        long[] compacted = new long[length * 2];
        int compactedIndex = 0;
        for (int position = 0; position < length; position++) {
            if (isSet(valueIsValid, offset, position)) {
                int rawValuesIndex = (position + offset) * 2;
                compacted[compactedIndex] = values[rawValuesIndex];
                compacted[compactedIndex + 1] = values[rawValuesIndex + 1];
                compactedIndex += 2;
            }
        }

        sliceOutput.writeInt(compactedIndex / 2);
        sliceOutput.writeLongs(compacted, 0, compactedIndex);
    }

    static Int128ArrayBlock expandInt128WithNulls(SliceInput sliceInput, int positionCount, long[] valueIsValid)
    {
        long[] values = new long[positionCount * 2];
        int nonNullPositionCount = sliceInput.readInt();
        sliceInput.readLongs(values, 0, nonNullPositionCount * 2);
        int compactedIndex = 2 * (nonNullPositionCount - 1);
        for (int position = positionCount - 1; position >= 0 && compactedIndex >= 0; position--) {
            if (isSet(valueIsValid, 0, position)) {
                int valuesIndex = position * 2;
                values[valuesIndex] = values[compactedIndex];
                values[valuesIndex + 1] = values[compactedIndex + 1];
                compactedIndex -= 2;
            }
        }
        return new Int128ArrayBlock(0, positionCount, valueIsValid, values);
    }
}
