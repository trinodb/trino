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

import static io.trino.spi.block.EncoderUtil.decodeNullBits;
import static io.trino.spi.block.EncoderUtil.encodeNullsAsBits;
import static io.trino.spi.block.EncoderUtil.retrieveNullBits;
import static java.util.Objects.checkFromIndexSize;

public class IntArrayBlockEncoding
        implements BlockEncoding
{
    public static final String NAME = "INT_ARRAY";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public Class<? extends Block> getBlockClass()
    {
        return IntArrayBlock.class;
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
    {
        IntArrayBlock intArrayBlock = (IntArrayBlock) block;
        int positionCount = intArrayBlock.getPositionCount();
        sliceOutput.appendInt(positionCount);

        int rawOffset = intArrayBlock.getRawValuesOffset();
        @Nullable
        boolean[] isNull = intArrayBlock.getRawValueIsNull();
        int[] rawValues = intArrayBlock.getRawValues();
        checkFromIndexSize(rawOffset, positionCount, rawValues.length);

        encodeNullsAsBits(sliceOutput, isNull, rawOffset, positionCount);

        sliceOutput.writeInts(rawValues, rawOffset, positionCount);
    }

    @Override
    public IntArrayBlock readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();

        byte[] valueIsNullPacked = retrieveNullBits(sliceInput, positionCount);
        int[] values = new int[positionCount];
        sliceInput.readInts(values);

        if (valueIsNullPacked == null) {
            return new IntArrayBlock(0, positionCount, null, values);
        }
        boolean[] valueIsNull = decodeNullBits(valueIsNullPacked, positionCount);

        return new IntArrayBlock(0, positionCount, valueIsNull, values);
    }
}
