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

public class LongArrayBlockEncoding
        implements BlockEncoding
{
    public static final String NAME = "LONG_ARRAY";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public Class<? extends Block> getBlockClass()
    {
        return LongArrayBlock.class;
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
    {
        LongArrayBlock longArrayBlock = (LongArrayBlock) block;
        int positionCount = longArrayBlock.getPositionCount();
        sliceOutput.appendInt(positionCount);

        int rawOffset = longArrayBlock.getRawValuesOffset();
        @Nullable
        boolean[] isNull = longArrayBlock.getRawValueIsNull();
        long[] rawValues = longArrayBlock.getRawValues();
        checkFromIndexSize(rawOffset, positionCount, rawValues.length);

        encodeNullsAsBits(sliceOutput, isNull, rawOffset, positionCount);

        sliceOutput.writeLongs(rawValues, rawOffset, positionCount);
    }

    @Override
    public LongArrayBlock readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();

        byte[] valueIsNullPacked = retrieveNullBits(sliceInput, positionCount);
        long[] values = new long[positionCount];

        if (valueIsNullPacked == null) {
            sliceInput.readLongs(values);
            return new LongArrayBlock(0, positionCount, null, values);
        }
        boolean[] valueIsNull = decodeNullBits(valueIsNullPacked, positionCount);

        sliceInput.readLongs(values);

        return new LongArrayBlock(0, positionCount, valueIsNull, values);
    }
}
