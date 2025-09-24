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

        int rawArrayOffset = int128ArrayBlock.getRawOffset();
        @Nullable
        boolean[] isNull = int128ArrayBlock.getRawValueIsNull();
        long[] rawValues = int128ArrayBlock.getRawValues();
        checkFromIndexSize(rawArrayOffset * 2, positionCount * 2, rawValues.length);

        encodeNullsAsBits(sliceOutput, isNull, rawArrayOffset, positionCount);

        sliceOutput.writeLongs(rawValues, rawArrayOffset * 2, positionCount * 2);
    }

    @Override
    public Int128ArrayBlock readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();

        byte[] valueIsNullPacked = retrieveNullBits(sliceInput, positionCount);
        long[] values = new long[positionCount * 2];

        if (valueIsNullPacked == null) {
            sliceInput.readLongs(values);
            return new Int128ArrayBlock(0, positionCount, null, values);
        }
        boolean[] valueIsNull = decodeNullBits(valueIsNullPacked, positionCount);

        sliceInput.readLongs(values);

        return new Int128ArrayBlock(0, positionCount, valueIsNull, values);
    }
}
