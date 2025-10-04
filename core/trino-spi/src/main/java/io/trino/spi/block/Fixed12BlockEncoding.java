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

        int rawArrayOffset = fixed12Block.getRawOffset();
        @Nullable
        boolean[] isNull = fixed12Block.getRawValueIsNull();
        int[] rawValues = fixed12Block.getRawValues();
        checkFromIndexSize(rawArrayOffset * 3, positionCount * 3, rawValues.length);

        encodeNullsAsBits(sliceOutput, isNull, rawArrayOffset, positionCount);

        sliceOutput.writeInts(rawValues, rawArrayOffset * 3, positionCount * 3);
    }

    @Override
    public Fixed12Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();

        byte[] valueIsNullPacked = retrieveNullBits(sliceInput, positionCount);
        int[] values = new int[positionCount * 3];
        sliceInput.readInts(values);

        if (valueIsNullPacked == null) {
            return new Fixed12Block(0, positionCount, null, values);
        }

        boolean[] valueIsNull = decodeNullBits(valueIsNullPacked, positionCount);

        return new Fixed12Block(0, positionCount, valueIsNull, values);
    }
}
