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
import io.airlift.slice.Slices;
import jakarta.annotation.Nullable;

import static io.trino.spi.block.EncoderUtil.decodeNullBits;
import static io.trino.spi.block.EncoderUtil.encodeNullsAsBits;
import static io.trino.spi.block.EncoderUtil.retrieveNullBits;
import static java.util.Objects.checkFromIndexSize;

public class ByteArrayBlockEncoding
        implements BlockEncoding
{
    public static final String NAME = "BYTE_ARRAY";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public Class<? extends Block> getBlockClass()
    {
        return ByteArrayBlock.class;
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
    {
        ByteArrayBlock byteArrayBlock = (ByteArrayBlock) block;
        int positionCount = byteArrayBlock.getPositionCount();
        sliceOutput.appendInt(positionCount);

        int rawOffset = byteArrayBlock.getRawValuesOffset();
        @Nullable
        boolean[] isNull = byteArrayBlock.getRawValueIsNull();
        byte[] rawValues = byteArrayBlock.getRawValues();
        checkFromIndexSize(rawOffset, positionCount, rawValues.length);

        encodeNullsAsBits(sliceOutput, isNull, rawOffset, positionCount);

        sliceOutput.writeBytes(rawValues, rawOffset, positionCount);
    }

    @Override
    public ByteArrayBlock readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();

        byte[] valueIsNullPacked = retrieveNullBits(sliceInput, positionCount);
        byte[] values = new byte[positionCount];
        sliceInput.readBytes(Slices.wrappedBuffer(values));

        if (valueIsNullPacked == null) {
            return new ByteArrayBlock(0, positionCount, null, values);
        }
        boolean[] valueIsNull = decodeNullBits(valueIsNullPacked, positionCount);

        return new ByteArrayBlock(0, positionCount, valueIsNull, values);
    }
}
