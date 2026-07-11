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

import static io.trino.spi.block.EncoderUtil.decodeBitmapAsLongs;
import static io.trino.spi.block.EncoderUtil.decodeValidityAsLongs;
import static io.trino.spi.block.EncoderUtil.encodeBitmapAsLongs;
import static io.trino.spi.block.EncoderUtil.encodeValidityAsLongs;

public class BitArrayBlockEncoding
        implements BlockEncoding
{
    public static final String NAME = "BIT_ARRAY";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public Class<? extends Block> getBlockClass()
    {
        return BitArrayBlock.class;
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
    {
        BitArrayBlock bitArrayBlock = (BitArrayBlock) block;
        int positionCount = bitArrayBlock.getPositionCount();
        sliceOutput.appendInt(positionCount);

        int rawOffset = bitArrayBlock.getRawValuesOffset();
        encodeValidityAsLongs(sliceOutput, bitArrayBlock.getRawValueIsValid(), rawOffset, positionCount);
        encodeBitmapAsLongs(sliceOutput, bitArrayBlock.getRawValues(), rawOffset, positionCount);
    }

    @Override
    public BitArrayBlock readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();
        long[] valueIsValid = decodeValidityAsLongs(sliceInput, positionCount);
        long[] values = decodeBitmapAsLongs(sliceInput, positionCount);
        return new BitArrayBlock(0, positionCount, valueIsValid, values);
    }
}
