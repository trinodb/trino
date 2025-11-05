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

        if (isNull == null) {
            sliceOutput.writeInts(rawValues, rawArrayOffset * 3, positionCount * 3);
        }
        else {
            int[] valuesWithoutNull = new int[positionCount * 3];
            int nonNullPositionCount = 0;
            for (int i = 0; i < positionCount; i++) {
                int rawIntOffset = (i + rawArrayOffset) * 3;
                valuesWithoutNull[nonNullPositionCount] = rawValues[rawIntOffset];
                valuesWithoutNull[nonNullPositionCount + 1] = rawValues[rawIntOffset + 1];
                valuesWithoutNull[nonNullPositionCount + 2] = rawValues[rawIntOffset + 2];
                nonNullPositionCount += isNull[i + rawArrayOffset] ? 0 : 3;
            }

            sliceOutput.writeInt(nonNullPositionCount / 3);
            sliceOutput.writeInts(valuesWithoutNull, 0, nonNullPositionCount);
        }
    }

    @Override
    public Fixed12Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);

        int[] values = new int[positionCount * 3];
        if (valueIsNull == null) {
            sliceInput.readInts(values);
        }
        else {
            int nonNullPositionCount = sliceInput.readInt();
            sliceInput.readInts(values, 0, nonNullPositionCount * 3);
            int position = 3 * (nonNullPositionCount - 1);
            for (int i = positionCount - 1; i >= 0 && position >= 0; i--) {
                System.arraycopy(values, position, values, 3 * i, 3);
                if (!valueIsNull[i]) {
                    position -= 3;
                }
            }
        }
        return new Fixed12Block(0, positionCount, valueIsNull, values);
    }
}
