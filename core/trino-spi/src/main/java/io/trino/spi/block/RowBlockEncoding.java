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

import static io.trino.spi.block.RowBlock.createRowBlockInternal;

public class RowBlockEncoding
        implements BlockEncoding
{
    public static final String NAME = "ROW";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
    {
        AbstractRowBlock rowBlock = (AbstractRowBlock) block;
        int[] fieldBlockOffsets = rowBlock.getFieldBlockOffsets();

        int numFields = rowBlock.numFields;

        int positionCount = rowBlock.getPositionCount();

        int offsetBase = rowBlock.getOffsetBase();

        int startFieldBlockOffset = fieldBlockOffsets != null ? fieldBlockOffsets[offsetBase] : offsetBase;
        int endFieldBlockOffset = fieldBlockOffsets != null ? fieldBlockOffsets[offsetBase + positionCount] : offsetBase + positionCount;

        sliceOutput.appendInt(numFields);
        sliceOutput.appendInt(positionCount);

        for (int i = 0; i < numFields; i++) {
            blockEncodingSerde.writeBlock(sliceOutput, rowBlock.getRawFieldBlocks()[i].getRegion(startFieldBlockOffset, endFieldBlockOffset - startFieldBlockOffset));
        }

        EncoderUtil.encodeNullsAsBits(sliceOutput, block);

        if ((rowBlock.getRowIsNull() == null) != (fieldBlockOffsets == null)) {
            throw new IllegalArgumentException("When rowIsNull is (non) null then fieldBlockOffsets should be (non) null as well");
        }

        if (fieldBlockOffsets != null) {
            if (startFieldBlockOffset == 0) {
                sliceOutput.writeInts(fieldBlockOffsets, offsetBase, positionCount + 1);
            }
            else {
                int[] newFieldBlockOffsets = new int[positionCount + 1];
                for (int position = 0; position < positionCount + 1; position++) {
                    newFieldBlockOffsets[position] = fieldBlockOffsets[offsetBase + position] - startFieldBlockOffset;
                }
                sliceOutput.writeInts(newFieldBlockOffsets);
            }
        }
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int numFields = sliceInput.readInt();
        int positionCount = sliceInput.readInt();

        Block[] fieldBlocks = new Block[numFields];
        for (int i = 0; i < numFields; i++) {
            fieldBlocks[i] = blockEncodingSerde.readBlock(sliceInput);
        }

        boolean[] rowIsNull = EncoderUtil.decodeNullBits(sliceInput, positionCount).orElse(null);
        int[] fieldBlockOffsets = null;
        if (rowIsNull != null) {
            fieldBlockOffsets = new int[positionCount + 1];
            sliceInput.readInts(fieldBlockOffsets);
        }
        return createRowBlockInternal(0, positionCount, rowIsNull, fieldBlockOffsets, fieldBlocks);
    }
}
