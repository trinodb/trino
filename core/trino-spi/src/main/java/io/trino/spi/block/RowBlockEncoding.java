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

import java.util.Optional;

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
        RowBlock rowBlock = (RowBlock) block;

        sliceOutput.appendInt(rowBlock.getPositionCount());

        Block[] rawFieldBlocks = rowBlock.getRawFieldBlocks();
        sliceOutput.appendInt(rawFieldBlocks.length);
        for (Block rawFieldBlock : rawFieldBlocks) {
            blockEncodingSerde.writeBlock(sliceOutput, rawFieldBlock);
        }

        EncoderUtil.encodeNullsAsBits(sliceOutput, block);
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();

        int numFields = sliceInput.readInt();
        Block[] fieldBlocks = new Block[numFields];
        for (int i = 0; i < numFields; i++) {
            fieldBlocks[i] = blockEncodingSerde.readBlock(sliceInput);
        }

        Optional<boolean[]> rowIsNull = EncoderUtil.decodeNullBits(sliceInput, positionCount);
        return RowBlock.fromNotNullSuppressedFieldBlocks(positionCount, rowIsNull, fieldBlocks);
    }
}
