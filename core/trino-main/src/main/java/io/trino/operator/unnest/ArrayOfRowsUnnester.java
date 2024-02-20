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
package io.trino.operator.unnest;

import io.trino.spi.block.Block;
import io.trino.spi.block.ColumnarArray;
import io.trino.spi.block.RowBlock;

import java.util.List;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.operator.unnest.UnnestOperator.ensureCapacity;
import static io.trino.spi.block.ColumnarArray.toColumnarArray;
import static java.util.Objects.requireNonNull;

public class ArrayOfRowsUnnester
        implements Unnester
{
    private static final int INSTANCE_SIZE = instanceSize(ArrayOfRowsUnnester.class);

    private final UnnestBlockBuilder[] blockBuilders;
    private int[] arrayLengths = new int[0];
    private ColumnarArray columnarArray;

    public ArrayOfRowsUnnester(int fieldCount)
    {
        blockBuilders = new UnnestBlockBuilder[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            blockBuilders[i] = new UnnestBlockBuilder();
        }
    }

    @Override
    public int getChannelCount()
    {
        return blockBuilders.length;
    }

    @Override
    public void resetInput(Block block)
    {
        requireNonNull(block, "block is null");
        columnarArray = toColumnarArray(block);

        List<Block> fields = RowBlock.getRowFieldsFromBlock(columnarArray.getElementsBlock());
        for (int i = 0; i < blockBuilders.length; i++) {
            blockBuilders[i].resetInputBlock(fields.get(i));
        }

        int positionCount = block.getPositionCount();
        arrayLengths = ensureCapacity(arrayLengths, positionCount, false);
        for (int i = 0; i < positionCount; i++) {
            arrayLengths[i] = columnarArray.getLength(i);
        }
    }

    @Override
    public int[] getOutputEntriesPerPosition()
    {
        return arrayLengths;
    }

    @Override
    public Block[] buildOutputBlocks(int[] outputEntriesPerPosition, int startPosition, int inputBatchSize, int outputRowCount)
    {
        int unnestLength = columnarArray.getOffset(startPosition + inputBatchSize) - columnarArray.getOffset(startPosition);
        boolean nullRequired = unnestLength < outputRowCount;

        Block[] outputBlocks = new Block[blockBuilders.length];
        for (int i = 0; i < blockBuilders.length; i++) {
            if (nullRequired) {
                outputBlocks[i] = blockBuilders[i].buildWithNulls(outputEntriesPerPosition, startPosition, inputBatchSize, outputRowCount, arrayLengths);
            }
            else {
                outputBlocks[i] = blockBuilders[i].buildWithoutNulls(outputRowCount);
            }
        }

        return outputBlocks;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        // The lengths array in blockBuilders is the same object as in the unnester and does not need to be counted again.
        return INSTANCE_SIZE + sizeOf(arrayLengths);
    }
}
