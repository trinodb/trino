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

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.operator.unnest.UnnestOperator.ensureCapacity;
import static io.trino.spi.block.ColumnarArray.toColumnarArray;
import static java.util.Objects.requireNonNull;

public class ArrayUnnester
        implements Unnester
{
    private static final int INSTANCE_SIZE = instanceSize(ArrayUnnester.class);

    private final UnnestBlockBuilder blockBuilder = new UnnestBlockBuilder();
    private int[] arrayLengths = new int[0];
    private ColumnarArray columnarArray;

    @Override
    public int getChannelCount()
    {
        return 1;
    }

    @Override
    public void resetInput(Block block)
    {
        requireNonNull(block, "block is null");

        columnarArray = toColumnarArray(block);
        blockBuilder.resetInputBlock(columnarArray.getElementsBlock());

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
        int unnestedLength = columnarArray.getOffset(startPosition + inputBatchSize) - columnarArray.getOffset(startPosition);
        boolean nullRequired = unnestedLength < outputRowCount;

        Block[] outputBlocks = new Block[1];
        if (nullRequired) {
            outputBlocks[0] = blockBuilder.buildWithNulls(outputEntriesPerPosition, startPosition, inputBatchSize, outputRowCount, arrayLengths);
        }
        else {
            outputBlocks[0] = blockBuilder.buildWithoutNulls(outputRowCount);
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
