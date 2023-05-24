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
import io.trino.spi.block.ColumnarRow;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.operator.unnest.UnnestOperator.ensureCapacity;
import static io.trino.spi.block.ColumnarArray.toColumnarArray;
import static io.trino.spi.block.ColumnarRow.toColumnarRow;
import static java.util.Objects.requireNonNull;

public class ArrayOfRowsUnnester
        implements Unnester
{
    private static final int INSTANCE_SIZE = instanceSize(ArrayOfRowsUnnester.class);

    private final int fieldCount;
    private final UnnestBlockBuilder[] blockBuilders;

    private int[] arrayLengths = new int[0];
    private ColumnarArray columnarArray;
    private ColumnarRow columnarRow;

    public ArrayOfRowsUnnester(int fieldCount)
    {
        blockBuilders = createUnnestBlockBuilders(fieldCount);
        this.fieldCount = fieldCount;
    }

    private static UnnestBlockBuilder[] createUnnestBlockBuilders(int fieldCount)
    {
        UnnestBlockBuilder[] builders = new UnnestBlockBuilder[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            builders[i] = new UnnestBlockBuilder();
        }
        return builders;
    }

    @Override
    public int getChannelCount()
    {
        return fieldCount;
    }

    @Override
    public void resetInput(Block block)
    {
        requireNonNull(block, "block is null");
        columnarArray = toColumnarArray(block);
        columnarRow = toColumnarRow(columnarArray.getElementsBlock());

        for (int i = 0; i < fieldCount; i++) {
            blockBuilders[i].resetInputBlock(columnarRow.getField(i), columnarRow.getNullCheckBlock());
        }

        int positionCount = block.getPositionCount();
        arrayLengths = ensureCapacity(arrayLengths, positionCount, false);
        for (int j = 0; j < positionCount; j++) {
            arrayLengths[j] = columnarArray.getLength(j);
        }
    }

    @Override
    public int[] getOutputEntriesPerPosition()
    {
        return arrayLengths;
    }

    @Override
    public Block[] buildOutputBlocks(int[] outputEntriesPerPosition, int startPosition, int batchSize, int outputRowCount)
    {
        boolean nullRequired = needToInsertNulls(startPosition, batchSize, outputRowCount);

        Block[] outputBlocks = new Block[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            if (nullRequired) {
                outputBlocks[i] = blockBuilders[i].buildWithNulls(outputEntriesPerPosition, startPosition, batchSize, outputRowCount, arrayLengths);
            }
            else {
                outputBlocks[i] = blockBuilders[i].buildWithoutNulls(outputRowCount);
            }
        }

        return outputBlocks;
    }

    private boolean needToInsertNulls(int offset, int inputBatchSize, int outputRowCount)
    {
        int start = columnarArray.getOffset(offset);
        int end = columnarArray.getOffset(offset + inputBatchSize);
        int totalLength = end - start;

        if (totalLength < outputRowCount) {
            return true;
        }

        if (columnarRow.mayHaveNull()) {
            for (int i = start; i < end; i++) {
                if (columnarRow.isNull(i)) {
                    return true;
                }
            }
        }

        return false;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        // The lengths array in blockBuilders is the same object as in the unnester and doesn't need to be counted again.
        return INSTANCE_SIZE + sizeOf(arrayLengths);
    }
}
