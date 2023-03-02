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
import io.trino.spi.block.ColumnarMap;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.operator.unnest.UnnestOperator.ensureCapacity;
import static io.trino.spi.block.ColumnarMap.toColumnarMap;
import static java.util.Objects.requireNonNull;

public class MapUnnester
        implements Unnester
{
    private static final int INSTANCE_SIZE = instanceSize(MapUnnester.class);

    private final UnnestBlockBuilder keyBlockBuilder;
    private final UnnestBlockBuilder valueBlockBuilder;

    int[] mapLengths = new int[0];
    private ColumnarMap columnarMap;

    public MapUnnester()
    {
        keyBlockBuilder = new UnnestBlockBuilder();
        valueBlockBuilder = new UnnestBlockBuilder();
    }

    @Override
    public int getChannelCount()
    {
        return 2;
    }

    @Override
    public void resetInput(Block block)
    {
        requireNonNull(block, "block is null");
        columnarMap = toColumnarMap(block);
        keyBlockBuilder.resetInputBlock(columnarMap.getKeysBlock());
        valueBlockBuilder.resetInputBlock(columnarMap.getValuesBlock());

        int positionCount = block.getPositionCount();
        mapLengths = ensureCapacity(mapLengths, positionCount, false);

        for (int i = 0; i < positionCount; i++) {
            mapLengths[i] = columnarMap.getEntryCount(i);
        }
    }

    @Override
    public int[] getOutputEntriesPerPosition()
    {
        return mapLengths;
    }

    @Override
    public Block[] buildOutputBlocks(int[] outputEntriesPerPosition, int startPosition, int inputBatchSize, int outputRowCount)
    {
        boolean nullRequired = needToInsertNulls(startPosition, inputBatchSize, outputRowCount);

        Block[] outputBlocks = new Block[2];
        if (nullRequired) {
            outputBlocks[0] = keyBlockBuilder.buildWithNulls(outputEntriesPerPosition, startPosition, inputBatchSize, outputRowCount, mapLengths);
            outputBlocks[1] = valueBlockBuilder.buildWithNulls(outputEntriesPerPosition, startPosition, inputBatchSize, outputRowCount, mapLengths);
        }
        else {
            outputBlocks[0] = keyBlockBuilder.buildWithoutNulls(outputRowCount);
            outputBlocks[1] = valueBlockBuilder.buildWithoutNulls(outputRowCount);
        }

        return outputBlocks;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        // The lengths array in key/value block builders is the same object as in the unnester and doesn't need
        // to be counted again.
        return INSTANCE_SIZE + sizeOf(mapLengths);
    }

    private boolean needToInsertNulls(int offset, int length, int outputRowCount)
    {
        int unnestedMapEntries = columnarMap.getOffset(offset + length) - columnarMap.getOffset(offset);
        return unnestedMapEntries < outputRowCount;
    }
}
