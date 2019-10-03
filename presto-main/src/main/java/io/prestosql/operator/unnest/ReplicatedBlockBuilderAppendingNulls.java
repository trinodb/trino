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
package io.prestosql.operator.unnest;

import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.DictionaryBlock;
import io.prestosql.spi.block.PageBuilderStatus;
import io.prestosql.spi.type.Type;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkElementIndex;
import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.operator.unnest.UnnestOperatorBlockUtil.calculateNewArraySize;
import static java.util.Objects.requireNonNull;

/**
 * This class manages the details for building replicate channel blocks.
 * It also supports appending nulls.
 * An attempt to avoid copying input data is made. However, in the absence
 * of null in the source, the class changes the strategy to copying input data.
 */
class ReplicatedBlockBuilderAppendingNulls
        implements ReplicatedBlockBuilder
{
    private Block source;
    private final Type type;

    // State for output dictionary block
    private int[] ids;
    private int positionCount;

    // State for output copied block
    private BlockBuilder outputBlockBuilder;

    // Index of source's null element or -1 if there is none
    private int nullElementIndex = -1;

    ReplicatedBlockBuilderAppendingNulls(Type type)
    {
        this.type = requireNonNull(type, "type is null");
    }

    @Override
    public void resetInputBlock(Block block)
    {
        this.source = requireNonNull(block, "block is null");
        nullElementIndex = getNullElementIndex();
        clearCurrentOutput();
    }

    @Override
    public void startNewOutput(PageBuilderStatus pageBuilderStatus, int expectedEntries)
    {
        clearCurrentOutput();
        checkState(source != null, "source is null");
        if (nullElementIndex != -1) {
            this.ids = new int[expectedEntries];
            this.positionCount = 0;
        }
        else {
            requireNonNull(pageBuilderStatus, "pageBuilderStatus is null");
            outputBlockBuilder = type.createBlockBuilder(pageBuilderStatus.createBlockBuilderStatus(), expectedEntries);
        }
    }

    /**
     * Repeat the source element at position {@code index} for {@code count} times in the output,
     * appending nulls at every other position starting from the first
     */
    public void appendRepeatedWithNulls(int index, int count)
    {
        checkState(source != null, "source is null");
        checkElementIndex(index, source.getPositionCount());
        checkArgument(count >= 0, "count should be >= 0");

        if (nullElementIndex != -1) {
            if (positionCount + 2 * count > ids.length) {
                // Grow capacity
                int newSize = Math.max(calculateNewArraySize(ids.length), positionCount + 2 * count);
                ids = Arrays.copyOf(ids, newSize);
            }
            for (int i = positionCount; i < positionCount + 2 * count; i = i + 2) {
                ids[i] = nullElementIndex;
                ids[i + 1] = index;
            }
            positionCount += 2 * count;
        }
        else {
            for (int i = 0; i < count; i++) {
                outputBlockBuilder.appendNull();
                type.appendTo(source, index, outputBlockBuilder);
            }
        }
    }

    public void appendElement(int index)
    {
        checkState(source != null, "source is null");
        checkElementIndex(index, source.getPositionCount());

        if (nullElementIndex != -1) {
            if (positionCount == ids.length) {
                // grow capacity
                int newSize = calculateNewArraySize(ids.length);
                ids = Arrays.copyOf(ids, newSize);
            }
            ids[positionCount] = index;
            positionCount++;
        }
        else {
            type.appendTo(source, index, outputBlockBuilder);
        }
    }

    @Override
    public Block buildOutputAndFlush()
    {
        Block outputBlock;
        if (nullElementIndex != -1) {
            outputBlock = new DictionaryBlock(positionCount, source, ids);
        }
        else {
            outputBlock = outputBlockBuilder.build();
        }
        clearCurrentOutput();
        return outputBlock;
    }

    private void clearCurrentOutput()
    {
        // clear dictionary block state
        ids = new int[0];
        positionCount = 0;

        // clear copied block state
        outputBlockBuilder = null;
    }

    private int getNullElementIndex()
    {
        for (int i = 0; i < source.getPositionCount(); i++) {
            if (source.isNull(i)) {
                return i;
            }
        }
        return -1;
    }
}
