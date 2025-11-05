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
package io.trino.spi;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.PageBuilderStatus;
import io.trino.spi.type.Type;

import java.util.List;

import static io.trino.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static java.lang.String.format;
import static java.util.Objects.checkIndex;

public class PageBuilder
{
    // We choose default initial size to be 8 for PageBuilder and BlockBuilder
    // so the underlying data is larger than the object overhead, and the size is power of 2.
    //
    // This could be any other small number.
    private static final int DEFAULT_INITIAL_EXPECTED_ENTRIES = 8;

    private final BlockBuilder[] blockBuilders;
    private PageBuilderStatus pageBuilderStatus;
    private int declaredPositions;

    /**
     * Create a PageBuilder with given types.
     * <p>
     * A PageBuilder instance created with this constructor has no estimation about bytes per entry,
     * therefore it can resize frequently while appending new rows.
     * <p>
     * This constructor should only be used to get the initial PageBuilder.
     * Once the PageBuilder is full use reset() or newPageBuilderLike() to create a new
     * PageBuilder instance with its size estimated based on previous data.
     */
    public PageBuilder(List<? extends Type> types)
    {
        this(DEFAULT_INITIAL_EXPECTED_ENTRIES, types);
    }

    public PageBuilder(int initialExpectedEntries, List<? extends Type> types)
    {
        this(initialExpectedEntries, DEFAULT_MAX_PAGE_SIZE_IN_BYTES, types);
    }

    public static PageBuilder withMaxPageSize(int maxPageBytes, List<? extends Type> types)
    {
        return new PageBuilder(DEFAULT_INITIAL_EXPECTED_ENTRIES, maxPageBytes, types);
    }

    private PageBuilder(int initialExpectedEntries, int maxPageBytes, List<? extends Type> types)
    {
        pageBuilderStatus = new PageBuilderStatus(maxPageBytes);

        // Stream API should not be used since constructor can be called in performance sensitive sections
        blockBuilders = new BlockBuilder[types.size()];
        for (int i = 0; i < blockBuilders.length; i++) {
            blockBuilders[i] = types.get(i).createBlockBuilder(pageBuilderStatus.createBlockBuilderStatus(), initialExpectedEntries);
        }
    }

    public void reset()
    {
        if (isEmpty()) {
            return;
        }
        pageBuilderStatus = new PageBuilderStatus(pageBuilderStatus.getMaxPageSizeInBytes());

        declaredPositions = 0;

        for (int i = 0; i < blockBuilders.length; i++) {
            blockBuilders[i] = blockBuilders[i].newBlockBuilderLike(pageBuilderStatus.createBlockBuilderStatus());
        }
    }

    /**
     * Rolls back added data to the specified position.
     * Resetting may result in a block without nulls with the may-have-nulls flag set.
     * The PageBuilder status will not be updated to reflect the removed data size.
     *
     * @throws IllegalArgumentException if the position is greater than the current position count
     */
    public void resetTo(int position)
    {
        checkIndex(position, declaredPositions + 1);
        declaredPositions = position;
        for (BlockBuilder blockBuilder : blockBuilders) {
            blockBuilder.resetTo(position);
        }
    }

    public BlockBuilder getBlockBuilder(int channel)
    {
        return blockBuilders[channel];
    }

    public void declarePosition()
    {
        declaredPositions++;
    }

    public void declarePositions(int positions)
    {
        declaredPositions += positions;
    }

    public boolean isFull()
    {
        return declaredPositions == Integer.MAX_VALUE || pageBuilderStatus.isFull();
    }

    public boolean isEmpty()
    {
        return declaredPositions == 0;
    }

    public int getPositionCount()
    {
        return declaredPositions;
    }

    public long getSizeInBytes()
    {
        return pageBuilderStatus.getSizeInBytes();
    }

    public long getRetainedSizeInBytes()
    {
        // We use a foreach loop instead of streams
        // as it has much better performance.
        long retainedSizeInBytes = 0;
        for (BlockBuilder blockBuilder : blockBuilders) {
            retainedSizeInBytes += blockBuilder.getRetainedSizeInBytes();
        }
        return retainedSizeInBytes;
    }

    public Page build()
    {
        if (blockBuilders.length == 0) {
            return new Page(declaredPositions);
        }

        Block[] blocks = new Block[blockBuilders.length];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = blockBuilders[i].build();
            if (blocks[i].getPositionCount() != declaredPositions) {
                throw new IllegalStateException(format("Declared positions (%s) does not match block %s's number of entries (%s)", declaredPositions, i, blocks[i].getPositionCount()));
            }
        }

        return Page.wrapBlocksWithoutCopy(declaredPositions, blocks);
    }
}
