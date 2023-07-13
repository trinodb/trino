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
package io.trino.operator.output;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class PositionsAppenderPageBuilder
{
    private static final int DEFAULT_INITIAL_EXPECTED_ENTRIES = 8;
    private final PositionsAppender[] channelAppenders;
    private final int maxPageSizeInBytes;
    private int declaredPositions;

    public static PositionsAppenderPageBuilder withMaxPageSize(int maxPageBytes, List<Type> sourceTypes, PositionsAppenderFactory positionsAppenderFactory)
    {
        return new PositionsAppenderPageBuilder(DEFAULT_INITIAL_EXPECTED_ENTRIES, maxPageBytes, sourceTypes, positionsAppenderFactory);
    }

    private PositionsAppenderPageBuilder(
            int initialExpectedEntries,
            int maxPageSizeInBytes,
            List<? extends Type> types,
            PositionsAppenderFactory positionsAppenderFactory)
    {
        requireNonNull(types, "types is null");
        requireNonNull(positionsAppenderFactory, "positionsAppenderFactory is null");

        this.maxPageSizeInBytes = maxPageSizeInBytes;
        channelAppenders = new PositionsAppender[types.size()];
        for (int i = 0; i < channelAppenders.length; i++) {
            channelAppenders[i] = positionsAppenderFactory.create(types.get(i), initialExpectedEntries, maxPageSizeInBytes);
        }
    }

    public void appendToOutputPartition(Page page, IntArrayList positions)
    {
        declarePositions(positions.size());

        for (int channel = 0; channel < channelAppenders.length; channel++) {
            Block block = page.getBlock(channel);
            channelAppenders[channel].append(positions, block);
        }
    }

    public void appendToOutputPartition(Page page, int position)
    {
        declarePositions(1);

        for (int channel = 0; channel < channelAppenders.length; channel++) {
            Block block = page.getBlock(channel);
            channelAppenders[channel].append(position, block);
        }
    }

    public long getRetainedSizeInBytes()
    {
        // We use a foreach loop instead of streams
        // as it has much better performance.
        long retainedSizeInBytes = 0;
        for (PositionsAppender positionsAppender : channelAppenders) {
            retainedSizeInBytes += positionsAppender.getRetainedSizeInBytes();
        }
        return retainedSizeInBytes;
    }

    public long getSizeInBytes()
    {
        long sizeInBytes = 0;
        for (PositionsAppender positionsAppender : channelAppenders) {
            sizeInBytes += positionsAppender.getSizeInBytes();
        }
        return sizeInBytes;
    }

    public void declarePositions(int positions)
    {
        declaredPositions += positions;
    }

    public boolean isFull()
    {
        return declaredPositions == Integer.MAX_VALUE || getSizeInBytes() >= maxPageSizeInBytes;
    }

    public boolean isEmpty()
    {
        return declaredPositions == 0;
    }

    public Page build()
    {
        Block[] blocks = new Block[channelAppenders.length];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = channelAppenders[i].build();
            checkState(blocks[i].getPositionCount() == declaredPositions, "Declared positions (%s) does not match block %s's number of entries (%s)", declaredPositions, i, blocks[i].getPositionCount());
        }

        Page page = new Page(declaredPositions, blocks);
        reset();
        return page;
    }

    private void reset()
    {
        declaredPositions = 0;
    }
}
