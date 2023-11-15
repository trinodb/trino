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

import com.google.common.annotations.VisibleForTesting;
import io.trino.operator.project.PageProcessor;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class PositionsAppenderPageBuilder
{
    private static final int DEFAULT_INITIAL_EXPECTED_ENTRIES = 8;
    @VisibleForTesting
    static final int MAX_POSITION_COUNT = PageProcessor.MAX_BATCH_SIZE * 4;
    // Maximum page size before being considered full based on current direct appender size and if RLE channels were converted to direct. Currently,
    // dictionary mode appenders still under-report because computing their equivalent size if converted to direct is prohibitively expensive.
    private static final int MAXIMUM_DIRECT_SIZE_MULTIPLIER = 8;

    private final UnnestingPositionsAppender[] channelAppenders;
    private final int maxPageSizeInBytes;
    private final int maxDirectPageSizeInBytes;
    private int declaredPositions;

    public static PositionsAppenderPageBuilder withMaxPageSize(int maxPageBytes, List<Type> sourceTypes, PositionsAppenderFactory positionsAppenderFactory)
    {
        return withMaxPageSize(maxPageBytes, maxPageBytes * MAXIMUM_DIRECT_SIZE_MULTIPLIER, sourceTypes, positionsAppenderFactory);
    }

    @VisibleForTesting
    static PositionsAppenderPageBuilder withMaxPageSize(int maxPageBytes, int maxDirectSizeInBytes, List<Type> sourceTypes, PositionsAppenderFactory positionsAppenderFactory)
    {
        return new PositionsAppenderPageBuilder(DEFAULT_INITIAL_EXPECTED_ENTRIES, maxPageBytes, maxDirectSizeInBytes, sourceTypes, positionsAppenderFactory);
    }

    private PositionsAppenderPageBuilder(
            int initialExpectedEntries,
            int maxPageSizeInBytes,
            int maxDirectPageSizeInBytes,
            List<? extends Type> types,
            PositionsAppenderFactory positionsAppenderFactory)
    {
        requireNonNull(types, "types is null");
        requireNonNull(positionsAppenderFactory, "positionsAppenderFactory is null");
        checkArgument(maxPageSizeInBytes > 0, "maxPageSizeInBytes is negative: %s", maxPageSizeInBytes);
        checkArgument(maxDirectPageSizeInBytes > 0, "maxDirectPageSizeInBytes is negative: %s", maxDirectPageSizeInBytes);
        checkArgument(maxDirectPageSizeInBytes >= maxPageSizeInBytes, "maxDirectPageSizeInBytes (%s) must be >= maxPageSizeInBytes (%s)", maxDirectPageSizeInBytes, maxPageSizeInBytes);

        this.maxPageSizeInBytes = maxPageSizeInBytes;
        this.maxDirectPageSizeInBytes = maxDirectPageSizeInBytes;
        channelAppenders = new UnnestingPositionsAppender[types.size()];
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
        for (UnnestingPositionsAppender positionsAppender : channelAppenders) {
            retainedSizeInBytes += positionsAppender.getRetainedSizeInBytes();
        }
        return retainedSizeInBytes;
    }

    public long getSizeInBytes()
    {
        long sizeInBytes = 0;
        for (UnnestingPositionsAppender positionsAppender : channelAppenders) {
            sizeInBytes += positionsAppender.getSizeInBytes();
        }
        return sizeInBytes;
    }

    private void declarePositions(int positions)
    {
        declaredPositions += positions;
    }

    public boolean isFull()
    {
        if (declaredPositions == 0) {
            return false;
        }
        if (declaredPositions >= MAX_POSITION_COUNT) {
            return true;
        }
        PositionsAppenderSizeAccumulator accumulator = computeAppenderSizes();
        return accumulator.getSizeInBytes() >= maxPageSizeInBytes || accumulator.getDirectSizeInBytes() >= maxDirectPageSizeInBytes;
    }

    @VisibleForTesting
    PositionsAppenderSizeAccumulator computeAppenderSizes()
    {
        PositionsAppenderSizeAccumulator accumulator = new PositionsAppenderSizeAccumulator();
        for (UnnestingPositionsAppender positionsAppender : channelAppenders) {
            positionsAppender.addSizesToAccumulator(accumulator);
        }
        return accumulator;
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
