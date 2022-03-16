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
import io.trino.spi.block.PageBuilderStatus;
import io.trino.spi.type.Type;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.PageBuilder.DEFAULT_INITIAL_EXPECTED_ENTRIES;
import static java.util.Objects.requireNonNull;

public class PositionsAppenderPageBuilder
{
    private final PositionsAppender[] positionsAppenders;
    private final List<? extends Type> types;
    private final PositionsAppenderFactory positionsAppenderFactory;
    private PageBuilderStatus pageBuilderStatus;
    private int declaredPositions;

    private PositionsAppenderPageBuilder(
            int initialExpectedEntries,
            int maxPageBytes,
            List<? extends Type> types,
            PositionsAppenderFactory positionsAppenderFactory)
    {
        this.types = requireNonNull(types, "types is null");
        this.positionsAppenderFactory = requireNonNull(positionsAppenderFactory, "positionsAppenderFactory is null");

        pageBuilderStatus = new PageBuilderStatus(maxPageBytes);
        positionsAppenders = new PositionsAppender[types.size()];
        createAppenders(initialExpectedEntries);
    }

    public static PositionsAppenderPageBuilder withMaxPageSize(int maxPageBytes, List<Type> sourceTypes, PositionsAppenderFactory positionsAppenderFactory)
    {
        return new PositionsAppenderPageBuilder(DEFAULT_INITIAL_EXPECTED_ENTRIES, maxPageBytes, sourceTypes, positionsAppenderFactory);
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
        for (PositionsAppender positionsAppender : positionsAppenders) {
            retainedSizeInBytes += positionsAppender.getRetainedSizeInBytes();
        }
        return retainedSizeInBytes;
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

    public PositionsAppender getAppender(int channel)
    {
        return positionsAppenders[channel];
    }

    public Page build()
    {
        if (positionsAppenders.length == 0) {
            return new Page(declaredPositions);
        }

        Block[] blocks = new Block[positionsAppenders.length];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = positionsAppenders[i].build();
            checkState(blocks[i].getPositionCount() == declaredPositions, "Declared positions (%s) does not match block %s's number of entries (%s)", declaredPositions, i, blocks[i].getPositionCount());
        }

        return Page.wrapBlocksWithoutCopy(declaredPositions, blocks);
    }

    public void reset()
    {
        if (isEmpty()) {
            return;
        }
        pageBuilderStatus = new PageBuilderStatus(pageBuilderStatus.getMaxPageSizeInBytes());
        for (int i = 0; i < positionsAppenders.length; i++) {
            positionsAppenders[i] = positionsAppenders[i].newStateLike(pageBuilderStatus.createBlockBuilderStatus());
        }

        declaredPositions = 0;
    }

    private void createAppenders(int expectedPositions)
    {
        for (int i = 0; i < positionsAppenders.length; i++) {
            positionsAppenders[i] = positionsAppenderFactory.create(types.get(i), pageBuilderStatus.createBlockBuilderStatus(), expectedPositions);
        }
    }
}
