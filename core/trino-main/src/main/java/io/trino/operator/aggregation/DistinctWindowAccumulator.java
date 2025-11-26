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
package io.trino.operator.aggregation;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.operator.FlatHashStrategyCompiler;
import io.trino.operator.MarkDistinctHash;
import io.trino.operator.PagesIndex;
import io.trino.operator.UpdateMemory;
import io.trino.operator.Work;
import io.trino.operator.window.PagesWindowIndex;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.function.WindowAccumulator;
import io.trino.spi.function.WindowIndex;
import io.trino.spi.type.Type;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class DistinctWindowAccumulator
        implements WindowAccumulator
{
    private final WindowAccumulator delegate;
    private final List<Type> argumentTypes;
    private final List<Integer> argumentChannels;
    private final MarkDistinctHash hash;
    private final PageBuilder pageBuilder;
    private final PagesIndex.Factory pagesIndexFactory;

    public DistinctWindowAccumulator(
            WindowAccumulator delegate,
            List<Type> argumentTypes,
            List<Integer> argumentChannels,
            FlatHashStrategyCompiler hashStrategyCompiler,
            Session session,
            PagesIndex.Factory pagesIndexFactory)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.argumentTypes = ImmutableList.copyOf(argumentTypes);
        this.argumentChannels = ImmutableList.copyOf(argumentChannels);
        this.hash = new MarkDistinctHash(
                session,
                argumentTypes,
                hashStrategyCompiler,
                UpdateMemory.NOOP);

        this.pageBuilder = new PageBuilder(argumentTypes);
        this.pagesIndexFactory = requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");
    }

    private DistinctWindowAccumulator(
            WindowAccumulator delegate,
            List<Type> argumentTypes,
            List<Integer> argumentChannels,
            MarkDistinctHash hash,
            PagesIndex.Factory pagesIndexFactory)
    {
        this.delegate = delegate;
        this.argumentTypes = argumentTypes;
        this.argumentChannels = argumentChannels;
        this.hash = hash;
        this.pageBuilder = new PageBuilder(argumentTypes);
        this.pagesIndexFactory = pagesIndexFactory;
    }

    @Override
    public long getEstimatedSize()
    {
        return delegate.getEstimatedSize()
                + pageBuilder.getRetainedSizeInBytes()
                + hash.getEstimatedSize();
    }

    @Override
    public WindowAccumulator copy()
    {
        Page page = pageBuilder.build();
        indexCurrentPage(page);
        pageBuilder.reset();
        return new DistinctWindowAccumulator(
                delegate.copy(),
                argumentTypes,
                argumentChannels,
                hash.copy(),
                pagesIndexFactory);
    }

    @Override
    public void addInput(WindowIndex index, int startPosition, int endPosition)
    {
        // index is remapped so just go from 0 to argumentChannels.size()
        for (int position = startPosition; position <= endPosition; position++) {
            if (pageBuilder.isFull()) {
                Page page = pageBuilder.build();
                indexCurrentPage(page);
                pageBuilder.reset();
            }
            for (int channel = 0; channel < argumentChannels.size(); channel++) {
                index.appendTo(channel, position, pageBuilder.getBlockBuilder(channel));
            }
            pageBuilder.declarePosition();
        }
    }

    private void indexCurrentPage(Page page)
    {
        long initialGroupCount = hash.getGroupCount();
        Work<Block> work = hash.markDistinctRows(page);
        checkState(work.process());
        Block distinctMask = work.getResult();

        int positionCount = distinctMask.getPositionCount();
        checkArgument(positionCount == page.getPositionCount(), "Page position count does not match distinct mask position count");

        int distinctPositions = toIntExact(hash.getGroupCount() - initialGroupCount);
        if (distinctPositions == 0) {
            return;
        }
        PagesIndex pagesIndex = pagesIndexFactory.newPagesIndex(argumentTypes, distinctPositions);

        if (distinctMask instanceof RunLengthEncodedBlock) {
            // all positions selected
            checkState(test(distinctMask, 0), "all positions must be distinct");
            pagesIndex.addPage(page);
        }
        else {
            int[] selectedPositions = new int[distinctPositions];
            int selectedIndex = 0;
            for (int position = 0; position < positionCount; position++) {
                if (test(distinctMask, position)) {
                    selectedPositions[selectedIndex++] = position;
                }
            }
            checkState(selectedIndex == selectedPositions.length, "Invalid positions in distinct mask");

            Block[] filteredBlocks = new Block[argumentChannels.size()];
            for (int channel = 0; channel < argumentChannels.size(); channel++) {
                filteredBlocks[channel] = page.getBlock(channel).copyPositions(selectedPositions, 0, selectedPositions.length);
            }
            pagesIndex.addPage(new Page(selectedPositions.length, filteredBlocks));
        }
        int selectedPositionsCount = pagesIndex.getPositionCount();
        checkState(selectedPositionsCount == distinctPositions, "unexpected pagesIndex positions: %s <> %s", selectedPositionsCount, distinctPositions);

        PagesWindowIndex selectedWindowIndex = new PagesWindowIndex(pagesIndex, 0, selectedPositionsCount);
        delegate.addInput(selectedWindowIndex, 0, selectedPositionsCount - 1);
    }

    private static boolean test(Block block, int position)
    {
        if (block.isNull(position)) {
            return false;
        }
        return BOOLEAN.getBoolean(block, position);
    }

    @Override
    public void output(BlockBuilder blockBuilder)
    {
        if (!pageBuilder.isEmpty()) {
            Page page = pageBuilder.build();
            indexCurrentPage(page);
            pageBuilder.reset();
        }
        delegate.output(blockBuilder);
    }
}
