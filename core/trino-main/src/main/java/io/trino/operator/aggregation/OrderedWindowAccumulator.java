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
import io.trino.operator.PagesIndex;
import io.trino.operator.window.PagesWindowIndex;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.function.WindowAccumulator;
import io.trino.spi.function.WindowIndex;
import io.trino.spi.type.Type;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class OrderedWindowAccumulator
        implements WindowAccumulator
{
    private final PagesIndex.Factory pagesIndexFactory;
    private final PagesIndex pagesIndex;
    private final List<Type> argumentTypes;
    private final List<Integer> argumentChannels;
    private final List<Integer> sortKeysArguments;
    private final List<SortOrder> sortOrders;

    private WindowAccumulator delegate;
    private final WindowAccumulator initialDelegate;

    private PageBuilder pageBuilder;
    private boolean pagesIndexSorted;

    public OrderedWindowAccumulator(
            PagesIndex.Factory pagesIndexFactory,
            WindowAccumulator delegate,
            List<Type> argumentTypes,
            List<Integer> argumentChannels,
            List<Integer> sortKeysArguments,
            List<SortOrder> sortOrders)
    {
        this(pagesIndexFactory, pagesIndexFactory.newPagesIndex(argumentTypes, 10_000), delegate, argumentTypes, argumentChannels, sortKeysArguments, sortOrders);
    }

    private OrderedWindowAccumulator(
            PagesIndex.Factory pagesIndexFactory,
            PagesIndex pagesIndex,
            WindowAccumulator delegate,
            List<Type> argumentTypes,
            List<Integer> argumentChannels,
            List<Integer> sortKeysArguments,
            List<SortOrder> sortOrders)
    {
        this.pagesIndexFactory = requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");
        this.pagesIndex = requireNonNull(pagesIndex, "pagesIndex is null");

        requireNonNull(argumentTypes, "argumentTypes is null");
        requireNonNull(argumentChannels, "argumentChannels is null");
        checkArgument(argumentTypes.size() == argumentChannels.size(), "argumentTypes and argumentChannels must have the same size");
        this.argumentTypes = ImmutableList.copyOf(argumentTypes);
        this.argumentChannels = ImmutableList.copyOf(argumentChannels);
        requireNonNull(sortOrders, "sortOrders is null");
        requireNonNull(sortKeysArguments, "sortChannels is null");
        checkArgument(sortOrders.size() == sortKeysArguments.size(), "sortOrders and sortChannels must have the same size");
        sortKeysArguments.forEach(argument -> {
            checkArgument(
                    argument < argumentChannels.size(),
                    "invalid argument %s referenced; total number of arguments is %s", argument, argumentChannels.size());
        });
        this.sortOrders = ImmutableList.copyOf(sortOrders);
        this.sortKeysArguments = ImmutableList.copyOf(sortKeysArguments);

        this.delegate = requireNonNull(delegate, "delegate is null");
        this.initialDelegate = delegate.copy();

        this.pageBuilder = new PageBuilder(argumentTypes);
    }

    @Override
    public long getEstimatedSize()
    {
        return delegate.getEstimatedSize() + initialDelegate.getEstimatedSize() + pagesIndex.getEstimatedSize().toBytes() + pageBuilder.getRetainedSizeInBytes();
    }

    @Override
    public WindowAccumulator copy()
    {
        PagesIndex pagesIndexCopy = pagesIndexFactory.newPagesIndex(argumentTypes, pagesIndex.getPositionCount());
        pagesIndex.getPages().forEachRemaining(pagesIndexCopy::addPage);
        return new OrderedWindowAccumulator(pagesIndexFactory, pagesIndexCopy, delegate.copy(), argumentTypes, argumentChannels, sortKeysArguments, sortOrders);
    }

    @Override
    public void addInput(WindowIndex index, int startPosition, int endPosition)
    {
        if (pagesIndexSorted) {
            pagesIndexSorted = false;
            // operate on delegate as of start
            // nicer would be to add reset() method to WindowAccumulator but it requires reset method in each AccumulatorState class
            delegate = initialDelegate.copy();
        }
        // index is remapped so just go from 0 to argumentChannels.size()
        for (int position = startPosition; position <= endPosition; position++) {
            if (pageBuilder.isFull()) {
                indexCurrentPage();
            }
            for (int channel = 0; channel < argumentChannels.size(); channel++) {
                ValueBlock value = index.getSingleValueBlock(channel, position).getSingleValueBlock(0);
                pageBuilder.getBlockBuilder(channel).append(value, 0);
            }
            pageBuilder.declarePosition();
        }
    }

    private void indexCurrentPage()
    {
        pagesIndex.addPage(pageBuilder.build());
        pageBuilder.reset();
    }

    @Override
    public void output(BlockBuilder blockBuilder)
    {
        if (!pagesIndexSorted) {
            if (!pageBuilder.isEmpty()) {
                indexCurrentPage();
            }
            int positionCount = pagesIndex.getPositionCount();
            if (positionCount == 0) {
                delegate.output(blockBuilder);
                return;
            }
            pagesIndex.sort(sortKeysArguments, sortOrders);
            WindowIndex sortedWindowIndex = new PagesWindowIndex(pagesIndex, 0, positionCount);
            delegate.addInput(sortedWindowIndex, 0, positionCount - 1);
            pagesIndexSorted = true;
        }
        checkState(pageBuilder.isEmpty());

        delegate.output(blockBuilder);
    }
}
