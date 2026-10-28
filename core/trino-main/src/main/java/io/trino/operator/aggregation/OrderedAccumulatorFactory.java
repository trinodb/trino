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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;
import io.trino.operator.PagesIndex;
import io.trino.operator.PagesIndex.Factory;
import io.trino.operator.PagesIndexOrdering;
import io.trino.operator.UpdateMemory;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.Type;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import jakarta.annotation.Nullable;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

public class OrderedAccumulatorFactory
        implements AccumulatorFactory
{
    private static final DataSize MAX_BATCH_SIZE = DataSize.of(16, MEGABYTE);

    private final AccumulatorFactory delegate;
    private final List<Type> sourceTypes;
    private final List<Integer> argumentChannels;
    private final List<Integer> orderByChannels;
    private final List<SortOrder> orderings;

    private final Factory pagesIndexFactory;
    private final long maxBatchSizeInBytes;

    public OrderedAccumulatorFactory(
            AccumulatorFactory delegate,
            List<Type> sourceTypes,
            List<Integer> argumentChannels,
            List<Integer> orderByChannels,
            List<SortOrder> orderings,
            Factory pagesIndexFactory)
    {
        this(delegate, sourceTypes, argumentChannels, orderByChannels, orderings, pagesIndexFactory, MAX_BATCH_SIZE);
    }

    @VisibleForTesting
    public OrderedAccumulatorFactory(
            AccumulatorFactory delegate,
            List<Type> sourceTypes,
            List<Integer> argumentChannels,
            List<Integer> orderByChannels,
            List<SortOrder> orderings,
            Factory pagesIndexFactory,
            DataSize maxBatchSize)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.sourceTypes = ImmutableList.copyOf(requireNonNull(sourceTypes, "sourceTypes is null"));
        this.argumentChannels = ImmutableList.copyOf(requireNonNull(argumentChannels, "argumentChannels is null"));
        this.orderByChannels = ImmutableList.copyOf(requireNonNull(orderByChannels, "orderByChannels is null"));
        this.orderings = ImmutableList.copyOf(requireNonNull(orderings, "orderings is null"));
        checkArgument(!orderByChannels.isEmpty(), "Order by channels is empty");
        this.pagesIndexFactory = requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");
        this.maxBatchSizeInBytes = maxBatchSize.toBytes();
    }

    @Override
    public List<Class<?>> getLambdaInterfaces()
    {
        return delegate.getLambdaInterfaces();
    }

    @Override
    public Accumulator createAccumulator(List<Supplier<Object>> lambdaProviders)
    {
        Accumulator accumulator = delegate.createAccumulator(lambdaProviders);
        return new OrderedAccumulator(accumulator, sourceTypes, argumentChannels, orderByChannels, orderings, pagesIndexFactory);
    }

    @Override
    public Accumulator createIntermediateAccumulator(List<Supplier<Object>> lambdaProviders)
    {
        return delegate.createIntermediateAccumulator(lambdaProviders);
    }

    @Override
    public GroupedAccumulator createGroupedAccumulator(List<Supplier<Object>> lambdaProviders)
    {
        return new OrderingGroupedAccumulator(
                () -> delegate.createGroupedAccumulator(lambdaProviders),
                sourceTypes,
                argumentChannels,
                orderByChannels,
                orderings,
                pagesIndexFactory,
                maxBatchSizeInBytes);
    }

    @Override
    public GroupedAccumulator createGroupedIntermediateAccumulator(List<Supplier<Object>> lambdaProviders)
    {
        return delegate.createGroupedIntermediateAccumulator(lambdaProviders);
    }

    @Override
    public AggregationMaskBuilder createAggregationMaskBuilder()
    {
        return delegate.createAggregationMaskBuilder();
    }

    private static class OrderedAccumulator
            implements Accumulator
    {
        private final Accumulator accumulator;
        private final int[] argumentChannels;
        private final PagesIndexOrdering pagesIndexOrdering;
        @Nullable
        private PagesIndex pagesIndex; // null after prepareFinal() completes
        @Nullable
        private Iterator<Page> sortedPages; // set while prepareFinal() replays pagesIndex

        private OrderedAccumulator(
                Accumulator accumulator,
                List<Type> aggregationSourceTypes,
                List<Integer> argumentChannels,
                List<Integer> orderByChannels,
                List<SortOrder> orderings,
                Factory pagesIndexFactory)
        {
            this.accumulator = requireNonNull(accumulator, "accumulator is null");
            this.argumentChannels = Ints.toArray(argumentChannels);
            this.pagesIndex = pagesIndexFactory.newPagesIndex(aggregationSourceTypes, 10_000);
            requireNonNull(orderByChannels, "orderByChannels is null");
            requireNonNull(orderings, "orderings is null");
            this.pagesIndexOrdering = pagesIndex.createPagesIndexComparator(orderByChannels, orderings);
        }

        @Override
        public long getEstimatedSize()
        {
            long estimatedSize = accumulator.getEstimatedSize();
            if (pagesIndex != null) {
                estimatedSize += pagesIndex.getEstimatedSize().toBytes();
            }
            return estimatedSize;
        }

        @Override
        public Accumulator copy()
        {
            throw new UnsupportedOperationException("Ordered aggregation function state can not be copied");
        }

        @Override
        public void addInput(Page page, AggregationMask mask)
        {
            checkState(pagesIndex != null, "prepareFinal() already called");
            pagesIndex.addPage(mask.filterPage(page));
        }

        @Override
        public void addIntermediate(Block block)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void evaluateIntermediate(BlockBuilder blockBuilder)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void evaluateFinal(BlockBuilder blockBuilder)
        {
            accumulator.evaluateFinal(blockBuilder);
        }

        @Override
        public boolean prepareFinal(UpdateMemory updateMemory)
        {
            if (pagesIndex != null) {
                if (sortedPages == null) {
                    pagesIndex.sort(pagesIndexOrdering);
                    sortedPages = pagesIndex.getSortedPages();
                }
                AggregationMask mask = AggregationMask.createSelectAll(0);
                while (sortedPages.hasNext()) {
                    Page arguments = sortedPages.next();
                    mask.reset(arguments.getPositionCount());
                    accumulator.addInput(arguments.getColumns(argumentChannels), mask);
                    // the last page is reported after pagesIndex is released
                    if (sortedPages.hasNext() && !updateMemory.update()) {
                        // resume from the next sorted page on the next call
                        return false;
                    }
                }
                // release pagesIndex memory after transferring its contents into the accumulator
                pagesIndex = null;
                sortedPages = null;
                if (!updateMemory.update()) {
                    return false;
                }
            }
            return accumulator.prepareFinal(updateMemory);
        }
    }

    // Replays the sorted rows into a fresh delegate for each batch of consecutive groups, so the state of only one batch is live
    private static class OrderingGroupedAccumulator
            implements GroupedAccumulator
    {
        private final Supplier<GroupedAccumulator> accumulatorFactory;
        private final int[] argumentChannels;
        private final long maxBatchSizeInBytes;
        private final PagesIndexOrdering pagesIndexOrdering;
        @Nullable
        private PagesIndex pagesIndex; // null after all sorted rows are replayed
        @Nullable
        private IntArrayList rowGroupIds = new IntArrayList(); // group id of each row in pagesIndex, null after prepareFinal()
        @Nullable
        private int[] groupStarts; // position of the first sorted row of each group, set by prepareFinal()
        @Nullable
        private UpdateMemory updateMemory; // set by prepareFinal()
        @Nullable
        private GroupedAccumulator batch; // state of the groups in [batchStart, batchEnd)
        private int batchStart;
        private int batchEnd;
        private int groupCount;

        private OrderingGroupedAccumulator(
                Supplier<GroupedAccumulator> accumulatorFactory,
                List<Type> aggregationSourceTypes,
                List<Integer> argumentChannels,
                List<Integer> orderByChannels,
                List<SortOrder> orderings,
                Factory pagesIndexFactory,
                long maxBatchSizeInBytes)
        {
            this.accumulatorFactory = requireNonNull(accumulatorFactory, "accumulatorFactory is null");
            this.argumentChannels = Ints.toArray(argumentChannels);
            this.maxBatchSizeInBytes = maxBatchSizeInBytes;
            this.pagesIndex = pagesIndexFactory.newPagesIndex(aggregationSourceTypes, 10_000);
            requireNonNull(orderByChannels, "orderByChannels is null");
            requireNonNull(orderings, "orderings is null");
            this.pagesIndexOrdering = pagesIndex.createPagesIndexComparator(orderByChannels, orderings);
        }

        @Override
        public long getEstimatedSize()
        {
            long estimatedSize = 0;
            if (pagesIndex != null) {
                estimatedSize += pagesIndex.getEstimatedSize().toBytes();
            }
            if (rowGroupIds != null) {
                estimatedSize += sizeOf(rowGroupIds.elements());
            }
            if (groupStarts != null) {
                estimatedSize += sizeOf(groupStarts);
            }
            if (batch != null) {
                estimatedSize += batch.getEstimatedSize();
            }
            return estimatedSize;
        }

        @Override
        public void setGroupCount(int groupCount)
        {
            this.groupCount = max(this.groupCount, groupCount);
        }

        @Override
        public void addInput(int[] groupIds, Page page, AggregationMask mask)
        {
            checkState(updateMemory == null, "prepareFinal() already called");
            if (mask.isSelectNone()) {
                return;
            }
            if (mask.isSelectAll()) {
                rowGroupIds.addElements(rowGroupIds.size(), groupIds, 0, page.getPositionCount());
            }
            else {
                int[] selectedPositions = mask.getSelectedPositions();
                for (int index = 0; index < mask.getSelectedPositionCount(); index++) {
                    rowGroupIds.add(groupIds[selectedPositions[index]]);
                }
            }
            pagesIndex.addPage(mask.filterPage(page));
        }

        @Override
        public void addIntermediate(int[] groupIds, Block block)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void evaluateIntermediate(int groupId, BlockBuilder output)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void evaluateFinal(int groupId, BlockBuilder output)
        {
            checkState(updateMemory != null, "prepareFinal() not called");
            checkArgument(groupId < groupCount, "groupId %s is not below group count %s", groupId, groupCount);
            checkArgument(groupId >= batchStart, "groupId %s precedes the current batch starting at %s", groupId, batchStart);
            while (groupId >= batchEnd) {
                buildNextBatch();
            }
            batch.evaluateFinal(groupId - batchStart, output);
        }

        @Override
        public void prepareFinal(UpdateMemory updateMemory)
        {
            checkState(this.updateMemory == null, "prepareFinal() already called");
            this.updateMemory = requireNonNull(updateMemory, "updateMemory is null");
            int positionCount = pagesIndex.getPositionCount();
            int[] groupIds = rowGroupIds.elements();
            groupStarts = new int[groupCount + 1];
            for (int position = 0; position < positionCount; position++) {
                groupStarts[groupIds[position] + 1]++;
            }
            for (int groupId = 0; groupId < groupCount; groupId++) {
                groupStarts[groupId + 1] += groupStarts[groupId];
            }
            // move each row into the position range of its group in place
            int[] nextPositions = Arrays.copyOf(groupStarts, groupCount);
            for (int groupId = 0; groupId < groupCount; groupId++) {
                int groupEnd = groupStarts[groupId + 1];
                int position = nextPositions[groupId];
                while (position < groupEnd) {
                    int rowGroupId = groupIds[position];
                    if (rowGroupId == groupId) {
                        position++;
                    }
                    else {
                        int target = nextPositions[rowGroupId]++;
                        pagesIndex.swap(position, target);
                        groupIds[position] = groupIds[target];
                        groupIds[target] = rowGroupId;
                    }
                }
            }
            rowGroupIds = null;
            for (int groupId = 0; groupId < groupCount; groupId++) {
                if (groupStarts[groupId + 1] - groupStarts[groupId] > 1) {
                    pagesIndex.sort(pagesIndexOrdering, groupStarts[groupId], groupStarts[groupId + 1]);
                }
            }
        }

        private void buildNextBatch()
        {
            batchStart = batchEnd;
            batchEnd = groupCount;
            batch = accumulatorFactory.get();
            int position = groupStarts[batchStart];
            int groupId = batchStart;
            boolean full = false;
            AggregationMask mask = AggregationMask.createSelectAll(0);
            Iterator<Page> sortedPages = pagesIndex.getSortedPages(position, groupStarts[groupCount]);
            while (sortedPages.hasNext()) {
                Page page = sortedPages.next();
                int[] groupIds = new int[page.getPositionCount()];
                for (int index = 0; index < groupIds.length; index++) {
                    while (groupStarts[groupId + 1] <= position) {
                        groupId++;
                    }
                    groupIds[index] = groupId - batchStart;
                    position++;
                }
                batch.setGroupCount(groupId - batchStart + 1);
                mask.reset(groupIds.length);
                batch.addInput(groupIds, page.getColumns(argumentChannels), mask);
                if (!full && batch.getEstimatedSize() >= maxBatchSizeInBytes) {
                    // the batch ends after the last group added, whose rows may continue after this page
                    full = true;
                    batchEnd = groupId + 1;
                    sortedPages = pagesIndex.getSortedPages(position, groupStarts[batchEnd]);
                }
                if (full) {
                    // result ignored, evaluateFinal cannot yield
                    updateMemory.update();
                }
            }
            if (batchEnd == groupCount) {
                // release pagesIndex memory after replaying all of its rows
                pagesIndex = null;
                groupStarts = null;
            }
            batch.setGroupCount(batchEnd - batchStart);
            batch.prepareFinal(updateMemory);
        }
    }
}
