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
import com.google.common.primitives.Ints;
import io.trino.operator.PagesIndex;
import io.trino.operator.PagesIndex.Factory;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.lang.Long.max;
import static java.util.Objects.requireNonNull;

public class OrderedAccumulatorFactory
        implements AccumulatorFactory
{
    private final AccumulatorFactory delegate;
    private final List<Type> sourceTypes;
    private final List<Integer> argumentChannels;
    private final List<Integer> orderByChannels;
    private final List<SortOrder> orderings;

    private final Factory pagesIndexFactory;

    public OrderedAccumulatorFactory(
            AccumulatorFactory delegate,
            List<Type> sourceTypes,
            List<Integer> argumentChannels,
            List<Integer> orderByChannels,
            List<SortOrder> orderings,
            Factory pagesIndexFactory)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.sourceTypes = ImmutableList.copyOf(requireNonNull(sourceTypes, "sourceTypes is null"));
        this.argumentChannels = ImmutableList.copyOf(requireNonNull(argumentChannels, "argumentChannels is null"));
        this.orderByChannels = ImmutableList.copyOf(requireNonNull(orderByChannels, "orderByChannels is null"));
        this.orderings = ImmutableList.copyOf(requireNonNull(orderings, "orderings is null"));
        checkArgument(!orderByChannels.isEmpty(), "Order by channels is empty");
        this.pagesIndexFactory = requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");
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
        GroupedAccumulator accumulator = delegate.createGroupedAccumulator(lambdaProviders);
        return new OrderingGroupedAccumulator(accumulator, sourceTypes, argumentChannels, orderByChannels, orderings, pagesIndexFactory);
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
        private final List<Integer> orderByChannels;
        private final List<SortOrder> orderings;
        private final PagesIndex pagesIndex;

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
            this.orderByChannels = ImmutableList.copyOf(requireNonNull(orderByChannels, "orderByChannels is null"));
            this.orderings = ImmutableList.copyOf(requireNonNull(orderings, "orderings is null"));
            this.pagesIndex = pagesIndexFactory.newPagesIndex(aggregationSourceTypes, 10_000);
        }

        @Override
        public long getEstimatedSize()
        {
            return pagesIndex.getEstimatedSize().toBytes() + accumulator.getEstimatedSize();
        }

        @Override
        public Accumulator copy()
        {
            throw new UnsupportedOperationException("Ordered aggregation function state can not be copied");
        }

        @Override
        public void addInput(Page page, AggregationMask mask)
        {
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
            pagesIndex.sort(orderByChannels, orderings);
            Iterator<Page> pagesIterator = pagesIndex.getSortedPages();
            AggregationMask mask = AggregationMask.createSelectAll(0);
            pagesIterator.forEachRemaining(arguments -> {
                mask.reset(arguments.getPositionCount());
                accumulator.addInput(arguments.getColumns(argumentChannels), mask);
            });
            accumulator.evaluateFinal(blockBuilder);
        }
    }

    private static class OrderingGroupedAccumulator
            implements GroupedAccumulator
    {
        private final GroupedAccumulator accumulator;
        private final int[] argumentChannels;
        private final List<Integer> orderByChannels;
        private final List<SortOrder> orderings;
        private final PagesIndex pagesIndex;
        private long groupCount;

        private OrderingGroupedAccumulator(
                GroupedAccumulator accumulator,
                List<Type> aggregationSourceTypes,
                List<Integer> argumentChannels,
                List<Integer> orderByChannels,
                List<SortOrder> orderings,
                Factory pagesIndexFactory)
        {
            this.accumulator = requireNonNull(accumulator, "accumulator is null");
            this.argumentChannels = Ints.toArray(argumentChannels);
            requireNonNull(aggregationSourceTypes, "aggregationSourceTypes is null");
            this.orderByChannels = ImmutableList.copyOf(requireNonNull(orderByChannels, "orderByChannels is null"));
            this.orderings = ImmutableList.copyOf(requireNonNull(orderings, "orderings is null"));
            List<Type> pageIndexTypes = new ArrayList<>(aggregationSourceTypes);
            // Add group id column
            pageIndexTypes.add(INTEGER);
            this.pagesIndex = pagesIndexFactory.newPagesIndex(pageIndexTypes, 10_000);
            this.groupCount = 0;
        }

        @Override
        public long getEstimatedSize()
        {
            return pagesIndex.getEstimatedSize().toBytes() + accumulator.getEstimatedSize();
        }

        @Override
        public void setGroupCount(long groupCount)
        {
            this.groupCount = max(this.groupCount, groupCount);
            accumulator.setGroupCount(groupCount);
        }

        @Override
        public void addInput(int[] groupIds, Page page, AggregationMask mask)
        {
            if (mask.isSelectNone()) {
                return;
            }

            // Add group id block
            page = page.appendColumn(new IntArrayBlock(page.getPositionCount(), Optional.empty(), groupIds));

            // mask page
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
            accumulator.evaluateFinal(groupId, output);
        }

        @Override
        public void prepareFinal()
        {
            pagesIndex.sort(orderByChannels, orderings);
            Iterator<Page> pagesIterator = pagesIndex.getSortedPages();
            AggregationMask mask = AggregationMask.createSelectAll(0);
            pagesIterator.forEachRemaining(page -> {
                mask.reset(page.getPositionCount());
                accumulator.addInput(
                        extractGroupIds(page),
                        page.getColumns(argumentChannels),
                        mask);
            });
        }

        private static int[] extractGroupIds(Page page)
        {
            // this works because getSortedPages copies data into new blocks
            IntArrayBlock groupIdBlock = (IntArrayBlock) page.getBlock(page.getChannelCount() - 1);
            verify(groupIdBlock.getRawValuesOffset() == 0);
            return groupIdBlock.getRawValues();
        }
    }
}
