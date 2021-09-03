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
package io.trino.operator.aggregation.builder;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.trino.array.IntBigArray;
import io.trino.operator.GroupByHash;
import io.trino.operator.GroupByIdBlock;
import io.trino.operator.HashCollisionsCounter;
import io.trino.operator.OperatorContext;
import io.trino.operator.StreamingReductionRatio;
import io.trino.operator.TransformWork;
import io.trino.operator.UpdateMemory;
import io.trino.operator.Work;
import io.trino.operator.WorkProcessor;
import io.trino.operator.WorkProcessor.ProcessState;
import io.trino.operator.aggregation.AccumulatorFactory;
import io.trino.operator.aggregation.GroupedAccumulator;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import io.trino.sql.gen.JoinCompiler;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Step;
import io.trino.type.BlockTypeOperators;
import it.unimi.dsi.fastutil.ints.AbstractIntIterator;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntIterators;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.SystemSessionProperties.isDictionaryAggregationEnabled;
import static io.trino.operator.GroupByHash.createGroupByHash;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;

public class InMemoryHashAggregationBuilder
        implements HashAggregationBuilder
{
    private final GroupByHash groupByHash;
    /**
     * each accumulator contains all positions (rows) that are in the
     * same group under grouping key(s).
     */
    private final List<Aggregator> aggregators;
    private final boolean partial;
    private final OptionalLong maxPartialMemory;
    private final UpdateMemory updateMemory;
    private final AtomicReference<StreamingReductionRatio> streamingReductionRatio;

    private boolean full;

    public InMemoryHashAggregationBuilder(
            List<AccumulatorFactory> accumulatorFactories,
            Step step,
            int expectedGroups,
            List<Type> groupByTypes,
            List<Integer> groupByChannels,
            Optional<Integer> hashChannel,
            OperatorContext operatorContext,
            Optional<DataSize> maxPartialMemory,
            JoinCompiler joinCompiler,
            BlockTypeOperators blockTypeOperators,
            UpdateMemory updateMemory,
            AtomicReference<StreamingReductionRatio> streamingReductionRatio)
    {
        this(accumulatorFactories,
                step,
                expectedGroups,
                groupByTypes,
                groupByChannels,
                hashChannel,
                operatorContext,
                maxPartialMemory,
                Optional.empty(),
                joinCompiler,
                blockTypeOperators,
                updateMemory,
                streamingReductionRatio);
    }

    public InMemoryHashAggregationBuilder(
            List<AccumulatorFactory> accumulatorFactories,
            Step step,
            int expectedGroups,
            List<Type> groupByTypes,
            List<Integer> groupByChannels,
            Optional<Integer> hashChannel,
            OperatorContext operatorContext,
            Optional<DataSize> maxPartialMemory,
            Optional<Integer> overwriteIntermediateChannelOffset,
            JoinCompiler joinCompiler,
            BlockTypeOperators blockTypeOperators,
            UpdateMemory updateMemory,
            AtomicReference<StreamingReductionRatio> streamingReductionRatio)
    {
        this.streamingReductionRatio = requireNonNull(streamingReductionRatio, "streamingReductionRatio is null");
        this.groupByHash = createGroupByHash(
                groupByTypes,
                Ints.toArray(groupByChannels),
                hashChannel,
                expectedGroups,
                isDictionaryAggregationEnabled(operatorContext.getSession()),
                joinCompiler,
                blockTypeOperators,
                updateMemory);
        this.partial = step.isOutputPartial();
        this.maxPartialMemory = maxPartialMemory.map(dataSize -> OptionalLong.of(dataSize.toBytes())).orElseGet(OptionalLong::empty);
        this.updateMemory = requireNonNull(updateMemory, "updateMemory is null");

        // wrapper each function with an aggregator
        ImmutableList.Builder<Aggregator> builder = ImmutableList.builder();
        requireNonNull(accumulatorFactories, "accumulatorFactories is null");
        for (int i = 0; i < accumulatorFactories.size(); i++) {
            AccumulatorFactory accumulatorFactory = accumulatorFactories.get(i);
            Optional<Integer> overwriteIntermediateChannel = Optional.empty();
            if (overwriteIntermediateChannelOffset.isPresent()) {
                overwriteIntermediateChannel = Optional.of(overwriteIntermediateChannelOffset.get() + i);
            }
            builder.add(new Aggregator(accumulatorFactory, step, overwriteIntermediateChannel, streamingReductionRatio));
        }
        aggregators = builder.build();
    }

    @Override
    public void close() {}

    @Override
    public Work<?> processPage(Page page)
    {
        if (aggregators.isEmpty()) {
            return groupByHash.addPage(page);
        }
        else {
            // Approach:

            // We want to avoid computing group ids in hash for all positions in page because this is
            // expensive.

            // find most frequent groups by group id, to select which groups to aggregate/pass-through

            // first, sample a few thousand rows from input pages to begin with.
            // Then, for the next few thousand, use streaming average of reduction ratio
            // to decide whether to box raw input as intermediately aggregated data for final aggregation.

            // Continue to monitor reduction ratio as new pages with positions are inputted.

            // When streaming average of reduction ratio falls below threshold, do not apply partial
            // aggregation push-down.

            // We could continue to sample another few thousand rows to update streaming average.

//            if (streamingReductionRatio.get().isSampleSizeTooSmall()) {
            return new TransformWork<>(
                    // TODO: we want to skip this (when calling .process()) by not returning this
                    groupByHash.getGroupIds(page),
                    groupByIdBlock -> {
                        for (Aggregator aggregator : aggregators) {
                            aggregator.processPage(groupByIdBlock, page);
                        }
                        // we do not need any output from TransformWork for this case
                        return null;
                    });
        }
//            }
    }

    @Override
    public void updateMemory()
    {
        updateMemory.update();
    }

    @Override
    public boolean isFull()
    {
        return full;
    }

    @Override
    public void recordHashCollisions(HashCollisionsCounter hashCollisionsCounter)
    {
        hashCollisionsCounter.recordHashCollision(groupByHash.getHashCollisions(), groupByHash.getExpectedHashCollisions());
    }

    public long getHashCollisions()
    {
        return groupByHash.getHashCollisions();
    }

    public double getExpectedHashCollisions()
    {
        return groupByHash.getExpectedHashCollisions();
    }

    @Override
    public ListenableFuture<Void> startMemoryRevoke()
    {
        throw new UnsupportedOperationException("startMemoryRevoke not supported for InMemoryHashAggregationBuilder");
    }

    @Override
    public void finishMemoryRevoke()
    {
        throw new UnsupportedOperationException("finishMemoryRevoke not supported for InMemoryHashAggregationBuilder");
    }

    public long getSizeInMemory()
    {
        long sizeInMemory = groupByHash.getEstimatedSize();
        for (Aggregator aggregator : aggregators) {
            sizeInMemory += aggregator.getEstimatedSize();
        }

        updateIsFull(sizeInMemory);
        return sizeInMemory;
    }

    private void updateIsFull(long sizeInMemory)
    {
        if (!partial || maxPartialMemory.isEmpty()) {
            return;
        }

        full = sizeInMemory > maxPartialMemory.getAsLong();
    }

    /**
     * building hash sorted results requires memory for sorting group IDs.
     * This method returns size of that memory requirement.
     */
    public long getGroupIdsSortingSize()
    {
        return getGroupCount() * Integer.BYTES;
    }

    public void setOutputPartial()
    {
        for (Aggregator aggregator : aggregators) {
            aggregator.setOutputPartial();
        }
    }

    public int getKeyChannels()
    {
        return groupByHash.getTypes().size();
    }

    public long getGroupCount()
    {
        return groupByHash.getGroupCount();
    }

    @Override
    public WorkProcessor<Page> buildResult()
    {
        for (Aggregator aggregator : aggregators) {
            aggregator.prepareFinal();
        }
        return buildResult(consecutiveGroupIds());
    }

    public WorkProcessor<Page> buildHashSortedResult()
    {
        return buildResult(hashSortedGroupIds());
    }

    public List<Type> buildIntermediateTypes()
    {
        ArrayList<Type> types = new ArrayList<>(groupByHash.getTypes());
        for (InMemoryHashAggregationBuilder.Aggregator aggregator : aggregators) {
            types.add(aggregator.getIntermediateType());
        }
        return types;
    }

    @VisibleForTesting
    public int getCapacity()
    {
        return groupByHash.getCapacity();
    }

    private WorkProcessor<Page> buildResult(IntIterator groupIds)
    {
        PageBuilder pageBuilder = new PageBuilder(buildTypes());
        return WorkProcessor.create(() -> {
            if (!groupIds.hasNext()) {
                return ProcessState.finished();
            }

            pageBuilder.reset();

            List<Type> types = groupByHash.getTypes();
            while (!pageBuilder.isFull() && groupIds.hasNext()) {
                int groupId = groupIds.nextInt();

                groupByHash.appendValuesTo(groupId, pageBuilder, 0);

                pageBuilder.declarePosition();
                for (int i = 0; i < aggregators.size(); i++) {
                    Aggregator aggregator = aggregators.get(i);
                    BlockBuilder output = pageBuilder.getBlockBuilder(types.size() + i);
                    aggregator.evaluate(groupId, output);
                }
            }

            return ProcessState.ofResult(pageBuilder.build());
        });
    }

    public List<Type> buildTypes()
    {
        ArrayList<Type> types = new ArrayList<>(groupByHash.getTypes());
        for (Aggregator aggregator : aggregators) {
            types.add(aggregator.getType());
        }
        return types;
    }

    private IntIterator consecutiveGroupIds()
    {
        return IntIterators.fromTo(0, groupByHash.getGroupCount());
    }

    private IntIterator hashSortedGroupIds()
    {
        IntBigArray groupIds = new IntBigArray();
        groupIds.ensureCapacity(groupByHash.getGroupCount());
        for (int i = 0; i < groupByHash.getGroupCount(); i++) {
            groupIds.set(i, i);
        }

        groupIds.sort(0, groupByHash.getGroupCount(), (leftGroupId, rightGroupId) ->
                Long.compare(groupByHash.getRawHash(leftGroupId), groupByHash.getRawHash(rightGroupId)));

        return new AbstractIntIterator()
        {
            private final int totalPositions = groupByHash.getGroupCount();
            private int position;

            @Override
            public boolean hasNext()
            {
                return position < totalPositions;
            }

            @Override
            public int nextInt()
            {
                return groupIds.get(position++);
            }
        };
    }

    private static class Aggregator
    {
        private final GroupedAccumulator aggregation;
        private AggregationNode.Step step;
        private final int intermediateChannel;
        private final AtomicReference<StreamingReductionRatio> streamingReductionRatio;

        private Aggregator(
                AccumulatorFactory accumulatorFactory,
                AggregationNode.Step step,
                Optional<Integer> overwriteIntermediateChannel,
                AtomicReference<StreamingReductionRatio> streamingReductionRatio)
        {
            this.streamingReductionRatio = requireNonNull(streamingReductionRatio, "streamingReductionRatio is null");
            if (step.isInputRaw()) {
                this.intermediateChannel = -1;
                this.aggregation = accumulatorFactory.createGroupedAccumulator();
            }
            else if (overwriteIntermediateChannel.isPresent()) {
                this.intermediateChannel = overwriteIntermediateChannel.get();
                this.aggregation = accumulatorFactory.createGroupedIntermediateAccumulator();
            }
            else {
                checkArgument(accumulatorFactory.getInputChannels().size() == 1, "expected 1 input channel for intermediate aggregation");
                this.intermediateChannel = accumulatorFactory.getInputChannels().get(0);
                this.aggregation = accumulatorFactory.createGroupedIntermediateAccumulator();
            }
            this.step = step;
        }

        public long getEstimatedSize()
        {
            return aggregation.getEstimatedSize();
        }

        public Type getType()
        {
            if (step.isOutputPartial()) {
                return aggregation.getIntermediateType();
            }
            else {
                return aggregation.getFinalType();
            }
        }

        /**
         * @param groupIds groupIds.getGroupCount() is the group count for all input
         * positions from the entire aggregation of the query. Since GroupByIdBlock
         * contains every group id for every position, groupIds.getPositionCount() is
         * equal to page.getPositionCount(). groupIds.getUniqueIdCount() is the total
         * number of unique groups after aggregation for all position in Page.
         * @param page input page that is streamed in from previous operator.
         */
        public void processPage(GroupByIdBlock groupIds, Page page)
        {
            if (step.isInputRaw()) {
                double pageDataReductionRatio = (double) groupIds.getUniqueIdCount() / (double) page.getPositionCount();
                streamingReductionRatio.get().update(pageDataReductionRatio);
                aggregation.addInput(groupIds, page);
            }
            else {
                aggregation.addIntermediate(groupIds, page.getBlock(intermediateChannel));
            }
        }

        public void prepareFinal()
        {
            aggregation.prepareFinal();
        }

        /**
         * Aggregates all column/block values in the same group (with the same value).
         *
         * @param groupId id of group that all values of BlockBuilder are in
         * @param output all values in the same group
         */
        public void evaluate(int groupId, BlockBuilder output)
        {
            if (step.isOutputPartial()) {
                aggregation.evaluateIntermediate(groupId, output);
            }
            else {
                aggregation.evaluateFinal(groupId, output);
            }
        }

        private static final double PARTIAL_AGGREGATION_REDUCTION_THRESHOLD = 0.9;

        private boolean isPartialAggregationInefficient(long pageInputRowCount, long pageOutputRowCount)
        {
            synchronized (this) {
                System.out.println("------------- start pa efficiency sampling -------------");

                System.out.println("Row count in page: " + pageInputRowCount);
                System.out.println("Group count in page: " + pageOutputRowCount);

                System.out.println("------------- end pa efficiency sampling -------------");

                boolean isDataReductionLow = pageOutputRowCount >
                        pageInputRowCount * PARTIAL_AGGREGATION_REDUCTION_THRESHOLD;

                System.out.println("is data reduction low: " + isDataReductionLow);

                return isDataReductionLow;
            }
        }

        public void setOutputPartial()
        {
            step = AggregationNode.Step.partialOutput(step);
        }

        public Type getIntermediateType()
        {
            return aggregation.getIntermediateType();
        }
    }

    public static List<Type> toTypes(List<? extends Type> groupByType, Step step, List<AccumulatorFactory> factories, Optional<Integer> hashChannel)
    {
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        types.addAll(groupByType);
        if (hashChannel.isPresent()) {
            types.add(BIGINT);
        }
        for (AccumulatorFactory factory : factories) {
            types.add(new Aggregator(factory, step, Optional.empty(), new AtomicReference<>(new StreamingReductionRatio())).getType());
        }
        return types.build();
    }
}
