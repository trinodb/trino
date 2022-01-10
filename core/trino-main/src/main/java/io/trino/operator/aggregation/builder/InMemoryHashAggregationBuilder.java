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
import io.trino.operator.GroupByHash;
import io.trino.operator.GroupByIdBlock;
import io.trino.operator.HashCollisionsCounter;
import io.trino.operator.OperatorContext;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.operator.GroupByHash.createGroupByHash;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;

public class InMemoryHashAggregationBuilder
        implements HashAggregationBuilder
{
    private final GroupByHash groupByHash;
    private final List<Aggregator> aggregators;
    private final boolean partial;
    private final OptionalLong maxPartialMemory;
    private final UpdateMemory updateMemory;

    private boolean full;
    private int processedPositions;

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
            UpdateMemory updateMemory)
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
                updateMemory);
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
            UpdateMemory updateMemory)
    {
        this.groupByHash = createGroupByHash(
                operatorContext.getSession(),
                groupByTypes,
                Ints.toArray(groupByChannels),
                hashChannel,
                expectedGroups,
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
            builder.add(new Aggregator(accumulatorFactory, step, overwriteIntermediateChannel));
        }
        aggregators = builder.build();
    }

    @Override
    public void close()
    {
        this.groupByHash.close();
    }

    @Override
    public Work<?> processPage(Page page)
    {
        processedPositions += page.getPositionCount();
        if (aggregators.isEmpty()) {
            return groupByHash.addPage(page);
        }
        else {
            return new TransformWork<>(
                    groupByHash.getGroupIds(page),
                    groupByIdBlock -> {
                        for (Aggregator aggregator : aggregators) {
                            aggregator.processPage(groupByIdBlock, page);
                        }
                        // we do not need any output from TransformWork for this case
                        return null;
                    });
        }
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
        System.err.println("buildResult processedPositions: " + processedPositions + ", groups: " + getGroupCount());
        return buildResult(groupByHash.consecutiveGroups());
    }

    public WorkProcessor<Page> buildHashSortedResult()
    {
        return buildResult(groupByHash.hashSortedGroups());
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

    private WorkProcessor<Page> buildResult(GroupByHash.GroupCursor groupIds)
    {
        PageBuilder pageBuilder = new PageBuilder(buildTypes());
        return WorkProcessor.create(() -> {
            if (!groupIds.hasNext()) {
                return ProcessState.finished();
            }

            pageBuilder.reset();

            groupIds.evaluatePage(pageBuilder, aggregators);

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

    public static class Aggregator
    {
        private final GroupedAccumulator aggregation;
        private AggregationNode.Step step;
        private final int intermediateChannel;

        private Aggregator(AccumulatorFactory accumulatorFactory, AggregationNode.Step step, Optional<Integer> overwriteIntermediateChannel)
        {
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

        public void processPage(GroupByIdBlock groupIds, Page page)
        {
            if (step.isInputRaw()) {
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

        public void evaluate(int groupId, BlockBuilder output)
        {
            if (step.isOutputPartial()) {
                aggregation.evaluateIntermediate(groupId, output);
            }
            else {
                aggregation.evaluateFinal(groupId, output);
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
            types.add(new Aggregator(factory, step, Optional.empty()).getType());
        }
        return types.build();
    }
}
