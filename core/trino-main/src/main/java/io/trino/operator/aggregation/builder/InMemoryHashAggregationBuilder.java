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
import io.trino.operator.OperatorContext;
import io.trino.operator.TransformWork;
import io.trino.operator.UpdateMemory;
import io.trino.operator.Work;
import io.trino.operator.WorkProcessor;
import io.trino.operator.WorkProcessor.ProcessState;
import io.trino.operator.aggregation.AggregatorFactory;
import io.trino.operator.aggregation.GroupedAggregator;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.JoinCompiler;
import io.trino.sql.planner.plan.AggregationNode.Step;
import it.unimi.dsi.fastutil.ints.AbstractIntIterator;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntIterators;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.operator.GroupByHash.createGroupByHash;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;

public class InMemoryHashAggregationBuilder
        implements HashAggregationBuilder
{
    private final int[] groupByChannels;
    private final GroupByHash groupByHash;
    private final List<Type> groupByOutputTypes;
    private final List<GroupedAggregator> groupedAggregators;
    private final boolean partial;
    private final OptionalLong maxPartialMemory;
    private final UpdateMemory updateMemory;

    private boolean full;

    public InMemoryHashAggregationBuilder(
            List<AggregatorFactory> aggregatorFactories,
            Step step,
            int expectedGroups,
            List<Type> groupByTypes,
            List<Integer> groupByChannels,
            Optional<Integer> hashChannel,
            OperatorContext operatorContext,
            Optional<DataSize> maxPartialMemory,
            JoinCompiler joinCompiler,
            TypeOperators typeOperators,
            UpdateMemory updateMemory)
    {
        this(aggregatorFactories,
                step,
                expectedGroups,
                groupByTypes,
                groupByChannels,
                hashChannel,
                operatorContext,
                maxPartialMemory,
                Optional.empty(),
                joinCompiler,
                typeOperators,
                updateMemory);
    }

    public InMemoryHashAggregationBuilder(
            List<AggregatorFactory> aggregatorFactories,
            Step step,
            int expectedGroups,
            List<Type> groupByTypes,
            List<Integer> groupByChannels,
            Optional<Integer> hashChannel,
            OperatorContext operatorContext,
            Optional<DataSize> maxPartialMemory,
            Optional<Integer> unspillIntermediateChannelOffset,
            JoinCompiler joinCompiler,
            TypeOperators typeOperators,
            UpdateMemory updateMemory)
    {
        if (hashChannel.isPresent()) {
            this.groupByOutputTypes = ImmutableList.<Type>builder()
                    .addAll(groupByTypes)
                    .add(BIGINT)
                    .build();
            this.groupByChannels = new int[groupByChannels.size() + 1];
            for (int i = 0; i < groupByChannels.size(); i++) {
                this.groupByChannels[i] = groupByChannels.get(i);
            }
            this.groupByChannels[groupByChannels.size()] = hashChannel.get();
        }
        else {
            this.groupByOutputTypes = ImmutableList.copyOf(groupByTypes);
            this.groupByChannels = Ints.toArray(groupByChannels);
        }

        this.groupByHash = createGroupByHash(
                operatorContext.getSession(),
                groupByTypes,
                hashChannel.isPresent(),
                expectedGroups,
                joinCompiler,
                typeOperators,
                updateMemory);
        this.partial = step.isOutputPartial();
        this.maxPartialMemory = maxPartialMemory.map(dataSize -> OptionalLong.of(dataSize.toBytes())).orElseGet(OptionalLong::empty);
        this.updateMemory = requireNonNull(updateMemory, "updateMemory is null");

        // wrapper each function with an aggregator
        ImmutableList.Builder<GroupedAggregator> builder = ImmutableList.builder();
        requireNonNull(aggregatorFactories, "aggregatorFactories is null");
        for (int i = 0; i < aggregatorFactories.size(); i++) {
            AggregatorFactory accumulatorFactory = aggregatorFactories.get(i);
            if (unspillIntermediateChannelOffset.isPresent()) {
                builder.add(accumulatorFactory.createUnspillGroupedAggregator(step, unspillIntermediateChannelOffset.get() + i));
            }
            else {
                builder.add(accumulatorFactory.createGroupedAggregator());
            }
        }
        groupedAggregators = builder.build();
    }

    @Override
    public void close() {}

    @Override
    public Work<?> processPage(Page page)
    {
        if (groupedAggregators.isEmpty()) {
            return groupByHash.addPage(page.getColumns(groupByChannels));
        }
        return new TransformWork<>(
                groupByHash.getGroupIds(page.getColumns(groupByChannels)),
                groupByIdBlock -> {
                    int groupCount = groupByHash.getGroupCount();
                    for (GroupedAggregator groupedAggregator : groupedAggregators) {
                        groupedAggregator.processPage(groupCount, groupByIdBlock, page);
                    }
                    // we do not need any output from TransformWork for this case
                    return null;
                });
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
        for (GroupedAggregator groupedAggregator : groupedAggregators) {
            sizeInMemory += groupedAggregator.getEstimatedSize();
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

    public void setSpillOutput()
    {
        for (GroupedAggregator groupedAggregator : groupedAggregators) {
            groupedAggregator.setSpillOutput();
        }
    }

    public int getKeyChannels()
    {
        return groupByChannels.length;
    }

    public long getGroupCount()
    {
        return groupByHash.getGroupCount();
    }

    @Override
    public WorkProcessor<Page> buildResult()
    {
        for (GroupedAggregator groupedAggregator : groupedAggregators) {
            groupedAggregator.prepareFinal();
        }
        return buildResult(consecutiveGroupIds());
    }

    public WorkProcessor<Page> buildHashSortedResult()
    {
        return buildResult(hashSortedGroupIds());
    }

    public List<Type> buildSpillTypes()
    {
        ArrayList<Type> types = new ArrayList<>(groupByOutputTypes);
        for (GroupedAggregator groupedAggregator : groupedAggregators) {
            types.add(groupedAggregator.getSpillType());
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

            while (!pageBuilder.isFull() && groupIds.hasNext()) {
                int groupId = groupIds.nextInt();

                groupByHash.appendValuesTo(groupId, pageBuilder);

                pageBuilder.declarePosition();
                for (int i = 0; i < groupedAggregators.size(); i++) {
                    GroupedAggregator groupedAggregator = groupedAggregators.get(i);
                    BlockBuilder output = pageBuilder.getBlockBuilder(groupByChannels.length + i);
                    groupedAggregator.evaluate(groupId, output);
                }
            }

            return ProcessState.ofResult(pageBuilder.build());
        });
    }

    public List<Type> buildTypes()
    {
        ArrayList<Type> types = new ArrayList<>(groupByOutputTypes);
        for (GroupedAggregator groupedAggregator : groupedAggregators) {
            types.add(groupedAggregator.getType());
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

    public static List<Type> toTypes(List<? extends Type> groupByType, List<AggregatorFactory> factories, Optional<Integer> hashChannel)
    {
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        types.addAll(groupByType);
        if (hashChannel.isPresent()) {
            types.add(BIGINT);
        }
        for (AggregatorFactory factory : factories) {
            types.add(factory.createAggregator().getType());
        }
        return types.build();
    }
}
