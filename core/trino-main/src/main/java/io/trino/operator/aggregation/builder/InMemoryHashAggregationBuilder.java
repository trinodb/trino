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
import io.trino.operator.AggregationMetrics;
import io.trino.operator.FlatHashStrategyCompiler;
import io.trino.operator.GroupByHash;
import io.trino.operator.MeasuredGroupByHashWork;
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
    private final AggregationMetrics aggregationMetrics;

    private boolean full;

    public InMemoryHashAggregationBuilder(
            List<AggregatorFactory> aggregatorFactories,
            Step step,
            int expectedGroups,
            List<Type> groupByTypes,
            List<Integer> groupByChannels,
            boolean spillable,
            OperatorContext operatorContext,
            Optional<DataSize> maxPartialMemory,
            FlatHashStrategyCompiler hashStrategyCompiler,
            UpdateMemory updateMemory,
            AggregationMetrics aggregationMetrics)
    {
        this(aggregatorFactories,
                step,
                expectedGroups,
                groupByTypes,
                groupByChannels,
                spillable,
                operatorContext,
                maxPartialMemory,
                Optional.empty(),
                hashStrategyCompiler,
                updateMemory,
                aggregationMetrics);
    }

    public InMemoryHashAggregationBuilder(
            List<AggregatorFactory> aggregatorFactories,
            Step step,
            int expectedGroups,
            List<Type> groupByTypes,
            List<Integer> groupByChannels,
            boolean spillable,
            OperatorContext operatorContext,
            Optional<DataSize> maxPartialMemory,
            Optional<Integer> unspillIntermediateChannelOffset,
            FlatHashStrategyCompiler hashStrategyCompiler,
            UpdateMemory updateMemory,
            AggregationMetrics aggregationMetrics)
    {
        this.groupByOutputTypes = ImmutableList.copyOf(groupByTypes);
        this.groupByChannels = Ints.toArray(groupByChannels);

        this.groupByHash = createGroupByHash(
                operatorContext.getSession(),
                groupByTypes,
                spillable,
                expectedGroups,
                hashStrategyCompiler,
                updateMemory);
        this.partial = step.isOutputPartial();
        this.maxPartialMemory = maxPartialMemory.map(dataSize -> OptionalLong.of(dataSize.toBytes())).orElseGet(OptionalLong::empty);
        this.updateMemory = requireNonNull(updateMemory, "updateMemory is null");

        // wrapper each function with an aggregator
        requireNonNull(aggregatorFactories, "aggregatorFactories is null");
        ImmutableList.Builder<GroupedAggregator> builder = ImmutableList.builderWithExpectedSize(aggregatorFactories.size());
        for (int i = 0; i < aggregatorFactories.size(); i++) {
            AggregatorFactory accumulatorFactory = aggregatorFactories.get(i);
            if (unspillIntermediateChannelOffset.isPresent()) {
                builder.add(accumulatorFactory.createUnspillGroupedAggregator(step, unspillIntermediateChannelOffset.get() + i, aggregationMetrics));
            }
            else {
                builder.add(accumulatorFactory.createGroupedAggregator(aggregationMetrics));
            }
        }
        groupedAggregators = builder.build();
        this.aggregationMetrics = requireNonNull(aggregationMetrics, "aggregationMetrics is null");
    }

    @Override
    public void close() {}

    @Override
    public Work<?> processPage(Page page)
    {
        if (groupedAggregators.isEmpty()) {
            return new MeasuredGroupByHashWork<>(groupByHash.addPage(page.getColumns(groupByChannels)), aggregationMetrics);
        }
        return new TransformWork<>(
                new MeasuredGroupByHashWork<>(groupByHash.getGroupIds(page.getColumns(groupByChannels)), aggregationMetrics),
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
        groupByHash.startReleasingOutput();
        for (GroupedAggregator groupedAggregator : groupedAggregators) {
            groupedAggregator.prepareFinal();
        }
        // Always update the current memory usage after calling GroupedAggregator#prepareFinal(), since it can increase
        // memory consumption significantly in some situations. This also captures any memory usage reduction the
        // groupByHash may have initiated for releasing output
        updateMemory();
        // Only incrementally release memory while producing output for final aggregations, since partial aggregations
        // have a fixed memory limit and can be expected to fully flush and release their output quickly
        boolean releaseMemoryOnOutput = !partial;
        return buildResult(consecutiveGroupIds(), new PageBuilder(buildTypes()), false, releaseMemoryOnOutput);
    }

    public WorkProcessor<Page> buildSpillResult()
    {
        return buildResult(hashSortedGroupIds(), new PageBuilder(buildSpillTypes()), true, false);
    }

    public List<Type> buildSpillTypes()
    {
        ArrayList<Type> types = new ArrayList<>(groupByOutputTypes);
        for (GroupedAggregator groupedAggregator : groupedAggregators) {
            types.add(groupedAggregator.getSpillType());
        }
        // raw hash
        types.add(BIGINT);
        return types;
    }

    @VisibleForTesting
    public int getCapacity()
    {
        return groupByHash.getCapacity();
    }

    private WorkProcessor<Page> buildResult(IntIterator groupIds, PageBuilder pageBuilder, boolean appendRawHash, boolean releaseMemoryOnOutput)
    {
        int rawHashIndex = groupByChannels.length + groupedAggregators.size();
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

                if (appendRawHash) {
                    BIGINT.writeLong(pageBuilder.getBlockBuilder(rawHashIndex), groupByHash.getRawHash(groupId));
                }
            }

            // Update memory usage after producing each page of output
            if (releaseMemoryOnOutput) {
                updateMemory();
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

    public static List<Type> toTypes(List<? extends Type> groupByType, List<AggregatorFactory> factories)
    {
        ImmutableList.Builder<Type> types = ImmutableList.builderWithExpectedSize(groupByType.size() + factories.size());
        types.addAll(groupByType);
        for (AggregatorFactory factory : factories) {
            types.add(factory.createAggregator(new AggregationMetrics()).getType());
        }
        return types.build();
    }
}
