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
package io.trino.operator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.spi.Page;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.TopNRankingNode.RankingType;
import io.trino.type.BlockTypeOperators;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.operator.GroupByHash.createGroupByHash;
import static java.util.Objects.requireNonNull;

public class TopNRankingOperator
        implements Operator
{
    public static class TopNRankingOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;

        private final RankingType rankingType;
        private final List<Type> sourceTypes;

        private final List<Integer> outputChannels;
        private final List<Integer> partitionChannels;
        private final List<Type> partitionTypes;
        private final List<Integer> sortChannels;
        private final int maxRowCountPerPartition;
        private final boolean partial;
        private final int expectedPositions;

        private final boolean generateRanking;
        private boolean closed;
        private final FlatHashStrategyCompiler hashStrategyCompiler;
        private final PageWithPositionComparator comparator;
        private final BlockTypeOperators blockTypeOperators;
        private final Optional<DataSize> maxPartialMemory;

        public TopNRankingOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                RankingType rankingType,
                List<? extends Type> sourceTypes,
                List<Integer> outputChannels,
                List<Integer> partitionChannels,
                List<? extends Type> partitionTypes,
                List<Integer> sortChannels,
                int maxRowCountPerPartition,
                boolean partial,
                int expectedPositions,
                Optional<DataSize> maxPartialMemory,
                FlatHashStrategyCompiler hashStrategyCompiler,
                PageWithPositionComparator comparator,
                BlockTypeOperators blockTypeOperators)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.rankingType = requireNonNull(rankingType, "rankingType is null");
            this.sourceTypes = ImmutableList.copyOf(sourceTypes);
            this.outputChannels = ImmutableList.copyOf(requireNonNull(outputChannels, "outputChannels is null"));
            this.partitionChannels = ImmutableList.copyOf(requireNonNull(partitionChannels, "partitionChannels is null"));
            this.partitionTypes = ImmutableList.copyOf(requireNonNull(partitionTypes, "partitionTypes is null"));
            this.sortChannels = ImmutableList.copyOf(requireNonNull(sortChannels));
            this.partial = partial;
            checkArgument(maxRowCountPerPartition > 0, "maxRowCountPerPartition must be > 0");
            this.maxRowCountPerPartition = maxRowCountPerPartition;
            checkArgument(expectedPositions > 0, "expectedPositions must be > 0");
            this.generateRanking = !partial;
            this.expectedPositions = expectedPositions;
            this.hashStrategyCompiler = requireNonNull(hashStrategyCompiler, "hashStrategyCompiler is null");
            this.comparator = requireNonNull(comparator, "comparator is null");
            this.blockTypeOperators = requireNonNull(blockTypeOperators, "blockTypeOperators is null");
            this.maxPartialMemory = requireNonNull(maxPartialMemory, "maxPartialMemory is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, TopNRankingOperator.class.getSimpleName());
            return new TopNRankingOperator(
                    operatorContext,
                    rankingType,
                    sourceTypes,
                    outputChannels,
                    partitionChannels,
                    partitionTypes,
                    sortChannels,
                    maxRowCountPerPartition,
                    generateRanking,
                    expectedPositions,
                    maxPartialMemory,
                    hashStrategyCompiler,
                    comparator,
                    blockTypeOperators);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new TopNRankingOperatorFactory(
                    operatorId,
                    planNodeId,
                    rankingType,
                    sourceTypes,
                    outputChannels,
                    partitionChannels,
                    partitionTypes,
                    sortChannels,
                    maxRowCountPerPartition,
                    partial,
                    expectedPositions,
                    maxPartialMemory,
                    hashStrategyCompiler,
                    comparator,
                    blockTypeOperators);
        }
    }

    private final OperatorContext operatorContext;
    private final LocalMemoryContext localMemoryContext;

    private final int[] outputChannels;
    private final Supplier<GroupedTopNBuilder> groupedTopNBuilderSupplier;
    private final boolean partial;
    private final long maxFlushableBytes;

    private GroupedTopNBuilder groupedTopNBuilder;
    private boolean finishing;
    private Work<?> unfinishedWork;
    private Iterator<Page> outputIterator;

    public TopNRankingOperator(
            OperatorContext operatorContext,
            RankingType rankingType,
            List<? extends Type> sourceTypes,
            List<Integer> outputChannels,
            List<Integer> partitionChannels,
            List<Type> partitionTypes,
            List<Integer> sortChannels,
            int maxRankingPerPartition,
            boolean generateRanking,
            int expectedPositions,
            Optional<DataSize> maxPartialMemory,
            FlatHashStrategyCompiler hashStrategyCompiler,
            PageWithPositionComparator comparator,
            BlockTypeOperators blockTypeOperators)
    {
        requireNonNull(maxPartialMemory, "maxPartialMemory is null");
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.localMemoryContext = operatorContext.localUserMemoryContext();

        ImmutableList.Builder<Integer> outputChannelsBuilder = ImmutableList.builder();
        for (int channel : requireNonNull(outputChannels, "outputChannels is null")) {
            outputChannelsBuilder.add(channel);
        }
        if (generateRanking) {
            outputChannelsBuilder.add(outputChannels.size());
        }
        this.outputChannels = Ints.toArray(outputChannelsBuilder.build());
        this.partial = !generateRanking;

        checkArgument(maxRankingPerPartition > 0, "maxRankingPerPartition must be > 0");

        checkArgument(maxPartialMemory.isEmpty() || !generateRanking, "no partial memory on final TopN");
        this.maxFlushableBytes = maxPartialMemory.map(DataSize::toBytes).orElse(Long.MAX_VALUE);

        int[] groupByChannels = Ints.toArray(partitionChannels);

        this.groupedTopNBuilderSupplier = getGroupedTopNBuilderSupplier(
                rankingType,
                ImmutableList.copyOf(sourceTypes),
                sortChannels,
                maxRankingPerPartition,
                generateRanking,
                comparator,
                blockTypeOperators,
                groupByChannels,
                getGroupByHashSupplier(
                        expectedPositions,
                        partitionTypes,
                        operatorContext.getSession(),
                        hashStrategyCompiler,
                        this::updateMemoryReservation));
    }

    private static Supplier<GroupByHash> getGroupByHashSupplier(
            int expectedPositions,
            List<Type> partitionTypes,
            Session session,
            FlatHashStrategyCompiler hashStrategyCompiler,
            UpdateMemory updateMemory)
    {
        if (partitionTypes.isEmpty()) {
            return Suppliers.ofInstance(new NoChannelGroupByHash());
        }
        checkArgument(expectedPositions > 0, "expectedPositions must be > 0");
        return () -> createGroupByHash(
                session,
                partitionTypes,
                false,
                expectedPositions,
                hashStrategyCompiler,
                updateMemory);
    }

    private static Supplier<GroupedTopNBuilder> getGroupedTopNBuilderSupplier(
            RankingType rankingType,
            List<Type> sourceTypes,
            List<Integer> sortChannels,
            int maxRankingPerPartition,
            boolean generateRanking,
            PageWithPositionComparator comparator,
            BlockTypeOperators blockTypeOperators,
            int[] groupByChannels,
            Supplier<GroupByHash> groupByHashSupplier)
    {
        if (rankingType == RankingType.ROW_NUMBER) {
            return () -> new GroupedTopNRowNumberBuilder(
                    sourceTypes,
                    comparator,
                    maxRankingPerPartition,
                    generateRanking,
                    groupByChannels,
                    groupByHashSupplier.get());
        }
        if (rankingType == RankingType.RANK) {
            List<Type> sortTypes = sortChannels.stream()
                    .map(sourceTypes::get)
                    .collect(toImmutableList());
            PageWithPositionEqualsAndHash equalsAndHash = new SimplePageWithPositionEqualsAndHash(sortTypes, sortChannels, blockTypeOperators);
            return () -> new GroupedTopNRankBuilder(
                    sourceTypes,
                    comparator,
                    equalsAndHash,
                    maxRankingPerPartition,
                    generateRanking,
                    groupByChannels,
                    groupByHashSupplier.get());
        }
        if (rankingType == RankingType.DENSE_RANK) {
            throw new UnsupportedOperationException();
        }
        throw new AssertionError("Unknown ranking type: " + rankingType);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        // has no more input, has finished flushing, and has no unfinished work
        return finishing && outputIterator == null && groupedTopNBuilder == null && unfinishedWork == null;
    }

    @Override
    public boolean needsInput()
    {
        // still has more input, has not started flushing yet, and has no unfinished work
        return !finishing && outputIterator == null && !isBuilderFull() && unfinishedWork == null;
    }

    @Override
    public void addInput(Page page)
    {
        checkState(!finishing, "Operator is already finishing");
        checkState(unfinishedWork == null, "Cannot add input with the operator when unfinished work is not empty");
        checkState(outputIterator == null, "Cannot add input with the operator when flushing");
        requireNonNull(page, "page is null");
        checkState(!isBuilderFull(), "TopN buffer is already full");

        if (groupedTopNBuilder == null) {
            groupedTopNBuilder = groupedTopNBuilderSupplier.get();
        }
        unfinishedWork = groupedTopNBuilder.processPage(page);
        if (unfinishedWork.process()) {
            unfinishedWork = null;
        }
        updateMemoryReservation();
    }

    @Override
    public Page getOutput()
    {
        if (unfinishedWork != null) {
            boolean finished = unfinishedWork.process();
            updateMemoryReservation();
            if (!finished) {
                return null;
            }
            unfinishedWork = null;
        }

        if (!finishing && (!partial || !isBuilderFull()) && outputIterator == null) {
            return null;
        }

        if (outputIterator == null && groupedTopNBuilder != null) {
            // start flushing
            outputIterator = groupedTopNBuilder.buildResult();
        }

        Page output;
        if (outputIterator != null && outputIterator.hasNext()) {
            // rewrite to expected column ordering
            output = outputIterator.next().getColumns(outputChannels);
        }
        else {
            closeGroupedTopNBuilder();
            return null;
        }
        updateMemoryReservation();
        return output;
    }

    @Override
    public void close()
    {
        closeGroupedTopNBuilder();
    }

    private void closeGroupedTopNBuilder()
    {
        outputIterator = null;
        groupedTopNBuilder = null;
        localMemoryContext.setBytes(0);
    }

    private boolean updateMemoryReservation()
    {
        if (groupedTopNBuilder == null) {
            localMemoryContext.setBytes(0);
            return true;
        }
        // TODO: may need to use trySetMemoryReservation with a compaction to free memory (but that may cause GC pressure)
        localMemoryContext.setBytes(groupedTopNBuilder.getEstimatedSizeInBytes());
        if (partial) {
            // do not yield on memory for partial aggregations
            return true;
        }
        return operatorContext.isWaitingForMemory().isDone();
    }

    private boolean isBuilderFull()
    {
        return groupedTopNBuilder != null && groupedTopNBuilder.getEstimatedSizeInBytes() >= maxFlushableBytes;
    }

    @VisibleForTesting
    GroupedTopNBuilder getGroupedTopNBuilder()
    {
        return groupedTopNBuilder;
    }
}
