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
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.spi.Page;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.JoinCompiler;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.TopNRankingNode.RankingType;
import io.trino.type.BlockTypeOperators;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
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
        private final List<SortOrder> sortOrder;
        private final int maxRowCountPerPartition;
        private final boolean partial;
        private final Optional<Integer> hashChannel;
        private final int expectedPositions;

        private final boolean generateRanking;
        private boolean closed;
        private final JoinCompiler joinCompiler;
        private final TypeOperators typeOperators;
        private final BlockTypeOperators blockTypeOperators;

        public TopNRankingOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                RankingType rankingType,
                List<? extends Type> sourceTypes,
                List<Integer> outputChannels,
                List<Integer> partitionChannels,
                List<? extends Type> partitionTypes,
                List<Integer> sortChannels,
                List<SortOrder> sortOrder,
                int maxRowCountPerPartition,
                boolean partial,
                Optional<Integer> hashChannel,
                int expectedPositions,
                JoinCompiler joinCompiler,
                TypeOperators typeOperators,
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
            this.sortOrder = ImmutableList.copyOf(requireNonNull(sortOrder));
            this.hashChannel = requireNonNull(hashChannel, "hashChannel is null");
            this.partial = partial;
            checkArgument(maxRowCountPerPartition > 0, "maxRowCountPerPartition must be > 0");
            this.maxRowCountPerPartition = maxRowCountPerPartition;
            checkArgument(expectedPositions > 0, "expectedPositions must be > 0");
            this.generateRanking = !partial;
            this.expectedPositions = expectedPositions;
            this.joinCompiler = requireNonNull(joinCompiler, "joinCompiler is null");
            this.typeOperators = requireNonNull(typeOperators, "typeOperators is null");
            this.blockTypeOperators = requireNonNull(blockTypeOperators, "blockTypeOperators is null");
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
                    sortOrder,
                    maxRowCountPerPartition,
                    generateRanking,
                    hashChannel,
                    expectedPositions,
                    joinCompiler,
                    typeOperators,
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
                    sortOrder,
                    maxRowCountPerPartition,
                    partial,
                    hashChannel,
                    expectedPositions,
                    joinCompiler,
                    typeOperators,
                    blockTypeOperators);
        }
    }

    private final OperatorContext operatorContext;
    private final LocalMemoryContext localUserMemoryContext;

    private final int[] outputChannels;

    private final GroupByHash groupByHash;
    private final GroupedTopNBuilder groupedTopNBuilder;

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
            List<SortOrder> sortOrders,
            int maxRankingPerPartition,
            boolean generateRanking,
            Optional<Integer> hashChannel,
            int expectedPositions,
            JoinCompiler joinCompiler,
            TypeOperators typeOperators,
            BlockTypeOperators blockTypeOperators)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.localUserMemoryContext = operatorContext.localUserMemoryContext();

        ImmutableList.Builder<Integer> outputChannelsBuilder = ImmutableList.builder();
        for (int channel : requireNonNull(outputChannels, "outputChannels is null")) {
            outputChannelsBuilder.add(channel);
        }
        if (generateRanking) {
            outputChannelsBuilder.add(outputChannels.size());
        }
        this.outputChannels = Ints.toArray(outputChannelsBuilder.build());

        checkArgument(maxRankingPerPartition > 0, "maxRankingPerPartition must be > 0");

        if (!partitionChannels.isEmpty()) {
            checkArgument(expectedPositions > 0, "expectedPositions must be > 0");
            groupByHash = createGroupByHash(
                    operatorContext.getSession(),
                    partitionTypes,
                    Ints.toArray(partitionChannels),
                    hashChannel,
                    expectedPositions,
                    joinCompiler,
                    blockTypeOperators,
                    this::updateMemoryReservation);
        }
        else {
            groupByHash = new NoChannelGroupByHash();
        }

        switch (rankingType) {
            case ROW_NUMBER:
                this.groupedTopNBuilder = new GroupedTopNRowNumberBuilder(
                        ImmutableList.copyOf(sourceTypes),
                        new SimplePageWithPositionComparator(ImmutableList.copyOf(sourceTypes), sortChannels, sortOrders, typeOperators),
                        maxRankingPerPartition,
                        generateRanking,
                        groupByHash);
                break;
            case RANK:
                this.groupedTopNBuilder = new GroupedTopNRankBuilder(
                        ImmutableList.copyOf(sourceTypes),
                        new SimplePageWithPositionComparator(ImmutableList.copyOf(sourceTypes), sortChannels, sortOrders, typeOperators),
                        new SimplePageWithPositionEqualsAndHash(ImmutableList.copyOf(sourceTypes), sortChannels, blockTypeOperators),
                        maxRankingPerPartition,
                        generateRanking,
                        groupByHash);
                break;
            case DENSE_RANK:
                throw new UnsupportedOperationException();
            default:
                throw new AssertionError("Unknown ranking type: " + rankingType);
        }
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
        return finishing && outputIterator != null && !outputIterator.hasNext() && unfinishedWork == null;
    }

    @Override
    public boolean needsInput()
    {
        // still has more input, has not started flushing yet, and has no unfinished work
        return !finishing && outputIterator == null && unfinishedWork == null;
    }

    @Override
    public void addInput(Page page)
    {
        checkState(!finishing, "Operator is already finishing");
        checkState(unfinishedWork == null, "Cannot add input with the operator when unfinished work is not empty");
        checkState(outputIterator == null, "Cannot add input with the operator when flushing");
        requireNonNull(page, "page is null");
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

        if (!finishing) {
            return null;
        }

        if (outputIterator == null) {
            // start flushing
            outputIterator = groupedTopNBuilder.buildResult();
        }

        Page output = null;
        if (outputIterator.hasNext()) {
            // rewrite to expected column ordering
            output = outputIterator.next().getColumns(outputChannels);
        }
        updateMemoryReservation();
        return output;
    }

    @VisibleForTesting
    public int getCapacity()
    {
        checkState(groupByHash != null);
        return groupByHash.getCapacity();
    }

    private boolean updateMemoryReservation()
    {
        // TODO: may need to use trySetMemoryReservation with a compaction to free memory (but that may cause GC pressure)
        localUserMemoryContext.setBytes(groupedTopNBuilder.getEstimatedSizeInBytes());
        return operatorContext.isWaitingForMemory().isDone();
    }
}
