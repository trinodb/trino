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
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.aggregation.AggregatorFactory;
import io.trino.operator.aggregation.builder.HashAggregationBuilder;
import io.trino.operator.aggregation.builder.InMemoryHashAggregationBuilder;
import io.trino.operator.aggregation.builder.SpillableHashAggregationBuilder;
import io.trino.operator.aggregation.partial.PartialAggregationController;
import io.trino.operator.aggregation.partial.SkipAggregationBuilder;
import io.trino.operator.scalar.CombineHashFunction;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spiller.SpillerFactory;
import io.trino.sql.gen.JoinCompiler;
import io.trino.sql.planner.plan.AggregationNode.Step;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.operator.aggregation.builder.InMemoryHashAggregationBuilder.toTypes;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.optimizations.HashGenerationOptimizer.INITIAL_HASH_VALUE;
import static io.trino.type.TypeUtils.NULL_HASH_CODE;
import static java.util.Objects.requireNonNull;

public class HashAggregationOperator
        implements Operator
{
    static final String INPUT_ROWS_WITH_PARTIAL_AGGREGATION_DISABLED_METRIC_NAME = "Input rows processed without partial aggregation enabled";
    private static final double MERGE_WITH_MEMORY_RATIO = 0.9;

    public static class HashAggregationOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Type> groupByTypes;
        private final List<Integer> groupByChannels;
        private final List<Integer> globalAggregationGroupIds;
        private final Step step;
        private final boolean produceDefaultOutput;
        private final List<AggregatorFactory> aggregatorFactories;
        private final Optional<Integer> hashChannel;
        private final Optional<Integer> groupIdChannel;

        private final int expectedGroups;
        private final Optional<DataSize> maxPartialMemory;
        private final boolean spillEnabled;
        private final DataSize memoryLimitForMerge;
        private final DataSize memoryLimitForMergeWithMemory;
        private final SpillerFactory spillerFactory;
        private final JoinCompiler joinCompiler;
        private final TypeOperators typeOperators;
        private final Optional<PartialAggregationController> partialAggregationController;

        private boolean closed;

        @VisibleForTesting
        public HashAggregationOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> groupByTypes,
                List<Integer> groupByChannels,
                List<Integer> globalAggregationGroupIds,
                Step step,
                List<AggregatorFactory> aggregatorFactories,
                Optional<Integer> hashChannel,
                Optional<Integer> groupIdChannel,
                int expectedGroups,
                Optional<DataSize> maxPartialMemory,
                JoinCompiler joinCompiler,
                TypeOperators typeOperators,
                Optional<PartialAggregationController> partialAggregationController)
        {
            this(operatorId,
                    planNodeId,
                    groupByTypes,
                    groupByChannels,
                    globalAggregationGroupIds,
                    step,
                    false,
                    aggregatorFactories,
                    hashChannel,
                    groupIdChannel,
                    expectedGroups,
                    maxPartialMemory,
                    false,
                    DataSize.of(0, MEGABYTE),
                    DataSize.of(0, MEGABYTE),
                    (types, spillContext, memoryContext) -> {
                        throw new UnsupportedOperationException();
                    },
                    joinCompiler,
                    typeOperators,
                    partialAggregationController);
        }

        public HashAggregationOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> groupByTypes,
                List<Integer> groupByChannels,
                List<Integer> globalAggregationGroupIds,
                Step step,
                boolean produceDefaultOutput,
                List<AggregatorFactory> aggregatorFactories,
                Optional<Integer> hashChannel,
                Optional<Integer> groupIdChannel,
                int expectedGroups,
                Optional<DataSize> maxPartialMemory,
                boolean spillEnabled,
                DataSize unspillMemoryLimit,
                SpillerFactory spillerFactory,
                JoinCompiler joinCompiler,
                TypeOperators typeOperators,
                Optional<PartialAggregationController> partialAggregationController)
        {
            this(operatorId,
                    planNodeId,
                    groupByTypes,
                    groupByChannels,
                    globalAggregationGroupIds,
                    step,
                    produceDefaultOutput,
                    aggregatorFactories,
                    hashChannel,
                    groupIdChannel,
                    expectedGroups,
                    maxPartialMemory,
                    spillEnabled,
                    unspillMemoryLimit,
                    DataSize.succinctBytes((long) (unspillMemoryLimit.toBytes() * MERGE_WITH_MEMORY_RATIO)),
                    spillerFactory,
                    joinCompiler,
                    typeOperators,
                    partialAggregationController);
        }

        @VisibleForTesting
        HashAggregationOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> groupByTypes,
                List<Integer> groupByChannels,
                List<Integer> globalAggregationGroupIds,
                Step step,
                boolean produceDefaultOutput,
                List<AggregatorFactory> aggregatorFactories,
                Optional<Integer> hashChannel,
                Optional<Integer> groupIdChannel,
                int expectedGroups,
                Optional<DataSize> maxPartialMemory,
                boolean spillEnabled,
                DataSize memoryLimitForMerge,
                DataSize memoryLimitForMergeWithMemory,
                SpillerFactory spillerFactory,
                JoinCompiler joinCompiler,
                TypeOperators typeOperators,
                Optional<PartialAggregationController> partialAggregationController)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.hashChannel = requireNonNull(hashChannel, "hashChannel is null");
            this.groupIdChannel = requireNonNull(groupIdChannel, "groupIdChannel is null");
            this.groupByTypes = ImmutableList.copyOf(groupByTypes);
            this.groupByChannels = ImmutableList.copyOf(groupByChannels);
            this.globalAggregationGroupIds = ImmutableList.copyOf(globalAggregationGroupIds);
            this.step = step;
            this.produceDefaultOutput = produceDefaultOutput;
            this.aggregatorFactories = ImmutableList.copyOf(aggregatorFactories);
            this.expectedGroups = expectedGroups;
            this.maxPartialMemory = requireNonNull(maxPartialMemory, "maxPartialMemory is null");
            this.spillEnabled = spillEnabled;
            this.memoryLimitForMerge = requireNonNull(memoryLimitForMerge, "memoryLimitForMerge is null");
            this.memoryLimitForMergeWithMemory = requireNonNull(memoryLimitForMergeWithMemory, "memoryLimitForMergeWithMemory is null");
            this.spillerFactory = requireNonNull(spillerFactory, "spillerFactory is null");
            this.joinCompiler = requireNonNull(joinCompiler, "joinCompiler is null");
            this.typeOperators = requireNonNull(typeOperators, "typeOperators is null");
            this.partialAggregationController = requireNonNull(partialAggregationController, "partialAggregationController is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");

            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, HashAggregationOperator.class.getSimpleName());
            HashAggregationOperator hashAggregationOperator = new HashAggregationOperator(
                    operatorContext,
                    groupByTypes,
                    groupByChannels,
                    globalAggregationGroupIds,
                    step,
                    produceDefaultOutput,
                    aggregatorFactories,
                    hashChannel,
                    groupIdChannel,
                    expectedGroups,
                    maxPartialMemory,
                    spillEnabled,
                    memoryLimitForMerge,
                    memoryLimitForMergeWithMemory,
                    spillerFactory,
                    joinCompiler,
                    typeOperators,
                    partialAggregationController);
            return hashAggregationOperator;
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new HashAggregationOperatorFactory(
                    operatorId,
                    planNodeId,
                    groupByTypes,
                    groupByChannels,
                    globalAggregationGroupIds,
                    step,
                    produceDefaultOutput,
                    aggregatorFactories,
                    hashChannel,
                    groupIdChannel,
                    expectedGroups,
                    maxPartialMemory,
                    spillEnabled,
                    memoryLimitForMerge,
                    memoryLimitForMergeWithMemory,
                    spillerFactory,
                    joinCompiler,
                    typeOperators,
                    partialAggregationController.map(PartialAggregationController::duplicate));
        }
    }

    private final OperatorContext operatorContext;
    private final Optional<PartialAggregationController> partialAggregationController;
    private final List<Type> groupByTypes;
    private final List<Integer> groupByChannels;
    private final List<Integer> globalAggregationGroupIds;
    private final Step step;
    private final boolean produceDefaultOutput;
    private final List<AggregatorFactory> aggregatorFactories;
    private final Optional<Integer> hashChannel;
    private final Optional<Integer> groupIdChannel;
    private final int expectedGroups;
    private final Optional<DataSize> maxPartialMemory;
    private final boolean spillEnabled;
    private final DataSize memoryLimitForMerge;
    private final DataSize memoryLimitForMergeWithMemory;
    private final SpillerFactory spillerFactory;
    private final JoinCompiler joinCompiler;
    private final TypeOperators typeOperators;

    private final List<Type> types;

    private HashAggregationBuilder aggregationBuilder;
    private final LocalMemoryContext memoryContext;
    private WorkProcessor<Page> outputPages;
    private long totalInputRowsProcessed;
    private long inputRowsProcessedWithPartialAggregationDisabled;
    private boolean finishing;
    private boolean finished;

    // for yield when memory is not available
    private Work<?> unfinishedWork;
    private long aggregationInputBytesProcessed;
    private long aggregationInputRowsProcessed;
    private long aggregationUniqueRowsProduced;

    private HashAggregationOperator(
            OperatorContext operatorContext,
            List<Type> groupByTypes,
            List<Integer> groupByChannels,
            List<Integer> globalAggregationGroupIds,
            Step step,
            boolean produceDefaultOutput,
            List<AggregatorFactory> aggregatorFactories,
            Optional<Integer> hashChannel,
            Optional<Integer> groupIdChannel,
            int expectedGroups,
            Optional<DataSize> maxPartialMemory,
            boolean spillEnabled,
            DataSize memoryLimitForMerge,
            DataSize memoryLimitForMergeWithMemory,
            SpillerFactory spillerFactory,
            JoinCompiler joinCompiler,
            TypeOperators typeOperators,
            Optional<PartialAggregationController> partialAggregationController)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.partialAggregationController = requireNonNull(partialAggregationController, "partialAggregationControl is null");
        requireNonNull(step, "step is null");
        requireNonNull(aggregatorFactories, "aggregatorFactories is null");
        requireNonNull(operatorContext, "operatorContext is null");
        checkArgument(partialAggregationController.isEmpty() || step.isOutputPartial(), "partialAggregationController should be present only for partial aggregation");

        this.groupByTypes = ImmutableList.copyOf(groupByTypes);
        this.groupByChannels = ImmutableList.copyOf(groupByChannels);
        this.globalAggregationGroupIds = ImmutableList.copyOf(globalAggregationGroupIds);
        this.aggregatorFactories = ImmutableList.copyOf(aggregatorFactories);
        this.hashChannel = requireNonNull(hashChannel, "hashChannel is null");
        this.groupIdChannel = requireNonNull(groupIdChannel, "groupIdChannel is null");
        this.step = step;
        this.produceDefaultOutput = produceDefaultOutput;
        this.expectedGroups = expectedGroups;
        this.maxPartialMemory = requireNonNull(maxPartialMemory, "maxPartialMemory is null");
        this.types = toTypes(groupByTypes, aggregatorFactories, hashChannel);
        this.spillEnabled = spillEnabled;
        this.memoryLimitForMerge = requireNonNull(memoryLimitForMerge, "memoryLimitForMerge is null");
        this.memoryLimitForMergeWithMemory = requireNonNull(memoryLimitForMergeWithMemory, "memoryLimitForMergeWithMemory is null");
        this.spillerFactory = requireNonNull(spillerFactory, "spillerFactory is null");
        this.joinCompiler = requireNonNull(joinCompiler, "joinCompiler is null");
        this.typeOperators = requireNonNull(typeOperators, "typeOperators is null");

        this.memoryContext = operatorContext.localUserMemoryContext();
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
        return finished;
    }

    @Override
    public boolean needsInput()
    {
        if (finishing || outputPages != null) {
            return false;
        }
        if (aggregationBuilder != null && aggregationBuilder.isFull()) {
            return false;
        }
        return unfinishedWork == null;
    }

    @Override
    public void addInput(Page page)
    {
        checkState(unfinishedWork == null, "Operator has unfinished work");
        checkState(!finishing, "Operator is already finishing");
        requireNonNull(page, "page is null");
        totalInputRowsProcessed += page.getPositionCount();

        if (aggregationBuilder == null) {
            boolean partialAggregationDisabled = partialAggregationController
                    .map(PartialAggregationController::isPartialAggregationDisabled)
                    .orElse(false);
            if (step.isOutputPartial() && partialAggregationDisabled) {
                aggregationBuilder = new SkipAggregationBuilder(groupByChannels, hashChannel, aggregatorFactories, memoryContext);
            }
            else if (step.isOutputPartial() || !spillEnabled || !isSpillable()) {
                // TODO: We ignore spillEnabled here if any aggregate has ORDER BY clause or DISTINCT because they are not yet implemented for spilling.
                aggregationBuilder = new InMemoryHashAggregationBuilder(
                        aggregatorFactories,
                        step,
                        expectedGroups,
                        groupByTypes,
                        groupByChannels,
                        hashChannel,
                        operatorContext,
                        maxPartialMemory,
                        joinCompiler,
                        typeOperators,
                        () -> {
                            memoryContext.setBytes(((InMemoryHashAggregationBuilder) aggregationBuilder).getSizeInMemory());
                            if (step.isOutputPartial() && maxPartialMemory.isPresent()) {
                                // do not yield on memory for partial aggregations
                                return true;
                            }
                            return operatorContext.isWaitingForMemory().isDone();
                        });
            }
            else {
                aggregationBuilder = new SpillableHashAggregationBuilder(
                        aggregatorFactories,
                        step,
                        expectedGroups,
                        groupByTypes,
                        groupByChannels,
                        hashChannel,
                        operatorContext,
                        memoryLimitForMerge,
                        memoryLimitForMergeWithMemory,
                        spillerFactory,
                        joinCompiler,
                        typeOperators);
            }

            // assume initial aggregationBuilder is not full
        }
        else {
            checkState(!aggregationBuilder.isFull(), "Aggregation buffer is full");
        }

        // process the current page; save the unfinished work if we are waiting for memory
        unfinishedWork = aggregationBuilder.processPage(page);
        if (unfinishedWork.process()) {
            unfinishedWork = null;
        }
        aggregationBuilder.updateMemory();
        aggregationInputBytesProcessed += page.getSizeInBytes();
        aggregationInputRowsProcessed += page.getPositionCount();
    }

    private boolean isSpillable()
    {
        return aggregatorFactories.stream().allMatch(AggregatorFactory::isSpillable);
    }

    @Override
    public ListenableFuture<Void> startMemoryRevoke()
    {
        if (aggregationBuilder != null) {
            return aggregationBuilder.startMemoryRevoke();
        }
        return NOT_BLOCKED;
    }

    @Override
    public void finishMemoryRevoke()
    {
        if (aggregationBuilder != null) {
            aggregationBuilder.finishMemoryRevoke();
        }
    }

    @Override
    public Page getOutput()
    {
        if (finished) {
            return null;
        }

        // process unfinished work if one exists
        if (unfinishedWork != null) {
            boolean workDone = unfinishedWork.process();
            aggregationBuilder.updateMemory();
            if (!workDone) {
                return null;
            }
            unfinishedWork = null;
        }

        if (outputPages == null) {
            if (finishing) {
                if (totalInputRowsProcessed == 0 && produceDefaultOutput) {
                    // global aggregations always generate an output row with the default aggregation output (e.g. 0 for COUNT, NULL for SUM)
                    finished = true;
                    return getGlobalAggregationOutput();
                }

                if (aggregationBuilder == null) {
                    finished = true;
                    return null;
                }
            }

            // only flush if we are finishing or the aggregation builder is full
            if (!finishing && (aggregationBuilder == null || !aggregationBuilder.isFull())) {
                return null;
            }

            outputPages = aggregationBuilder.buildResult();
        }

        if (!outputPages.process()) {
            return null;
        }

        if (outputPages.isFinished()) {
            closeAggregationBuilder();
            return null;
        }

        Page result = outputPages.getResult();
        aggregationUniqueRowsProduced += result.getPositionCount();
        return result;
    }

    @Override
    public void close()
    {
        closeAggregationBuilder();
    }

    @VisibleForTesting
    public HashAggregationBuilder getAggregationBuilder()
    {
        return aggregationBuilder;
    }

    private void closeAggregationBuilder()
    {
        if (aggregationBuilder instanceof SkipAggregationBuilder) {
            inputRowsProcessedWithPartialAggregationDisabled += aggregationInputRowsProcessed;
            operatorContext.setLatestMetrics(new Metrics(ImmutableMap.of(
                    INPUT_ROWS_WITH_PARTIAL_AGGREGATION_DISABLED_METRIC_NAME, new LongCount(inputRowsProcessedWithPartialAggregationDisabled))));
            partialAggregationController.ifPresent(controller -> controller.onFlush(aggregationInputBytesProcessed, aggregationInputRowsProcessed, OptionalLong.empty()));
        }
        else {
            partialAggregationController.ifPresent(controller -> controller.onFlush(aggregationInputBytesProcessed, aggregationInputRowsProcessed, OptionalLong.of(aggregationUniqueRowsProduced)));
        }
        aggregationInputBytesProcessed = 0;
        aggregationInputRowsProcessed = 0;
        aggregationUniqueRowsProduced = 0;

        outputPages = null;
        if (aggregationBuilder != null) {
            aggregationBuilder.close();
            // aggregationBuilder.close() will release all memory reserved in memory accounting.
            // The reference must be set to null afterwards to avoid unaccounted memory.
            aggregationBuilder = null;
        }
        memoryContext.setBytes(0);
    }

    private Page getGlobalAggregationOutput()
    {
        // global aggregation output page will only be constructed once,
        // so a new PageBuilder is constructed (instead of using PageBuilder.reset)
        PageBuilder output = new PageBuilder(globalAggregationGroupIds.size(), types);

        for (int groupId : globalAggregationGroupIds) {
            output.declarePosition();
            int channel = 0;

            while (channel < groupByTypes.size()) {
                if (channel == groupIdChannel.orElseThrow()) {
                    BIGINT.writeLong(output.getBlockBuilder(channel), groupId);
                }
                else {
                    output.getBlockBuilder(channel).appendNull();
                }
                channel++;
            }

            if (hashChannel.isPresent()) {
                long hashValue = calculateDefaultOutputHash(groupByTypes, groupIdChannel.orElseThrow(), groupId);
                BIGINT.writeLong(output.getBlockBuilder(channel), hashValue);
                channel++;
            }

            for (AggregatorFactory aggregatorFactory : aggregatorFactories) {
                aggregatorFactory.createAggregator().evaluate(output.getBlockBuilder(channel));
                channel++;
            }
        }

        if (output.isEmpty()) {
            return null;
        }
        return output.build();
    }

    private static long calculateDefaultOutputHash(List<Type> groupByChannels, int groupIdChannel, int groupId)
    {
        // Default output has NULLs on all columns except of groupIdChannel
        long result = INITIAL_HASH_VALUE;
        for (int channel = 0; channel < groupByChannels.size(); channel++) {
            if (channel != groupIdChannel) {
                result = CombineHashFunction.getHash(result, NULL_HASH_CODE);
            }
            else {
                result = CombineHashFunction.getHash(result, BigintType.hash(groupId));
            }
        }
        return result;
    }
}
