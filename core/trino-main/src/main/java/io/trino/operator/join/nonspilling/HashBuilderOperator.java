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
package io.trino.operator.join.nonspilling;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.ThreadSafe;
import io.airlift.units.DataSize;
import io.trino.memory.context.CoarseGrainLocalMemoryContext;
import io.trino.operator.DriverContext;
import io.trino.operator.HashArraySizeSupplier;
import io.trino.operator.Operator;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorFactory;
import io.trino.operator.PagesIndex;
import io.trino.operator.RuntimeConstraintCollectionLimits;
import io.trino.operator.RuntimeConstraintRequest;
import io.trino.operator.RuntimeConstraintSourceConsumer;
import io.trino.operator.RuntimeConstraintSourceOperator;
import io.trino.operator.RuntimeConstraintWiringContext;
import io.trino.operator.TaskRuntimeConstraintManager;
import io.trino.operator.join.JoinBridgeManager;
import io.trino.operator.join.LookupSourceSupplier;
import io.trino.operator.join.RuntimeConstraintComparison;
import io.trino.spi.Page;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.JoinFilterFunctionCompiler.JoinFilterFunctionFactory;
import io.trino.sql.ir.ComparisonOperator;
import io.trino.sql.planner.LocalRuntimeConstraintConsumer;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.runtimeconstraint.DistributedCompletionPolicy;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintWiringReport.CollectedConstraint;
import jakarta.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.trino.memory.context.CoarseGrainLocalMemoryContext.DEFAULT_GRANULARITY;
import static java.util.Objects.requireNonNull;

/**
 * Like {@link io.trino.operator.join.spilling.HashBuilderOperator} but simplified,
 * without spill support.
 */
@ThreadSafe
public class HashBuilderOperator
        implements Operator
{
    public static class HashBuilderOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager;
        private final List<Integer> outputChannels;
        private final List<Integer> hashChannels;
        private final Optional<JoinFilterFunctionFactory> filterFunctionFactory;
        private final OptionalInt sortChannel;
        private final List<JoinFilterFunctionFactory> searchFunctionFactories;
        private final PagesIndex.Factory pagesIndexFactory;

        private final int expectedPositions;
        private final HashArraySizeSupplier hashArraySizeSupplier;
        private final RuntimeConstraintCollectionLimits runtimeConstraintLimits;
        private final TypeOperators typeOperators;
        private final DistributedCompletionPolicy runtimeConstraintCompletionPolicy;

        private int partitionIndex;
        private final AtomicReference<TaskRuntimeConstraintManager> runtimeConstraintManager = new AtomicReference<>();
        private RuntimeConstraintSourceConsumer runtimeConstraintConsumer;
        private boolean runtimeConstraintCollectionRelocated;

        private boolean closed;

        public HashBuilderOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager,
                List<Integer> outputChannels,
                List<Integer> hashChannels,
                Optional<JoinFilterFunctionFactory> filterFunctionFactory,
                OptionalInt sortChannel,
                List<JoinFilterFunctionFactory> searchFunctionFactories,
                int expectedPositions,
                PagesIndex.Factory pagesIndexFactory,
                HashArraySizeSupplier hashArraySizeSupplier)
        {
            this(operatorId,
                    planNodeId,
                    lookupSourceFactoryManager,
                    outputChannels,
                    hashChannels,
                    filterFunctionFactory,
                    sortChannel,
                    searchFunctionFactories,
                    expectedPositions,
                    pagesIndexFactory,
                    hashArraySizeSupplier,
                    new RuntimeConstraintCollectionLimits(50_000, DataSize.of(4, DataSize.Unit.MEGABYTE), 100_000, DataSize.of(5, DataSize.Unit.MEGABYTE)),
                    new TypeOperators(),
                    DistributedCompletionPolicy.UNION_ALL_PARTITIONS);
        }

        public HashBuilderOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager,
                List<Integer> outputChannels,
                List<Integer> hashChannels,
                Optional<JoinFilterFunctionFactory> filterFunctionFactory,
                OptionalInt sortChannel,
                List<JoinFilterFunctionFactory> searchFunctionFactories,
                int expectedPositions,
                PagesIndex.Factory pagesIndexFactory,
                HashArraySizeSupplier hashArraySizeSupplier,
                RuntimeConstraintCollectionLimits runtimeConstraintLimits,
                TypeOperators typeOperators)
        {
            this(operatorId, planNodeId, lookupSourceFactoryManager, outputChannels, hashChannels, filterFunctionFactory, sortChannel, searchFunctionFactories, expectedPositions, pagesIndexFactory, hashArraySizeSupplier, runtimeConstraintLimits, typeOperators, DistributedCompletionPolicy.UNION_ALL_PARTITIONS);
        }

        public HashBuilderOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager,
                List<Integer> outputChannels,
                List<Integer> hashChannels,
                Optional<JoinFilterFunctionFactory> filterFunctionFactory,
                OptionalInt sortChannel,
                List<JoinFilterFunctionFactory> searchFunctionFactories,
                int expectedPositions,
                PagesIndex.Factory pagesIndexFactory,
                HashArraySizeSupplier hashArraySizeSupplier,
                RuntimeConstraintCollectionLimits runtimeConstraintLimits,
                TypeOperators typeOperators,
                DistributedCompletionPolicy runtimeConstraintCompletionPolicy)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            requireNonNull(sortChannel, "sortChannel cannot be null");
            requireNonNull(searchFunctionFactories, "searchFunctionFactories is null");
            checkArgument(sortChannel.isPresent() != searchFunctionFactories.isEmpty(), "both or none sortChannel and searchFunctionFactories must be set");
            this.lookupSourceFactoryManager = requireNonNull(lookupSourceFactoryManager, "lookupSourceFactoryManager is null");

            this.outputChannels = ImmutableList.copyOf(requireNonNull(outputChannels, "outputChannels is null"));
            this.hashChannels = ImmutableList.copyOf(requireNonNull(hashChannels, "hashChannels is null"));
            this.filterFunctionFactory = requireNonNull(filterFunctionFactory, "filterFunctionFactory is null");
            this.sortChannel = sortChannel;
            this.searchFunctionFactories = ImmutableList.copyOf(searchFunctionFactories);
            this.pagesIndexFactory = requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");
            this.hashArraySizeSupplier = requireNonNull(hashArraySizeSupplier, "hashArraySizeSupplier is null");

            this.expectedPositions = expectedPositions;
            this.runtimeConstraintLimits = requireNonNull(runtimeConstraintLimits, "runtimeConstraintLimits is null");
            this.typeOperators = requireNonNull(typeOperators, "typeOperators is null");
            this.runtimeConstraintCompletionPolicy = requireNonNull(runtimeConstraintCompletionPolicy, "runtimeConstraintCompletionPolicy is null");
        }

        @Override
        public HashBuilderOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            if (!runtimeConstraintCollectionRelocated) {
                initializeRuntimeConstraintSource();
            }
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, HashBuilderOperator.class.getSimpleName());
            TaskRuntimeConstraintManager manager = driverContext.getPipelineContext().getTaskContext().getRuntimeConstraintManager();
            runtimeConstraintManager.compareAndSet(null, manager);
            checkState(runtimeConstraintManager.get() == manager, "runtime constraint manager changed");

            PartitionedLookupSourceFactory lookupSourceFactory = this.lookupSourceFactoryManager.getJoinBridge();
            verify(partitionIndex < lookupSourceFactory.partitions());
            partitionIndex++;
            List<Integer> runtimeConstraintChannels = runtimeConstraintChannels();
            List<Type> buildTypes = runtimeConstraintChannels.stream().map(lookupSourceFactory.getTypes()::get).toList();
            Operator runtimeConstraintCollector = runtimeConstraintChannels.isEmpty() || runtimeConstraintCollectionRelocated
                    ? null
                    : RuntimeConstraintSourceOperator.createCollector(
                    operatorContext,
                    requireNonNull(runtimeConstraintConsumer, "runtimeConstraintConsumer is not initialized"),
                    IntStream.range(0, runtimeConstraintChannels.size())
                    .mapToObj(index -> new RuntimeConstraintSourceOperator.Channel(buildTypes.get(index), runtimeConstraintChannels.get(index)))
                    .toList(),
                    runtimeConstraintLimits.maxDistinctValues(),
                    runtimeConstraintLimits.maxFilterSize(),
                    runtimeConstraintLimits.minMaxCollectionLimit(),
                    typeOperators);
            return new HashBuilderOperator(
                    operatorContext,
                    lookupSourceFactory,
                    partitionIndex - 1,
                    outputChannels,
                    hashChannels,
                    filterFunctionFactory,
                    sortChannel,
                    searchFunctionFactories,
                    expectedPositions,
                    pagesIndexFactory,
                    hashArraySizeSupplier,
                    runtimeConstraintCollector);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
            if (runtimeConstraintConsumer != null && partitionIndex > 0) {
                runtimeConstraintConsumer.setPartitionCount(partitionIndex);
            }
        }

        @Override
        public void completeRuntimeConstraintWiring(RuntimeConstraintWiringContext context)
        {
            if (runtimeConstraintCollectionRelocated) {
                return;
            }
            List<Type> buildTypes = lookupSourceFactoryManager.getJoinBridge().getTypes();
            List<Integer> channels = runtimeConstraintChannels();
            if (!channels.isEmpty()) {
                context.registerSource(
                        planNodeId,
                        runtimeConstraintRequests().stream()
                                .map(request -> new CollectedConstraint(request.constraintId(), request.operator(), request.nullAllowed(), channels.indexOf(request.channel())))
                                .toList(),
                        channels.stream().map(buildTypes::get).toList(),
                        runtimeConstraintCompletionPolicy);
                initializeRuntimeConstraintSource();
            }
        }

        @Override
        public void propagateRuntimeConstraint(RuntimeConstraintRequest request, Consumer<RuntimeConstraintRequest> input, RuntimeConstraintWiringContext context)
        {
            if (!request.isConstraint() || !request.channelsMatch(channel -> channel < outputChannels.size())) {
                context.stop(this, request);
                return;
            }
            input.accept(request.mapChannels(outputChannels::get));
        }

        @Override
        public void registerRuntimeConstraintInput(Consumer<List<RuntimeConstraintRequest>> requests, RuntimeConstraintWiringContext context)
        {
            context.registerLocalConsumer(lookupSourceFactoryManager, requests);
        }

        @Override
        public List<RuntimeConstraintRequest> getInputRuntimeConstraints(RuntimeConstraintWiringContext context)
        {
            if (!context.isTaskRetry()) {
                return ImmutableList.of();
            }
            runtimeConstraintCollectionRelocated = true;
            return runtimeConstraintRequests();
        }

        private List<Integer> runtimeConstraintChannels()
        {
            return runtimeConstraintRequests().stream().map(RuntimeConstraintRequest::channel).distinct().toList();
        }

        private List<RuntimeConstraintRequest> runtimeConstraintRequests()
        {
            List<Type> buildTypes = lookupSourceFactoryManager.getJoinBridge().getTypes();
            boolean replicated = runtimeConstraintCompletionPolicy == DistributedCompletionPolicy.EQUIVALENT_REPLICAS;
            List<RuntimeConstraintRequest> requests = new ArrayList<>();
            for (int channel : hashChannels) {
                requests.add(RuntimeConstraintRequest.collection(
                        RuntimeConstraintRequest.joinConstraintId(planNodeId, requests.size()),
                        channel,
                        ComparisonOperator.EQUAL,
                        false,
                        buildTypes.get(channel),
                        replicated));
            }
            for (RuntimeConstraintComparison comparison : filterFunctionFactory.stream()
                    .flatMap(factory -> factory.getRuntimeConstraintComparisons().stream())
                    .toList()) {
                requests.add(RuntimeConstraintRequest.collection(
                        RuntimeConstraintRequest.joinConstraintId(planNodeId, requests.size()),
                        comparison.buildChannel(),
                        comparison.operator(),
                        comparison.nullAllowed(),
                        buildTypes.get(comparison.buildChannel()),
                        replicated));
            }
            return ImmutableList.copyOf(requests);
        }

        private void initializeRuntimeConstraintSource()
        {
            if (runtimeConstraintConsumer != null || runtimeConstraintChannels().isEmpty()) {
                return;
            }
            List<Type> buildTypes = lookupSourceFactoryManager.getJoinBridge().getTypes();
            List<Integer> channels = runtimeConstraintChannels();
            runtimeConstraintConsumer = new LocalRuntimeConstraintConsumer(
                    channels,
                    channels.stream().map(buildTypes::get).toList(),
                    payload -> runtimeConstraintManager.get().addContribution(planNodeId, payload),
                    runtimeConstraintLimits.maxSizePerOperator());
        }

        @Override
        public OperatorFactory duplicate()
        {
            throw new UnsupportedOperationException("Parallel hash build cannot be duplicated");
        }
    }

    @VisibleForTesting
    public enum State
    {
        /**
         * Operator accepts input
         */
        CONSUMING_INPUT,

        /**
         * LookupSource has been built and passed on without any spill occurring
         */
        LOOKUP_SOURCE_BUILT,

        /**
         * No longer needed
         */
        CLOSED,
    }

    private final OperatorContext operatorContext;
    private final CoarseGrainLocalMemoryContext localUserMemoryContext;
    private final PartitionedLookupSourceFactory lookupSourceFactory;
    private final ListenableFuture<Void> lookupSourceFactoryDestroyed;
    private final int partitionIndex;

    private final List<Integer> outputChannels;
    private final List<Integer> hashChannels;
    private final Optional<JoinFilterFunctionFactory> filterFunctionFactory;
    private final OptionalInt sortChannel;
    private final List<JoinFilterFunctionFactory> searchFunctionFactories;
    private final HashArraySizeSupplier hashArraySizeSupplier;
    @Nullable
    private final Operator runtimeConstraintCollector;

    private State state = State.CONSUMING_INPUT;
    @Nullable
    private PagesIndex index;
    private Optional<ListenableFuture<Void>> lookupSourceNotNeeded = Optional.empty();
    @Nullable
    private LookupSourceSupplier lookupSourceSupplier;

    public HashBuilderOperator(
            OperatorContext operatorContext,
            PartitionedLookupSourceFactory lookupSourceFactory,
            int partitionIndex,
            List<Integer> outputChannels,
            List<Integer> hashChannels,
            Optional<JoinFilterFunctionFactory> filterFunctionFactory,
            OptionalInt sortChannel,
            List<JoinFilterFunctionFactory> searchFunctionFactories,
            int expectedPositions,
            PagesIndex.Factory pagesIndexFactory,
            HashArraySizeSupplier hashArraySizeSupplier)
    {
        this(operatorContext, lookupSourceFactory, partitionIndex, outputChannels, hashChannels, filterFunctionFactory, sortChannel, searchFunctionFactories, expectedPositions, pagesIndexFactory, hashArraySizeSupplier, null, DEFAULT_GRANULARITY);
    }

    private HashBuilderOperator(
            OperatorContext operatorContext,
            PartitionedLookupSourceFactory lookupSourceFactory,
            int partitionIndex,
            List<Integer> outputChannels,
            List<Integer> hashChannels,
            Optional<JoinFilterFunctionFactory> filterFunctionFactory,
            OptionalInt sortChannel,
            List<JoinFilterFunctionFactory> searchFunctionFactories,
            int expectedPositions,
            PagesIndex.Factory pagesIndexFactory,
            HashArraySizeSupplier hashArraySizeSupplier,
            Operator runtimeConstraintCollector)
    {
        this(operatorContext, lookupSourceFactory, partitionIndex, outputChannels, hashChannels, filterFunctionFactory, sortChannel, searchFunctionFactories, expectedPositions, pagesIndexFactory, hashArraySizeSupplier, runtimeConstraintCollector, DEFAULT_GRANULARITY);
    }

    @VisibleForTesting
    HashBuilderOperator(
            OperatorContext operatorContext,
            PartitionedLookupSourceFactory lookupSourceFactory,
            int partitionIndex,
            List<Integer> outputChannels,
            List<Integer> hashChannels,
            Optional<JoinFilterFunctionFactory> filterFunctionFactory,
            OptionalInt sortChannel,
            List<JoinFilterFunctionFactory> searchFunctionFactories,
            int expectedPositions,
            PagesIndex.Factory pagesIndexFactory,
            HashArraySizeSupplier hashArraySizeSupplier,
            long memorySyncThreshold)
    {
        this(operatorContext, lookupSourceFactory, partitionIndex, outputChannels, hashChannels, filterFunctionFactory, sortChannel, searchFunctionFactories, expectedPositions, pagesIndexFactory, hashArraySizeSupplier, null, memorySyncThreshold);
    }

    private HashBuilderOperator(
            OperatorContext operatorContext,
            PartitionedLookupSourceFactory lookupSourceFactory,
            int partitionIndex,
            List<Integer> outputChannels,
            List<Integer> hashChannels,
            Optional<JoinFilterFunctionFactory> filterFunctionFactory,
            OptionalInt sortChannel,
            List<JoinFilterFunctionFactory> searchFunctionFactories,
            int expectedPositions,
            PagesIndex.Factory pagesIndexFactory,
            HashArraySizeSupplier hashArraySizeSupplier,
            Operator runtimeConstraintCollector,
            long memorySyncThreshold)
    {
        requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");

        this.operatorContext = operatorContext;
        this.partitionIndex = partitionIndex;
        this.filterFunctionFactory = filterFunctionFactory;
        this.sortChannel = sortChannel;
        this.searchFunctionFactories = searchFunctionFactories;
        this.localUserMemoryContext = new CoarseGrainLocalMemoryContext(operatorContext.localUserMemoryContext(), memorySyncThreshold);

        this.index = pagesIndexFactory.newPagesIndex(lookupSourceFactory.getTypes(), expectedPositions);
        this.lookupSourceFactory = lookupSourceFactory;
        lookupSourceFactoryDestroyed = lookupSourceFactory.isDestroyed();

        this.outputChannels = outputChannels;
        this.hashChannels = hashChannels;

        this.hashArraySizeSupplier = requireNonNull(hashArraySizeSupplier, "hashArraySizeSupplier is null");
        this.runtimeConstraintCollector = runtimeConstraintCollector;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public ListenableFuture<Void> isBlocked()
    {
        return switch (state) {
            case CONSUMING_INPUT -> NOT_BLOCKED;
            case LOOKUP_SOURCE_BUILT -> lookupSourceNotNeeded.orElseThrow(() -> new IllegalStateException("Lookup source built, but disposal future not set"));
            case CLOSED -> NOT_BLOCKED;
        };
    }

    @Override
    public boolean needsInput()
    {
        boolean stateNeedsInput = (state == State.CONSUMING_INPUT);

        return stateNeedsInput && !lookupSourceFactoryDestroyed.isDone();
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");

        if (lookupSourceFactoryDestroyed.isDone()) {
            close();
            return;
        }

        checkState(state == State.CONSUMING_INPUT);
        if (runtimeConstraintCollector != null) {
            runtimeConstraintCollector.addInput(page);
            runtimeConstraintCollector.getOutput();
        }
        updateIndex(page);
    }

    private void updateIndex(Page page)
    {
        checkState(index != null, "index is null");

        index.addPage(page);

        if (!localUserMemoryContext.trySetBytes(index.getEstimatedSize().toBytes())) {
            index.compact();
            localUserMemoryContext.setBytes(index.getEstimatedSize().toBytes());
        }
        operatorContext.recordOutput(page.getSizeInBytes(), page.getPositionCount());
    }

    @Override
    public Page getOutput()
    {
        return null;
    }

    @Override
    public void finish()
    {
        if (lookupSourceFactoryDestroyed.isDone()) {
            close();
            return;
        }

        switch (state) {
            case CONSUMING_INPUT -> {
                finishInput();
                return;
            }
            case LOOKUP_SOURCE_BUILT -> {
                disposeLookupSourceIfRequested();
                return;
            }
            case CLOSED -> {
                // no-op
                return;
            }
        }

        throw new IllegalStateException("Unhandled state: " + state);
    }

    private void finishInput()
    {
        if (runtimeConstraintCollector != null) {
            runtimeConstraintCollector.finish();
        }
        checkState(state == State.CONSUMING_INPUT);
        if (lookupSourceFactoryDestroyed.isDone()) {
            close();
            return;
        }

        checkState(index != null, "index is null");
        long outerPositionTrackerSizeInBytes = lookupSourceFactory.getOuterPositionTrackerSizeInBytes(index.getPositionCount());
        ListenableFuture<Void> reserved = localUserMemoryContext.setBytes(index.getEstimatedMemoryRequiredToCreateLookupSource(
                hashArraySizeSupplier,
                sortChannel,
                hashChannels) + outerPositionTrackerSizeInBytes);
        if (!reserved.isDone() || !operatorContext.isWaitingForMemory().isDone()) {
            // Yield when not enough memory is available to proceed, finish is expected to be called again when some memory is freed
            return;
        }
        LookupSourceSupplier partition = buildLookupSource();
        localUserMemoryContext.setBytes(partition.get().getInMemorySizeInBytes() + outerPositionTrackerSizeInBytes);
        lookupSourceNotNeeded = Optional.of(lookupSourceFactory.lendPartitionLookupSource(partitionIndex, partition));

        index = null;
        state = State.LOOKUP_SOURCE_BUILT;
    }

    private void disposeLookupSourceIfRequested()
    {
        checkState(state == State.LOOKUP_SOURCE_BUILT);
        verify(lookupSourceNotNeeded.isPresent());
        if (!lookupSourceNotNeeded.get().isDone()) {
            return;
        }

        close();
    }

    private LookupSourceSupplier buildLookupSource()
    {
        checkState(index != null, "index is null");
        LookupSourceSupplier partition = index.createLookupSourceSupplier(operatorContext.getSession(), hashChannels, filterFunctionFactory, sortChannel, searchFunctionFactories, Optional.of(outputChannels), hashArraySizeSupplier);
        checkState(lookupSourceSupplier == null, "lookupSourceSupplier is already set");
        this.lookupSourceSupplier = partition;
        return partition;
    }

    @Override
    public boolean isFinished()
    {
        if (lookupSourceFactoryDestroyed.isDone()) {
            // Finish early when the probe side is empty
            close();
            return true;
        }

        return state == State.CLOSED;
    }

    @Override
    public void close()
    {
        if (state == State.CLOSED) {
            return;
        }
        // close() can be called in any state, due for example to query failure, and must clean resource up unconditionally

        lookupSourceSupplier = null;
        index = null;
        localUserMemoryContext.setBytes(0);
        if (runtimeConstraintCollector != null) {
            try {
                runtimeConstraintCollector.close();
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        state = State.CLOSED;
    }

    @VisibleForTesting
    State getState()
    {
        return state;
    }
}
