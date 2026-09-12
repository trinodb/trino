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
package io.trino.operator.join.spilling;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.ThreadSafe;
import io.airlift.concurrent.MoreFutures;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.trino.memory.context.CoarseGrainLocalMemoryContext;
import io.trino.memory.context.LocalMemoryContext;
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
import io.trino.operator.SpillMetrics;
import io.trino.operator.TaskRuntimeConstraintManager;
import io.trino.operator.join.JoinBridgeManager;
import io.trino.operator.join.LookupSourceSupplier;
import io.trino.operator.join.RuntimeConstraintComparison;
import io.trino.spi.Page;
import io.trino.spi.metrics.Metric;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spiller.SingleStreamSpiller;
import io.trino.spiller.SingleStreamSpillerFactory;
import io.trino.sql.gen.JoinFilterFunctionCompiler.JoinFilterFunctionFactory;
import io.trino.sql.ir.ComparisonOperator;
import io.trino.sql.planner.LocalRuntimeConstraintConsumer;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.runtimeconstraint.DistributedCompletionPolicy;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintWiringReport.CollectedConstraint;
import jakarta.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.airlift.concurrent.MoreFutures.addSuccessCallback;
import static io.airlift.concurrent.MoreFutures.asVoid;
import static io.airlift.concurrent.MoreFutures.checkSuccess;
import static io.airlift.concurrent.MoreFutures.getDone;
import static io.airlift.units.DataSize.succinctBytes;
import static io.trino.memory.context.CoarseGrainLocalMemoryContext.DEFAULT_GRANULARITY;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class HashBuilderOperator
        implements Operator
{
    private static final Logger log = Logger.get(HashBuilderOperator.class);

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
        private final boolean spillEnabled;
        private final SingleStreamSpillerFactory singleStreamSpillerFactory;
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
                boolean spillEnabled,
                SingleStreamSpillerFactory singleStreamSpillerFactory,
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
                    spillEnabled,
                    singleStreamSpillerFactory,
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
                boolean spillEnabled,
                SingleStreamSpillerFactory singleStreamSpillerFactory,
                HashArraySizeSupplier hashArraySizeSupplier,
                RuntimeConstraintCollectionLimits runtimeConstraintLimits,
                TypeOperators typeOperators)
        {
            this(operatorId, planNodeId, lookupSourceFactoryManager, outputChannels, hashChannels, filterFunctionFactory, sortChannel, searchFunctionFactories, expectedPositions, pagesIndexFactory, spillEnabled, singleStreamSpillerFactory, hashArraySizeSupplier, runtimeConstraintLimits, typeOperators, DistributedCompletionPolicy.UNION_ALL_PARTITIONS);
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
                boolean spillEnabled,
                SingleStreamSpillerFactory singleStreamSpillerFactory,
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
            this.spillEnabled = spillEnabled;
            this.singleStreamSpillerFactory = requireNonNull(singleStreamSpillerFactory, "singleStreamSpillerFactory is null");
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
                    spillEnabled,
                    singleStreamSpillerFactory,
                    hashArraySizeSupplier,
                    runtimeConstraintCollector,
                    DEFAULT_GRANULARITY);
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
         * Memory revoking occurred during {@link #CONSUMING_INPUT}. Operator accepts input and spills it
         */
        SPILLING_INPUT,

        /**
         * LookupSource has been built and passed on without any spill occurring
         */
        LOOKUP_SOURCE_BUILT,

        /**
         * Input has been finished and spilled
         */
        INPUT_SPILLED,

        /**
         * Spilled input is being unspilled
         */
        INPUT_UNSPILLING,

        /**
         * Spilled input has been unspilled, LookupSource built from it
         */
        INPUT_UNSPILLED_AND_BUILT,

        /**
         * No longer needed
         */
        CLOSED,
    }

    private static final double INDEX_COMPACTION_ON_REVOCATION_TARGET = 0.8;

    private final OperatorContext operatorContext;
    private final LocalMemoryContext localUserMemoryContext;
    private final LocalMemoryContext localRevocableMemoryContext;
    private final PartitionedLookupSourceFactory lookupSourceFactory;
    private final ListenableFuture<Void> lookupSourceFactoryDestroyed;
    private final int partitionIndex;

    private final List<Integer> outputChannels;
    private final List<Integer> hashChannels;
    private final Optional<JoinFilterFunctionFactory> filterFunctionFactory;
    private final OptionalInt sortChannel;
    private final List<JoinFilterFunctionFactory> searchFunctionFactories;

    private final PagesIndex index;
    private final HashArraySizeSupplier hashArraySizeSupplier;

    private final boolean spillEnabled;
    private final SingleStreamSpillerFactory singleStreamSpillerFactory;

    private final SpillMetrics inputSpillMetrics = new SpillMetrics("Build input");
    private final SpillMetrics indexSpillMetrics = new SpillMetrics("Index");

    private State state = State.CONSUMING_INPUT;
    private Optional<ListenableFuture<Void>> lookupSourceNotNeeded = Optional.empty();
    private final SpilledLookupSourceHandle spilledLookupSourceHandle = new SpilledLookupSourceHandle();
    private Optional<SingleStreamSpiller> spiller = Optional.empty();
    private ListenableFuture<DataSize> spillInProgress = immediateFuture(DataSize.ofBytes(0));
    private Optional<ListenableFuture<List<Page>>> unspillInProgress = Optional.empty();
    private boolean unspilledPagesAdded;
    @Nullable
    private LookupSourceSupplier lookupSourceSupplier;
    private OptionalLong lookupSourceChecksum = OptionalLong.empty();

    private Optional<Runnable> finishMemoryRevoke = Optional.empty();
    @Nullable
    private final Operator runtimeConstraintCollector;

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
            boolean spillEnabled,
            SingleStreamSpillerFactory singleStreamSpillerFactory,
            HashArraySizeSupplier hashArraySizeSupplier,
            long memorySyncGranularity)
    {
        this(operatorContext, lookupSourceFactory, partitionIndex, outputChannels, hashChannels, filterFunctionFactory, sortChannel, searchFunctionFactories, expectedPositions, pagesIndexFactory, spillEnabled, singleStreamSpillerFactory, hashArraySizeSupplier, null, memorySyncGranularity);
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
            boolean spillEnabled,
            SingleStreamSpillerFactory singleStreamSpillerFactory,
            HashArraySizeSupplier hashArraySizeSupplier,
            Operator runtimeConstraintCollector,
            long memorySyncGranularity)
    {
        requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");

        this.operatorContext = operatorContext;
        this.partitionIndex = partitionIndex;
        this.filterFunctionFactory = filterFunctionFactory;
        this.sortChannel = sortChannel;
        this.searchFunctionFactories = searchFunctionFactories;
        this.localUserMemoryContext = new CoarseGrainLocalMemoryContext(operatorContext.localUserMemoryContext(), memorySyncGranularity);
        this.localRevocableMemoryContext = new CoarseGrainLocalMemoryContext(operatorContext.localRevocableMemoryContext(), memorySyncGranularity);

        this.index = pagesIndexFactory.newPagesIndex(lookupSourceFactory.getTypes(), expectedPositions);
        this.lookupSourceFactory = lookupSourceFactory;
        lookupSourceFactoryDestroyed = lookupSourceFactory.isDestroyed();

        this.outputChannels = outputChannels;
        this.hashChannels = hashChannels;

        this.spillEnabled = spillEnabled;
        this.singleStreamSpillerFactory = requireNonNull(singleStreamSpillerFactory, "singleStreamSpillerFactory is null");
        this.hashArraySizeSupplier = requireNonNull(hashArraySizeSupplier, "hashArraySizeSupplier is null");
        this.runtimeConstraintCollector = runtimeConstraintCollector;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @VisibleForTesting
    public State getState()
    {
        return state;
    }

    @Override
    public ListenableFuture<Void> isBlocked()
    {
        return switch (state) {
            case CONSUMING_INPUT -> NOT_BLOCKED;
            case SPILLING_INPUT -> asVoid(spillInProgress);
            case LOOKUP_SOURCE_BUILT -> lookupSourceNotNeeded.orElseThrow(() -> new IllegalStateException("Lookup source built, but disposal future not set"));
            case INPUT_SPILLED -> spilledLookupSourceHandle.getUnspillingOrDisposeRequested();
            case INPUT_UNSPILLING -> unspillInProgress.map(MoreFutures::asVoid).orElseThrow(() -> new IllegalStateException("Unspilling in progress, but unspilling future not set"));
            case INPUT_UNSPILLED_AND_BUILT -> spilledLookupSourceHandle.getDisposeRequested();
            case CLOSED -> NOT_BLOCKED;
        };
    }

    @Override
    public boolean needsInput()
    {
        boolean stateNeedsInput = (state == State.CONSUMING_INPUT)
                || (state == State.SPILLING_INPUT && spillInProgress.isDone());

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

        if (runtimeConstraintCollector != null) {
            runtimeConstraintCollector.addInput(page);
            runtimeConstraintCollector.getOutput();
        }

        if (state == State.SPILLING_INPUT) {
            spillInput(page);
            return;
        }

        checkState(state == State.CONSUMING_INPUT);
        updateIndex(page);
    }

    private void updateIndex(Page page)
    {
        index.addPage(page);

        if (spillEnabled) {
            localRevocableMemoryContext.setBytes(index.getEstimatedSize().toBytes());
        }
        else {
            if (!localUserMemoryContext.trySetBytes(index.getEstimatedSize().toBytes())) {
                index.compact();
                localUserMemoryContext.setBytes(index.getEstimatedSize().toBytes());
            }
        }
        operatorContext.recordOutput(page.getSizeInBytes(), page.getPositionCount());
    }

    private void spillInput(Page page)
    {
        checkState(spillInProgress.isDone(), "Previous spill still in progress");
        checkSuccess(spillInProgress, "spilling failed");
        long spillStartNanos = System.nanoTime();
        spillInProgress = getSpiller().spill(page);
        addSuccessCallback(spillInProgress, dataSize -> {
            inputSpillMetrics.recordSpillSince(spillStartNanos, dataSize.toBytes());
            updateMetrics();
        });
    }

    @Override
    public ListenableFuture<Void> startMemoryRevoke()
    {
        checkState(spillEnabled, "Spill not enabled, no revokable memory should be reserved");

        if (state == State.CONSUMING_INPUT) {
            long indexSizeBeforeCompaction = index.getEstimatedSize().toBytes();
            index.compact();
            long indexSizeAfterCompaction = index.getEstimatedSize().toBytes();
            if (indexSizeAfterCompaction < indexSizeBeforeCompaction * INDEX_COMPACTION_ON_REVOCATION_TARGET) {
                finishMemoryRevoke = Optional.of(() -> {});
                localRevocableMemoryContext.setBytes(indexSizeAfterCompaction);
                return immediateVoidFuture();
            }

            finishMemoryRevoke = Optional.of(() -> {
                index.clear();
                localUserMemoryContext.setBytes(index.getEstimatedSize().toBytes());
                localRevocableMemoryContext.setBytes(0);
                lookupSourceFactory.setPartitionSpilledLookupSourceHandle(partitionIndex, spilledLookupSourceHandle);
                state = State.SPILLING_INPUT;
            });
            return spillIndex();
        }
        if (state == State.LOOKUP_SOURCE_BUILT) {
            finishMemoryRevoke = Optional.of(() -> {
                lookupSourceFactory.setPartitionSpilledLookupSourceHandle(partitionIndex, spilledLookupSourceHandle);
                lookupSourceNotNeeded = Optional.empty();
                index.clear();
                lookupSourceChecksum = OptionalLong.of(lookupSourceSupplier.checksum());
                lookupSourceSupplier = null;
                localUserMemoryContext.setBytes(index.getEstimatedSize().toBytes());
                localRevocableMemoryContext.setBytes(0);
                state = State.INPUT_SPILLED;
            });
            return spillIndex();
        }
        if (operatorContext.getReservedRevocableBytes() == 0) {
            // Probably stale revoking request
            finishMemoryRevoke = Optional.of(() -> {});
            return immediateVoidFuture();
        }

        throw new IllegalStateException(format("State %s cannot have revocable memory, but has %s revocable bytes", state, operatorContext.getReservedRevocableBytes()));
    }

    private ListenableFuture<Void> spillIndex()
    {
        checkState(spiller.isEmpty(), "Spiller already created");
        spiller = Optional.of(singleStreamSpillerFactory.create(
                index.getTypes(),
                operatorContext.getSpillContext().newLocalSpillContext(),
                operatorContext.newLocalUserMemoryContext(HashBuilderOperator.class.getSimpleName()),
                true));
        long spillStartNanos = System.nanoTime();
        ListenableFuture<DataSize> spillFuture = getSpiller().spill(index.getPages());
        addSuccessCallback(spillFuture, dataSize -> {
            indexSpillMetrics.recordSpillSince(spillStartNanos, dataSize.toBytes());
            updateMetrics();
        });
        return asVoid(spillFuture);
    }

    @Override
    public void finishMemoryRevoke()
    {
        checkState(finishMemoryRevoke.isPresent(), "Cannot finish unknown revoking");
        finishMemoryRevoke.get().run();
        finishMemoryRevoke = Optional.empty();
    }

    @Override
    public Page getOutput()
    {
        return null;
    }

    @Override
    public void finish()
    {
        if (runtimeConstraintCollector != null) {
            runtimeConstraintCollector.finish();
        }
        if (lookupSourceFactoryDestroyed.isDone()) {
            close();
            return;
        }

        if (finishMemoryRevoke.isPresent()) {
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
            case SPILLING_INPUT -> {
                finishSpilledInput();
                return;
            }
            case INPUT_SPILLED -> {
                if (spilledLookupSourceHandle.getDisposeRequested().isDone()) {
                    close();
                }
                else {
                    unspillLookupSourceIfRequested();
                }
                return;
            }
            case INPUT_UNSPILLING -> {
                finishLookupSourceUnspilling();
                return;
            }
            case INPUT_UNSPILLED_AND_BUILT -> {
                disposeUnspilledLookupSourceIfRequested();
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
        checkState(state == State.CONSUMING_INPUT);
        if (lookupSourceFactoryDestroyed.isDone()) {
            close();
            return;
        }

        long outerPositionTrackerSizeInBytes = lookupSourceFactory.getOuterPositionTrackerSizeInBytes(index.getPositionCount());
        // Outer join does not support spill, so the tracker bytes never land in revocable memory
        verify(outerPositionTrackerSizeInBytes == 0 || !spillEnabled);
        long memoryRequired = index.getEstimatedMemoryRequiredToCreateLookupSource(
                hashArraySizeSupplier,
                sortChannel,
                hashChannels) + outerPositionTrackerSizeInBytes;

        ListenableFuture<Void> reserved;
        if (spillEnabled) {
            reserved = localRevocableMemoryContext.setBytes(memoryRequired);
        }
        else {
            reserved = localUserMemoryContext.setBytes(memoryRequired);
        }

        if (!reserved.isDone() || !operatorContext.isWaitingForMemory().isDone() || !operatorContext.isWaitingForRevocableMemory().isDone()) {
            // wait for memory
            return;
        }

        LookupSourceSupplier partition = buildLookupSource();
        if (spillEnabled) {
            localRevocableMemoryContext.setBytes(partition.get().getInMemorySizeInBytes() + index.getExtraPagesIndexMemoryWithLookupSourceBuild() + outerPositionTrackerSizeInBytes);
        }
        else {
            localUserMemoryContext.setBytes(partition.get().getInMemorySizeInBytes() + index.getExtraPagesIndexMemoryWithLookupSourceBuild() + outerPositionTrackerSizeInBytes);
        }
        lookupSourceNotNeeded = Optional.of(lookupSourceFactory.lendPartitionLookupSource(partitionIndex, partition));

        state = State.LOOKUP_SOURCE_BUILT;
    }

    private void disposeLookupSourceIfRequested()
    {
        checkState(state == State.LOOKUP_SOURCE_BUILT);
        verify(lookupSourceNotNeeded.isPresent());
        if (!lookupSourceNotNeeded.get().isDone()) {
            return;
        }

        index.clear();
        localRevocableMemoryContext.setBytes(0);
        localUserMemoryContext.setBytes(index.getEstimatedSize().toBytes());
        lookupSourceSupplier = null;
        close();
    }

    private void finishSpilledInput()
    {
        checkState(state == State.SPILLING_INPUT);
        if (!spillInProgress.isDone()) {
            // Not ready to handle finish() yet
            return;
        }
        checkSuccess(spillInProgress, "spilling failed");
        state = State.INPUT_SPILLED;
    }

    private void unspillLookupSourceIfRequested()
    {
        checkState(state == State.INPUT_SPILLED);
        if (!spilledLookupSourceHandle.getUnspillingRequested().isDone()) {
            // Nothing to do yet.
            return;
        }

        verify(spiller.isPresent());
        verify(unspillInProgress.isEmpty());

        long spilledPagesInMemorySize = getSpiller().getSpilledPagesInMemorySize();
        ListenableFuture<Void> reserved = localUserMemoryContext.setBytes(spilledPagesInMemorySize + index.getEstimatedSize().toBytes());
        if (!reserved.isDone() || !operatorContext.isWaitingForMemory().isDone()) {
            // wait for memory
            return;
        }
        long unspillStartNanos = System.nanoTime();
        unspillInProgress = Optional.of(getSpiller().getAllSpilledPages());
        addSuccessCallback(unspillInProgress.get(), _ -> {
            indexSpillMetrics.recordUnspillSince(unspillStartNanos, spilledPagesInMemorySize);
            updateMetrics();
        });

        state = State.INPUT_UNSPILLING;
        unspilledPagesAdded = false;
    }

    private void finishLookupSourceUnspilling()
    {
        checkState(state == State.INPUT_UNSPILLING);

        if (!unspilledPagesAdded) {
            if (!unspillInProgress.get().isDone()) {
                // Pages have not been unspilled yet.
                return;
            }

            Queue<Page> pages = new ArrayDeque<>(getDone(unspillInProgress.get()));
            unspillInProgress = Optional.empty();
            long sizeOfUnspilledPages = pages.stream()
                    .mapToLong(Page::getSizeInBytes)
                    .sum();
            long retainedSizeOfUnspilledPages = pages.stream()
                    .mapToLong(Page::getRetainedSizeInBytes)
                    .sum();
            log.debug(
                    "Unspilling for operator %s, unspilled partition %d, sizeOfUnspilledPages %s, retainedSizeOfUnspilledPages %s",
                    operatorContext,
                    partitionIndex,
                    succinctBytes(sizeOfUnspilledPages),
                    succinctBytes(retainedSizeOfUnspilledPages));
            localUserMemoryContext.setBytes(retainedSizeOfUnspilledPages + index.getEstimatedSize().toBytes());

            while (!pages.isEmpty()) {
                Page next = pages.remove();
                index.addPage(next);
                // There is no attempt to compact index, since unspilled pages are unlikely to have blocks with retained size > logical size.
                retainedSizeOfUnspilledPages -= next.getRetainedSizeInBytes();
                localUserMemoryContext.setBytes(retainedSizeOfUnspilledPages + index.getEstimatedSize().toBytes());
            }

            unspilledPagesAdded = true;
        }

        ListenableFuture<Void> reserved = localUserMemoryContext.setBytes(index.getEstimatedMemoryRequiredToCreateLookupSource(
                hashArraySizeSupplier,
                sortChannel,
                hashChannels));
        if (!reserved.isDone() || !operatorContext.isWaitingForMemory().isDone()) {
            // Wait for memory
            return;
        }

        LookupSourceSupplier partition = buildLookupSource();
        lookupSourceChecksum.ifPresent(checksum ->
                checkState(partition.checksum() == checksum, "Unspilled lookupSource checksum does not match original one"));
        localUserMemoryContext.setBytes(partition.get().getInMemorySizeInBytes() + index.getExtraPagesIndexMemoryWithLookupSourceBuild());

        spilledLookupSourceHandle.setLookupSource(partition);

        state = State.INPUT_UNSPILLED_AND_BUILT;
    }

    private void disposeUnspilledLookupSourceIfRequested()
    {
        checkState(state == State.INPUT_UNSPILLED_AND_BUILT);
        if (!spilledLookupSourceHandle.getDisposeRequested().isDone()) {
            return;
        }

        index.clear();
        localUserMemoryContext.setBytes(index.getEstimatedSize().toBytes());

        close();
        spilledLookupSourceHandle.setDisposeCompleted();
    }

    private LookupSourceSupplier buildLookupSource()
    {
        LookupSourceSupplier partition = index.createLookupSourceSupplier(operatorContext.getSession(), hashChannels, filterFunctionFactory, sortChannel, searchFunctionFactories, Optional.of(outputChannels), hashArraySizeSupplier);
        checkState(lookupSourceSupplier == null, "lookupSourceSupplier is already set");
        this.lookupSourceSupplier = partition;
        return partition;
    }

    private void updateMetrics()
    {
        operatorContext.setLatestMetrics(new Metrics(ImmutableMap.<String, Metric<?>>builder()
                .putAll(inputSpillMetrics.getMetrics().getMetrics())
                .putAll(indexSpillMetrics.getMetrics().getMetrics())
                .buildOrThrow()));
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

    private SingleStreamSpiller getSpiller()
    {
        return spiller.orElseThrow(() -> new IllegalStateException("Spiller not created"));
    }

    @Override
    public void close()
    {
        if (state == State.CLOSED) {
            return;
        }
        // close() can be called in any state, due for example to query failure, and must clean resource up unconditionally

        lookupSourceSupplier = null;
        unspillInProgress = Optional.empty();
        state = State.CLOSED;
        finishMemoryRevoke = finishMemoryRevoke.map(_ -> () -> {});

        try (Closer closer = Closer.create()) {
            closer.register(index::clear);
            spiller.ifPresent(closer::register);
            closer.register(() -> localUserMemoryContext.setBytes(0));
            closer.register(() -> localRevocableMemoryContext.setBytes(0));
            if (runtimeConstraintCollector != null) {
                closer.register(() -> {
                    try {
                        runtimeConstraintCollector.close();
                    }
                    catch (Exception e) {
                        throw new IOException(e);
                    }
                });
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
