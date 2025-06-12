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
package io.trino.operator.join;

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
import io.trino.operator.SpillMetrics;
import io.trino.spi.Page;
import io.trino.spi.metrics.Metric;
import io.trino.spi.metrics.Metrics;
import io.trino.spiller.SingleStreamSpiller;
import io.trino.spiller.SingleStreamSpillerFactory;
import io.trino.sql.gen.JoinFilterFunctionCompiler.JoinFilterFunctionFactory;
import io.trino.sql.planner.plan.PlanNodeId;
import jakarta.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Queue;

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
        private final OptionalInt preComputedHashChannel;
        private final Optional<JoinFilterFunctionFactory> filterFunctionFactory;
        private final Optional<Integer> sortChannel;
        private final List<JoinFilterFunctionFactory> searchFunctionFactories;
        private final PagesIndex.Factory pagesIndexFactory;

        private final int expectedPositions;
        private final boolean spillEnabled;
        private final SingleStreamSpillerFactory singleStreamSpillerFactory;
        private final HashArraySizeSupplier hashArraySizeSupplier;

        private int partitionIndex;

        private boolean closed;

        public HashBuilderOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager,
                List<Integer> outputChannels,
                List<Integer> hashChannels,
                OptionalInt preComputedHashChannel,
                Optional<JoinFilterFunctionFactory> filterFunctionFactory,
                Optional<Integer> sortChannel,
                List<JoinFilterFunctionFactory> searchFunctionFactories,
                int expectedPositions,
                PagesIndex.Factory pagesIndexFactory,
                boolean spillEnabled,
                SingleStreamSpillerFactory singleStreamSpillerFactory,
                HashArraySizeSupplier hashArraySizeSupplier)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            requireNonNull(sortChannel, "sortChannel cannot be null");
            requireNonNull(searchFunctionFactories, "searchFunctionFactories is null");
            checkArgument(sortChannel.isPresent() != searchFunctionFactories.isEmpty(), "both or none sortChannel and searchFunctionFactories must be set");
            this.lookupSourceFactoryManager = requireNonNull(lookupSourceFactoryManager, "lookupSourceFactoryManager is null");

            this.outputChannels = ImmutableList.copyOf(requireNonNull(outputChannels, "outputChannels is null"));
            this.hashChannels = ImmutableList.copyOf(requireNonNull(hashChannels, "hashChannels is null"));
            this.preComputedHashChannel = requireNonNull(preComputedHashChannel, "preComputedHashChannel is null");
            this.filterFunctionFactory = requireNonNull(filterFunctionFactory, "filterFunctionFactory is null");
            this.sortChannel = sortChannel;
            this.searchFunctionFactories = ImmutableList.copyOf(searchFunctionFactories);
            this.pagesIndexFactory = requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");
            this.spillEnabled = spillEnabled;
            this.singleStreamSpillerFactory = requireNonNull(singleStreamSpillerFactory, "singleStreamSpillerFactory is null");
            this.hashArraySizeSupplier = requireNonNull(hashArraySizeSupplier, "hashArraySizeSupplier is null");

            this.expectedPositions = expectedPositions;
        }

        @Override
        public HashBuilderOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, HashBuilderOperator.class.getSimpleName());

            PartitionedLookupSourceFactory lookupSourceFactory = this.lookupSourceFactoryManager.getJoinBridge();
            verify(partitionIndex < lookupSourceFactory.partitions());
            partitionIndex++;
            return new HashBuilderOperator(
                    operatorContext,
                    lookupSourceFactory,
                    partitionIndex - 1,
                    outputChannels,
                    hashChannels,
                    preComputedHashChannel,
                    filterFunctionFactory,
                    sortChannel,
                    searchFunctionFactories,
                    expectedPositions,
                    pagesIndexFactory,
                    spillEnabled,
                    singleStreamSpillerFactory,
                    hashArraySizeSupplier,
                    DEFAULT_GRANULARITY);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
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
        CLOSED
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
    private final OptionalInt preComputedHashChannel;
    private final Optional<JoinFilterFunctionFactory> filterFunctionFactory;
    private final Optional<Integer> sortChannel;
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

    public HashBuilderOperator(
            OperatorContext operatorContext,
            PartitionedLookupSourceFactory lookupSourceFactory,
            int partitionIndex,
            List<Integer> outputChannels,
            List<Integer> hashChannels,
            OptionalInt preComputedHashChannel,
            Optional<JoinFilterFunctionFactory> filterFunctionFactory,
            Optional<Integer> sortChannel,
            List<JoinFilterFunctionFactory> searchFunctionFactories,
            int expectedPositions,
            PagesIndex.Factory pagesIndexFactory,
            boolean spillEnabled,
            SingleStreamSpillerFactory singleStreamSpillerFactory,
            HashArraySizeSupplier hashArraySizeSupplier,
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
        this.preComputedHashChannel = preComputedHashChannel;

        this.spillEnabled = spillEnabled;
        this.singleStreamSpillerFactory = requireNonNull(singleStreamSpillerFactory, "singleStreamSpillerFactory is null");
        this.hashArraySizeSupplier = requireNonNull(hashArraySizeSupplier, "hashArraySizeSupplier is null");
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
                localUserMemoryContext.setBytes(index.getEstimatedSize().toBytes());
                localRevocableMemoryContext.setBytes(0);
                lookupSourceChecksum = OptionalLong.of(lookupSourceSupplier.checksum());
                lookupSourceSupplier = null;
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
                operatorContext.newLocalUserMemoryContext(HashBuilderOperator.class.getSimpleName())));
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
        if (lookupSourceFactoryDestroyed.isDone()) {
            close();
            return;
        }

        if (finishMemoryRevoke.isPresent()) {
            return;
        }

        switch (state) {
            case CONSUMING_INPUT:
                finishInput();
                return;

            case LOOKUP_SOURCE_BUILT:
                disposeLookupSourceIfRequested();
                return;

            case SPILLING_INPUT:
                finishSpilledInput();
                return;

            case INPUT_SPILLED:
                if (spilledLookupSourceHandle.getDisposeRequested().isDone()) {
                    close();
                }
                else {
                    unspillLookupSourceIfRequested();
                }
                return;

            case INPUT_UNSPILLING:
                finishLookupSourceUnspilling();
                return;

            case INPUT_UNSPILLED_AND_BUILT:
                disposeUnspilledLookupSourceIfRequested();
                return;

            case CLOSED:
                // no-op
                return;
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

        long memoryRequired = index.getEstimatedMemoryRequiredToCreateLookupSource(
                hashArraySizeSupplier,
                sortChannel,
                hashChannels);

        ListenableFuture<Void> reserved;
        if (spillEnabled) {
            reserved = localRevocableMemoryContext.setBytes(memoryRequired);
        }
        else {
            reserved = localUserMemoryContext.setBytes(memoryRequired);
        }

        if (!reserved.isDone()) {
            // wait for memory
            return;
        }

        LookupSourceSupplier partition = buildLookupSource();
        if (spillEnabled) {
            localRevocableMemoryContext.setBytes(partition.get().getInMemorySizeInBytes() + index.getExtraPagesIndexMemoryWithLookupSourceBuild());
        }
        else {
            localUserMemoryContext.setBytes(partition.get().getInMemorySizeInBytes() + index.getExtraPagesIndexMemoryWithLookupSourceBuild());
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
        if (!localUserMemoryContext.setBytes(spilledPagesInMemorySize + index.getEstimatedSize().toBytes()).isDone()) {
            // wait for memory
            return;
        }
        long unspillStartNanos = System.nanoTime();
        unspillInProgress = Optional.of(getSpiller().getAllSpilledPages());
        addSuccessCallback(unspillInProgress.get(), ignored -> {
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
        if (!reserved.isDone()) {
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
        LookupSourceSupplier partition = index.createLookupSourceSupplier(operatorContext.getSession(), hashChannels, preComputedHashChannel, filterFunctionFactory, sortChannel, searchFunctionFactories, Optional.of(outputChannels), hashArraySizeSupplier);
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
        finishMemoryRevoke = finishMemoryRevoke.map(ifPresent -> () -> {});

        try (Closer closer = Closer.create()) {
            closer.register(index::clear);
            spiller.ifPresent(closer::register);
            closer.register(() -> localUserMemoryContext.setBytes(0));
            closer.register(() -> localRevocableMemoryContext.setBytes(0));
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
