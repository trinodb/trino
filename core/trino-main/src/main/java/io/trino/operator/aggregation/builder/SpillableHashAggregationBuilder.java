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

import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.AggregationMetrics;
import io.trino.operator.FlatHashStrategyCompiler;
import io.trino.operator.MergeHashSort;
import io.trino.operator.OperatorContext;
import io.trino.operator.Work;
import io.trino.operator.WorkProcessor;
import io.trino.operator.aggregation.AggregatorFactory;
import io.trino.spi.Page;
import io.trino.spi.type.Type;
import io.trino.spiller.Spiller;
import io.trino.spiller.SpillerFactory;
import io.trino.sql.planner.plan.AggregationNode;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.airlift.concurrent.MoreFutures.addSuccessCallback;
import static io.airlift.concurrent.MoreFutures.asVoid;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.operator.WorkProcessor.ProcessState.Type.FINISHED;
import static io.trino.operator.WorkProcessor.ProcessState.Type.RESULT;
import static io.trino.operator.WorkProcessor.ProcessState.blocked;
import static io.trino.operator.WorkProcessor.ProcessState.finished;
import static io.trino.operator.WorkProcessor.ProcessState.ofResult;
import static io.trino.operator.WorkProcessor.flatten;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

public class SpillableHashAggregationBuilder
        implements HashAggregationBuilder
{
    private InMemoryHashAggregationBuilder hashAggregationBuilder;
    private final SpillerFactory spillerFactory;
    private final List<AggregatorFactory> aggregatorFactories;
    private final AggregationNode.Step step;
    private final int expectedGroups;
    private final List<Type> groupByTypes;
    private final List<Integer> groupByChannels;
    private final OperatorContext operatorContext;
    private final LocalMemoryContext localUserMemoryContext;
    private final LocalMemoryContext localRevocableMemoryContext;
    private final long memoryLimitForMerge;
    private final long memoryLimitForMergeWithMemory;
    private Optional<Spiller> spiller = Optional.empty();
    private Optional<MergingHashAggregationBuilder> merger = Optional.empty();
    private Optional<MergeHashSort> mergeHashSort = Optional.empty();
    private ListenableFuture<DataSize> spillInProgress = immediateFuture(DataSize.ofBytes(0L));
    private final FlatHashStrategyCompiler hashStrategyCompiler;
    private final AggregationMetrics aggregationMetrics;

    // todo get rid of that and only use revocable memory
    private long emptyHashAggregationBuilderSize;

    private boolean producingOutput;
    private boolean finishing;

    public SpillableHashAggregationBuilder(
            List<AggregatorFactory> aggregatorFactories,
            AggregationNode.Step step,
            int expectedGroups,
            List<Type> groupByTypes,
            List<Integer> groupByChannels,
            OperatorContext operatorContext,
            DataSize memoryLimitForMerge,
            DataSize memoryLimitForMergeWithMemory,
            SpillerFactory spillerFactory,
            FlatHashStrategyCompiler hashStrategyCompiler,
            AggregationMetrics aggregationMetrics)
    {
        this.aggregatorFactories = aggregatorFactories;
        this.step = step;
        this.expectedGroups = expectedGroups;
        this.groupByTypes = groupByTypes;
        this.groupByChannels = groupByChannels;
        this.operatorContext = operatorContext;
        this.localUserMemoryContext = operatorContext.localUserMemoryContext();
        this.localRevocableMemoryContext = operatorContext.localRevocableMemoryContext();
        this.memoryLimitForMerge = memoryLimitForMerge.toBytes();
        this.memoryLimitForMergeWithMemory = memoryLimitForMergeWithMemory.toBytes();
        this.spillerFactory = spillerFactory;
        this.hashStrategyCompiler = hashStrategyCompiler;
        this.aggregationMetrics = requireNonNull(aggregationMetrics, "aggregationMetrics is null");

        rebuildHashAggregationBuilder();
    }

    @Override
    public Work<?> processPage(Page page)
    {
        checkState(hasPreviousSpillCompletedSuccessfully(), "Previous spill hasn't yet finished");
        // hashAggregationBuilder is constructed with non yielding UpdateMemory instance.
        // Therefore the processing of the returned Work should always be true.
        // It is not possible to spill during processing of a page.
        return hashAggregationBuilder.processPage(page);
    }

    @Override
    public void updateMemory()
    {
        checkState(spillInProgress.isDone());

        if (producingOutput) {
            localRevocableMemoryContext.setBytes(0);
            localUserMemoryContext.setBytes(hashAggregationBuilder.getSizeInMemory());
        }
        else {
            localUserMemoryContext.setBytes(emptyHashAggregationBuilderSize);
            localRevocableMemoryContext.setBytes(hashAggregationBuilder.getSizeInMemory() - emptyHashAggregationBuilderSize);
        }
    }

    @Override
    public boolean isFull()
    {
        return false;
    }

    private boolean hasPreviousSpillCompletedSuccessfully()
    {
        if (spillInProgress.isDone()) {
            // check for exception from previous spill for early failure
            getFutureValue(spillInProgress);
            return true;
        }
        return false;
    }

    @Override
    public ListenableFuture<Void> startMemoryRevoke()
    {
        return spillToDisk();
    }

    @Override
    public void finishMemoryRevoke()
    {
        checkState(hasPreviousSpillCompletedSuccessfully(), "Previous spill hasn't yet finished");
        updateMemory();
    }

    private boolean shouldMergeWithMemory(long memorySize)
    {
        return memorySize < memoryLimitForMergeWithMemory;
    }

    @Override
    public WorkProcessor<Page> buildResult()
    {
        return flatten(WorkProcessor.create(() -> {
            checkState(hasPreviousSpillCompletedSuccessfully(), "Previous spill hasn't yet finished");
            // update memory after potential spill from the previous call to buildResult
            updateMemory();
            producingOutput = true;

            if (finishing) {
                return finished();
            }

            if (localRevocableMemoryContext.getBytes() > 0) {
                // No spill happened, try to build result from memory
                if (spiller.isEmpty()) {
                    // No spill happened, try to build result from memory. Revocable memory needs to be converted to user memory as producing output stage is no longer revocable.
                    long currentRevocableBytes = localRevocableMemoryContext.getBytes();
                    localRevocableMemoryContext.setBytes(0);
                    if (!localUserMemoryContext.trySetBytes(localUserMemoryContext.getBytes() + currentRevocableBytes)) {
                        // TODO: this might fail (even though we have just released memory), but we don't
                        // have a proper way to atomically convert memory reservations
                        localRevocableMemoryContext.setBytes(currentRevocableBytes);
                        // spill since revocable memory could not be converted to user memory immediately
                        return blocked(spillToDisk());
                    }
                }
                else if (!shouldMergeWithMemory(getSizeInMemoryWhenUnspilling())) {
                    return blocked(spillToDisk());
                }
            }

            finishing = true;
            if (spiller.isEmpty()) {
                return ofResult(hashAggregationBuilder.buildResult());
            }
            return ofResult(mergeFromDiskAndMemory());
        }));
    }

    /**
     * Estimates future memory usage, during unspilling.
     */
    private long getSizeInMemoryWhenUnspilling()
    {
        // TODO: we could skip memory reservation for hashAggregationBuilder.getGroupIdsSortingSize()
        // if before building result from hashAggregationBuilder we would convert it to "read only" version.
        // Read only version of GroupByHash from hashAggregationBuilder could be compacted by dropping
        // most of it's field, freeing up some memory that could be used for sorting.
        return hashAggregationBuilder.getSizeInMemory() + hashAggregationBuilder.getGroupIdsSortingSize();
    }

    @Override
    public void close()
    {
        try (Closer closer = Closer.create()) {
            if (hashAggregationBuilder != null) {
                closer.register(hashAggregationBuilder::close);
            }
            merger.ifPresent(closer::register);
            spiller.ifPresent(closer::register);
            mergeHashSort.ifPresent(closer::register);
            closer.register(() -> localUserMemoryContext.setBytes(0));
            closer.register(() -> localRevocableMemoryContext.setBytes(0));
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private ListenableFuture<Void> spillToDisk()
    {
        if (!spillInProgress.isDone()) {
            // Spill can be triggered first in SpillableHashAggregationBuilder.buildResult and then by Driver (via HashAggregationOperator#startMemoryRevoke).
            // While spill is in progress revocable memory is not released, hence redundant call to spillToDisk might be made.
            return asVoid(spillInProgress);
        }

        if (localRevocableMemoryContext.getBytes() == 0 || hasNoGroups()) {
            // This must be a stale revoke request as hashAggregationBuilder revocable memory could have been converted to user memory or
            // spill is completed but updateMemory() was not called yet.
            return immediateVoidFuture();
        }

        checkState(hasPreviousSpillCompletedSuccessfully(), "Previous spill hasn't yet finished");
        hashAggregationBuilder.setSpillOutput();

        if (spiller.isEmpty()) {
            spiller = Optional.of(spillerFactory.create(
                    hashAggregationBuilder.buildSpillTypes(),
                    operatorContext.getSpillContext(),
                    operatorContext.newAggregateUserMemoryContext()));
        }

        // start spilling process with current content of the hashAggregationBuilder builder...
        long spillStartNanos = System.nanoTime();
        spillInProgress = spiller.get().spill(hashAggregationBuilder.buildSpillResult().iterator());
        addSuccessCallback(spillInProgress, dataSize -> aggregationMetrics.recordSpillSince(spillStartNanos, dataSize.toBytes()));
        // ... and immediately create new hashAggregationBuilder so effectively memory ownership
        // over hashAggregationBuilder is transferred from this thread to a spilling thread
        rebuildHashAggregationBuilder();

        return asVoid(spillInProgress);
    }

    private boolean hasNoGroups()
    {
        return hashAggregationBuilder.getGroupCount() == 0;
    }

    private WorkProcessor<Page> mergeFromDiskAndMemory()
    {
        checkState(spiller.isPresent());

        hashAggregationBuilder.setSpillOutput();
        mergeHashSort = Optional.of(new MergeHashSort(operatorContext.newAggregateUserMemoryContext()));

        List<Type> spillTypes = hashAggregationBuilder.buildSpillTypes();
        long unspillStartNanos = System.nanoTime();
        AtomicLong unspillBytes = new AtomicLong(0);
        WorkProcessor<Page> mergedSpilledPages = mergeHashSort.get().merge(
                spillTypes,
                ImmutableList.<WorkProcessor<Page>>builder()
                        .addAll(spiller.get().getSpills().stream()
                                .map(WorkProcessor::fromIterator)
                                .map(processor -> processor.withProcessStateMonitor(state -> {
                                    if (state.getType() == RESULT) {
                                        unspillBytes.addAndGet(state.getResult().getSizeInBytes());
                                    }
                                }))
                                .collect(toImmutableList()))
                        .add(hashAggregationBuilder.buildSpillResult())
                        .build(),
                operatorContext.getDriverContext().getYieldSignal(),
                spillTypes.size() - 1);
        mergedSpilledPages = mergedSpilledPages.withProcessStateMonitor(state -> {
            if (state.getType() == FINISHED) {
                aggregationMetrics.recordUnspillSince(unspillStartNanos, unspillBytes.get());
            }
        });

        return mergeSortedPages(mergedSpilledPages, max(memoryLimitForMerge - memoryLimitForMergeWithMemory, 1L));
    }

    private WorkProcessor<Page> mergeSortedPages(WorkProcessor<Page> sortedPages, long memoryLimitForMerge)
    {
        merger = Optional.of(new MergingHashAggregationBuilder(
                aggregatorFactories,
                step,
                expectedGroups,
                groupByTypes,
                operatorContext,
                sortedPages,
                operatorContext.aggregateUserMemoryContext(),
                memoryLimitForMerge,
                hashAggregationBuilder.getKeyChannels(),
                hashStrategyCompiler,
                aggregationMetrics));

        return merger.get().buildResult();
    }

    private void rebuildHashAggregationBuilder()
    {
        if (hashAggregationBuilder != null) {
            hashAggregationBuilder.close();
        }

        hashAggregationBuilder = new InMemoryHashAggregationBuilder(
                aggregatorFactories,
                step,
                expectedGroups,
                groupByTypes,
                groupByChannels,
                true, // spillable
                operatorContext,
                Optional.of(DataSize.succinctBytes(0)),
                hashStrategyCompiler,
                () -> {
                    updateMemory();
                    // TODO: Support GroupByHash yielding in spillable hash aggregation (https://github.com/trinodb/trino/issues/460)
                    return true;
                },
                aggregationMetrics);
        emptyHashAggregationBuilderSize = hashAggregationBuilder.getSizeInMemory();
    }
}
